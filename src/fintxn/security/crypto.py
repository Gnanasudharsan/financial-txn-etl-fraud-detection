"""Column-level encryption with AES-256-GCM and a role-gated decrypt utility.

Ciphertext format: ``v1:<base64(nonce(12) || ciphertext || tag(16))>``. The column name is used
as GCM associated data, so a value encrypted for ``email`` cannot be swapped into ``phone``.
The key is read only from ``FINTXN_ENCRYPTION_KEY`` (base64, 32 bytes): a local ``.env``,
GitHub Secrets in CI, or a Databricks secret scope in deployment.
"""

from __future__ import annotations

import base64
import os
from collections.abc import Iterable

import pandas as pd
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from fintxn.security.tokenize import MissingKeyError

KEY_VERSION = "v1"
NONCE_BYTES = 12
# Roles allowed to see decrypted PII. Analysts only ever see tokens / masked values.
DECRYPT_ROLES = frozenset({"auditor", "data_engineer"})
ROLES = frozenset({"data_engineer", "analyst", "auditor"})


class AccessDeniedError(PermissionError):
    pass


def load_encryption_key(env_var: str = "FINTXN_ENCRYPTION_KEY") -> bytes:
    raw = os.environ.get(env_var, "")
    if not raw:
        raise MissingKeyError(f"{env_var} is not set; run `make keys` or configure a secret scope")
    key = base64.b64decode(raw)
    if len(key) != 32:
        raise MissingKeyError(f"{env_var} must decode to exactly 32 bytes (AES-256)")
    return key


def encrypt_value(plaintext: str | None, key: bytes, aad: str) -> str | None:
    if plaintext is None:
        return None
    nonce = os.urandom(NONCE_BYTES)
    ct = AESGCM(key).encrypt(nonce, plaintext.encode(), aad.encode())
    return f"{KEY_VERSION}:{base64.b64encode(nonce + ct).decode()}"


def decrypt_value(token: str | None, key: bytes, aad: str) -> str | None:
    if token is None:
        return None
    version, _, payload = token.partition(":")
    if version != KEY_VERSION:
        raise ValueError(f"unsupported key version {version!r}")
    blob = base64.b64decode(payload)
    return AESGCM(key).decrypt(blob[:NONCE_BYTES], blob[NONCE_BYTES:], aad.encode()).decode()


def require_role(role: str | None, allowed: Iterable[str] = DECRYPT_ROLES) -> str:
    value: str = role or os.environ.get("FINTXN_ROLE") or ""
    role = value.strip()
    if role not in ROLES:
        raise AccessDeniedError(f"unknown role {role!r}; expected one of {sorted(ROLES)}")
    if role not in set(allowed):
        raise AccessDeniedError(f"role {role!r} is not permitted to decrypt PII")
    return role


def encrypt_col(col: str, key: bytes) -> Column:
    """Spark column encrypting ``col`` (vectorised pandas UDF, AAD = column name)."""

    @F.pandas_udf(StringType())  # type: ignore[call-overload, misc, attr-defined]
    def _enc(s: pd.Series) -> pd.Series:
        # Imported inside the UDF: the Rust-backed class must not be captured by cloudpickle.
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

        aes = _AESGCM(key)
        aad = col.encode()

        def one(v: object) -> str | None:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            nonce = os.urandom(NONCE_BYTES)
            ct = aes.encrypt(nonce, str(v).encode(), aad)
            return f"{KEY_VERSION}:{base64.b64encode(nonce + ct).decode()}"

        return s.map(one)

    return _enc(F.col(col))  # type: ignore[no-any-return]


def decrypt_col(col: str, key: bytes, aad: str, role: str | None = None) -> Column:
    """Spark column decrypting ``col``; raises :class:`AccessDeniedError` for analysts."""
    require_role(role)

    @F.pandas_udf(StringType())  # type: ignore[call-overload, misc, attr-defined]
    def _dec(s: pd.Series) -> pd.Series:
        from fintxn.security.crypto import decrypt_value as _decrypt

        return s.map(lambda v: _decrypt(v, key, aad) if isinstance(v, str) else None)

    return _dec(F.col(col))  # type: ignore[no-any-return]


def mask_email_col(col: str) -> Column:
    """``jane.doe12@gmail.com`` -> ``j*********@gmail.com`` (analyst-safe)."""
    c = F.col(col)
    return F.when(
        c.contains("@"),
        F.concat(F.substring(c, 1, 1), F.lit("*********@"), F.substring_index(c, "@", -1)),
    )


def mask_phone_col(col: str) -> Column:
    c = F.col(col)
    return F.when(F.length(c) >= 4, F.concat(F.lit("***-***-"), F.substring(c, -4, 4)))
