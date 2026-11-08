"""Deterministic card-number tokenization with HMAC-SHA256.

Implemented twice and kept in lock-step by a unit test:

* :func:`hmac_token` - pure Python reference (used by tools and tests)
* :func:`hmac_token_col` - pure Spark SQL expression (no Python UDF), so tokenizing millions of
  rows stays inside the JVM. HMAC(K, m) = H((K ^ opad) || H((K ^ ipad) || m)).

The key comes only from ``FINTXN_TOKEN_KEY`` (hex) - never from code or config files.
"""

from __future__ import annotations

import hashlib
import hmac

from pyspark.sql import Column
from pyspark.sql import functions as F

from fintxn.secrets import get_secret

BLOCK = 64
PAN_REGEX = r"(?<![0-9])[0-9]{13,19}(?![0-9])"


class MissingKeyError(RuntimeError):
    pass


def load_token_key(env_var: str = "FINTXN_TOKEN_KEY") -> bytes:
    raw = get_secret(env_var)
    if not raw:
        raise MissingKeyError(f"{env_var} is not set; run `make keys` or configure a secret scope")
    key = bytes.fromhex(raw)
    if len(key) < 32:
        raise MissingKeyError(f"{env_var} must be at least 32 bytes (64 hex chars)")
    return key


def hmac_token(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()


def _pads(key: bytes) -> tuple[str, str]:
    if len(key) > BLOCK:
        key = hashlib.sha256(key).digest()
    key = key.ljust(BLOCK, b"\x00")
    ipad = bytes(b ^ 0x36 for b in key).hex()
    opad = bytes(b ^ 0x5C for b in key).hex()
    return ipad, opad


def hmac_token_col(col: Column | str, key: bytes) -> Column:
    """Spark Column computing ``hex(HMAC-SHA256(key, col))``; nulls stay null."""
    c = F.col(col) if isinstance(col, str) else col
    ipad, opad = _pads(key)
    inner = F.sha2(F.concat(F.unhex(F.lit(ipad)), c.cast("binary")), 256)
    outer = F.sha2(F.concat(F.unhex(F.lit(opad)), F.unhex(inner)), 256)
    return F.when(c.isNull(), F.lit(None)).otherwise(outer)


def last4_col(col: Column | str) -> Column:
    c = F.col(col) if isinstance(col, str) else col
    return F.when(F.length(c) >= 4, F.substring(c, -4, 4))


def redact_pans_col(col: Column | str) -> Column:
    """Remove anything that looks like a PAN from free text (e.g. corrupt raw lines)."""
    c = F.col(col) if isinstance(col, str) else col
    return F.regexp_replace(c, PAN_REGEX, "[PAN-REDACTED]")
