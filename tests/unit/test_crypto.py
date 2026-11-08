from __future__ import annotations

import base64

import pytest
from cryptography.exceptions import InvalidTag

from fintxn.security.crypto import (
    AccessDeniedError,
    decrypt_col,
    decrypt_value,
    encrypt_col,
    encrypt_value,
    load_encryption_key,
    mask_email_col,
    require_role,
)

KEY = b"k" * 32


def test_roundtrip_and_randomized_nonce() -> None:
    a = encrypt_value("jane@example.com", KEY, "email")
    b = encrypt_value("jane@example.com", KEY, "email")
    assert a != b and a.startswith("v1:")
    assert decrypt_value(a, KEY, "email") == "jane@example.com"
    assert encrypt_value(None, KEY, "email") is None


def test_aad_binds_ciphertext_to_column() -> None:
    token = encrypt_value("555-0100", KEY, "phone")
    with pytest.raises(InvalidTag):
        decrypt_value(token, KEY, "email")


def test_wrong_key_fails() -> None:
    token = encrypt_value("x", KEY, "c")
    with pytest.raises(InvalidTag):
        decrypt_value(token, b"z" * 32, "c")


def test_role_gate() -> None:
    assert require_role("auditor") == "auditor"
    assert require_role("data_engineer") == "data_engineer"
    with pytest.raises(AccessDeniedError):
        require_role("analyst")
    with pytest.raises(AccessDeniedError):
        require_role("intern")


def test_key_must_be_32_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINTXN_ENCRYPTION_KEY", base64.b64encode(b"short").decode())
    with pytest.raises(RuntimeError):
        load_encryption_key()


@pytest.mark.spark
def test_spark_encrypt_decrypt_and_mask(spark) -> None:
    df = spark.createDataFrame([("jane.doe@gmail.com",), (None,)], "email string")
    enc = df.withColumn("email_enc", encrypt_col("email", KEY)).withColumn(
        "m", mask_email_col("email")
    )
    rows = enc.collect()
    assert rows[0]["email_enc"].startswith("v1:") and rows[1]["email_enc"] is None
    assert rows[0]["m"] == "j*********@gmail.com"
    dec = enc.select(decrypt_col("email_enc", KEY, "email", role="auditor").alias("d")).collect()
    assert dec[0]["d"] == "jane.doe@gmail.com"
    with pytest.raises(AccessDeniedError):
        decrypt_col("email_enc", KEY, "email", role="analyst")
