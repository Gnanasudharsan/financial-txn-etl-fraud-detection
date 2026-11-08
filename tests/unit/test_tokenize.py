from __future__ import annotations

import pytest
from chispa import assert_df_equality

from fintxn.security.tokenize import (
    MissingKeyError,
    hmac_token,
    hmac_token_col,
    last4_col,
    load_token_key,
    redact_pans_col,
)

KEY = bytes.fromhex("0f" * 32)


@pytest.mark.spark
def test_spark_hmac_matches_python_reference(spark) -> None:
    pans = ["4111111111111111", "5500005555555559", "378282246310005", None]
    df = spark.createDataFrame([(p,) for p in pans], "pan string")
    got = [r["t"] for r in df.select(hmac_token_col("pan", KEY).alias("t")).collect()]
    assert got == [hmac_token(p, KEY) if p else None for p in pans]


@pytest.mark.spark
def test_long_keys_are_hashed_like_hmac(spark) -> None:
    key = b"x" * 100
    df = spark.createDataFrame([("4111111111111111",)], "pan string")
    assert df.select(hmac_token_col("pan", key)).first()[0] == hmac_token("4111111111111111", key)


@pytest.mark.spark
def test_last4_and_redaction(spark) -> None:
    df = spark.createDataFrame(
        [("4111111111111111", "tx,4111111111111111,ACME, LLC,12.5")], "pan string, raw string"
    )
    out = df.select(last4_col("pan").alias("l4"), redact_pans_col("raw").alias("raw"))
    expected = spark.createDataFrame(
        [("1111", "tx,[PAN-REDACTED],ACME, LLC,12.5")], "l4 string, raw string"
    )
    assert_df_equality(out, expected, ignore_nullable=True)


def test_missing_or_short_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FINTXN_TOKEN_KEY", raising=False)
    with pytest.raises(MissingKeyError):
        load_token_key()
    monkeypatch.setenv("FINTXN_TOKEN_KEY", "ab")
    with pytest.raises(MissingKeyError):
        load_token_key()
