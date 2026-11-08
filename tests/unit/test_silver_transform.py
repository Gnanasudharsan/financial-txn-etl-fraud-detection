from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from chispa import assert_column_equality
from pyspark.sql import functions as F

from fintxn.silver.transform import clean_amount, flag_duplicates, parse_event_ts, validate

pytestmark = pytest.mark.spark


def test_clean_amount_standardizes_and_rejects(spark) -> None:
    rows = [
        ("12.50", Decimal("12.50")),
        ("$12.50", Decimal("12.50")),
        (" 7.00 ", Decimal("7.00")),
        ("1,234.56", Decimal("1234.56")),
        ("12,34", None),
        ("abc", None),
        ("1.2.3", None),
        ("-45.10", None),
        ("9e9999", None),
        (None, None),
    ]
    df = spark.createDataFrame(rows, "raw string, expected decimal(18,2)").withColumn(
        "got", clean_amount("raw")
    )
    assert_column_equality(df, "got", "expected")


def test_parse_event_ts_formats_to_utc(spark) -> None:
    exp = datetime(2026, 1, 5, 13, 45, 12)
    rows = [
        ("2026-01-05T13:45:12Z", exp),
        ("2026-01-05 13:45:12", exp),
        ("2026-01-05T08:45:12-05:00", exp),
        ("2026-01-05T19:15:12+05:30", exp),
        ("1767620712000", exp),
        ("2026-13-45 25:61:00", None),
        ("2026-02-30T10:00:00Z", None),
        ("not_a_date", None),
    ]
    df = spark.createDataFrame(rows, "raw string, expected timestamp").withColumn(
        "got", parse_event_ts("raw")
    )
    got = {r["raw"]: r["got"] for r in df.collect()}
    assert got["2026-01-05T13:45:12Z"] == exp
    assert got["2026-01-05 13:45:12"] == exp
    assert got["2026-01-05T08:45:12-05:00"] == exp
    assert got["2026-01-05T19:15:12+05:30"] == exp
    assert got["1767620712000"] == exp
    for bad in ("2026-13-45 25:61:00", "2026-02-30T10:00:00Z", "not_a_date"):
        assert got[bad] is None


def _base_row(**over):
    row = {
        "transaction_id": "t1",
        "event_ts": "2026-01-05T13:45:12Z",
        "event_ts_parsed": datetime(2026, 1, 5, 13, 45, 12),
        "_ingested_at": datetime(2026, 11, 1),
        "amount": "10.00",
        "amount_dec": Decimal("10.00"),
        "currency": "USD",
        "currency_std": "USD",
        "card_token": "a" * 64,
        "merchant_id": "M1",
        "customer_id": "C1",
        "channel_std": "POS",
        "auth_status_std": "APPROVED",
        "_corrupt_record": None,
        "_d_card_token": "a" * 64,
        "_d_card_customer": "C1",
        "_d_merchant_id": "M1",
        "_d_customer_id": "C1",
        "_source_file": "f1",
    }
    row.update(over)
    return row


SCHEMA = (
    "transaction_id string, event_ts string, event_ts_parsed timestamp, _ingested_at timestamp, "
    "amount string, amount_dec decimal(18,2), currency string, currency_std string, "
    "card_token string, merchant_id string, customer_id string, channel_std string, "
    "auth_status_std string, _corrupt_record string, _d_card_token string, "
    "_d_card_customer string, _d_merchant_id string, _d_customer_id string, _source_file string"
)


def _codes(spark, **over):
    cols = SCHEMA.split(", ")
    names = [c.split(" ")[0] for c in cols]
    row = _base_row(**over)
    df = spark.createDataFrame([tuple(row[n] for n in names)], SCHEMA)
    return validate(df).first()["reason_codes"]


def test_validate_reason_codes(spark) -> None:
    assert _codes(spark) == []
    assert _codes(spark, amount="abc", amount_dec=None) == ["AMOUNT_MALFORMED"]
    assert _codes(spark, amount_dec=Decimal("0.00")) == ["AMOUNT_NON_POSITIVE"]
    assert _codes(spark, event_ts_parsed=datetime(2031, 1, 1)) == ["TS_FUTURE"]
    assert _codes(spark, event_ts="??", event_ts_parsed=None) == ["TS_UNPARSEABLE"]
    assert _codes(spark, currency="XXX", currency_std="XXX") == ["CURRENCY_INVALID"]
    assert _codes(spark, _d_merchant_id=None) == ["RI_MERCHANT_UNKNOWN"]
    assert _codes(spark, _d_card_customer="C9") == ["CARD_CUSTOMER_MISMATCH"]
    assert _codes(spark, _corrupt_record="x,y")[0] == "CORRUPT_RECORD"
    assert set(_codes(spark, merchant_id=None, _d_merchant_id=None, channel_std="FAX")) == {
        "NULL_MERCHANT",
        "CHANNEL_INVALID",
    }


def test_flag_duplicates_exact_and_near(spark) -> None:
    rows = [
        (
            "t1",
            "card",
            "M1",
            Decimal("5.00"),
            datetime(2026, 1, 1, 10, 0, 0),
            datetime(2026, 2, 1),
            "f1",
        ),
        (
            "t1",
            "card",
            "M1",
            Decimal("5.00"),
            datetime(2026, 1, 1, 10, 0, 0),
            datetime(2026, 2, 1),
            "f2",
        ),
        (
            "t2",
            "card",
            "M1",
            Decimal("5.00"),
            datetime(2026, 1, 1, 10, 0, 20),
            datetime(2026, 2, 1),
            "f1",
        ),
        (
            "t3",
            "card",
            "M1",
            Decimal("5.00"),
            datetime(2026, 1, 1, 10, 5, 0),
            datetime(2026, 2, 1),
            "f1",
        ),
        (
            "t4",
            "card",
            "M2",
            Decimal("5.00"),
            datetime(2026, 1, 1, 10, 0, 10),
            datetime(2026, 2, 1),
            "f1",
        ),
    ]
    df = spark.createDataFrame(
        rows,
        "transaction_id string, card_token string, merchant_id string, amount_dec decimal(18,2), "
        "event_ts_parsed timestamp, _ingested_at timestamp, _source_file string",
    ).withColumn("reason_codes", F.array().cast("array<string>"))
    out = {
        (r["transaction_id"], r["_source_file"]): r["reason_codes"]
        for r in flag_duplicates(df).collect()
    }
    assert out[("t1", "f1")] == []
    assert out[("t1", "f2")] == ["DUPLICATE_EXACT"]
    assert out[("t2", "f1")] == ["DUPLICATE_NEAR"]
    assert out[("t3", "f1")] == []  # 5 minutes later: a genuine repeat purchase
    assert out[("t4", "f1")] == []  # different merchant
