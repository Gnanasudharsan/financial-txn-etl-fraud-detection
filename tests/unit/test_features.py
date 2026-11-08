from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from fintxn.gold.features import (
    FEATURE_COLUMNS,
    build_features,
    card_velocity,
    customer_history,
    haversine_km,
    mcc_risk,
    merchant_fraud_lagged,
)

pytestmark = pytest.mark.spark

TXN_SCHEMA = (
    "transaction_id string, event_ts timestamp, event_date date, card_token string, "
    "customer_id string, merchant_id string, mcc string, channel string, entry_mode string, "
    "device_id string, latitude double, longitude double, country string, auth_status string, "
    "amount_usd decimal(18,2)"
)


def _txn(
    tid,
    ts,
    card="c1",
    cust="u1",
    merch="m1",
    amt="10.00",
    dev=None,
    lat=40.7,
    lon=-74.0,
    country="US",
    status="APPROVED",
    mcc="5411",
):
    from decimal import Decimal

    return (
        tid,
        ts,
        ts.date(),
        card,
        cust,
        merch,
        mcc,
        "POS",
        "CHIP",
        dev,
        lat,
        lon,
        country,
        status,
        Decimal(amt),
    )


@pytest.fixture
def frame(spark):
    rows = [
        _txn("t1", datetime(2026, 1, 1, 10, 0, 0)),
        _txn("t2", datetime(2026, 1, 1, 10, 30, 0), merch="m2", amt="20.00"),
        _txn("t3", datetime(2026, 1, 1, 12, 0, 0), amt="30.00", status="DECLINED"),
        _txn(
            "t4",
            datetime(2026, 1, 3, 12, 0, 0),
            merch="m2",
            amt="40.00",
            lat=51.5,
            lon=-0.12,
            country="GB",
        ),
    ]
    df = spark.createDataFrame(rows, TXN_SCHEMA)
    return (
        df.withColumn("ts", F.col("event_ts").cast("long"))
        .withColumn("amount_usd", F.col("amount_usd").cast("double"))
        .withColumn("is_fraud", F.lit(0))
        .withColumn("reported_at", F.lit(None).cast("timestamp"))
    )


def test_windows_exclude_current_and_future(frame) -> None:
    out = {r["transaction_id"]: r for r in card_velocity(frame).collect()}
    assert out["t1"]["card_txn_cnt_24h"] == 0  # nothing before the first txn
    assert out["t2"]["card_txn_cnt_1h"] == 1 and out["t2"]["card_amt_sum_1h"] == 10.0
    assert out["t3"]["card_txn_cnt_1h"] == 0  # t2 is 90 min earlier
    assert out["t3"]["card_txn_cnt_24h"] == 2 and out["t3"]["card_amt_sum_24h"] == 30.0
    assert out["t4"]["card_txn_cnt_24h"] == 0 and out["t4"]["card_txn_cnt_7d"] == 3
    assert out["t4"]["card_declines_24h"] == 0
    assert out["t2"]["secs_since_last_txn"] == 1800
    assert out["t4"]["country_changed"] == 1
    assert 5500 < out["t4"]["geo_distance_km"] < 5600  # NYC -> London


def test_customer_history_is_strictly_prior(frame) -> None:
    out = {r["transaction_id"]: r for r in customer_history(frame).collect()}
    assert out["t1"]["cust_txn_count_prior"] == 0 and out["t1"]["amount_zscore"] is None
    assert out["t2"]["amount_to_mean_ratio"] == 2.0  # 20 / mean(10)
    assert out["t1"]["new_merchant_flag"] == 1 and out["t3"]["new_merchant_flag"] == 0
    assert out["t4"]["new_merchant_flag"] == 0  # m2 already seen at t2
    assert all(r["new_device_flag"] == 0 for r in out.values())  # card-present: no device


def test_mcc_risk_uses_training_window_only(spark, frame) -> None:
    df = frame.withColumn("is_fraud", F.when(F.col("transaction_id") == "t4", 1).otherwise(0))
    out = {
        r["transaction_id"]: r["mcc_risk_score"] for r in mcc_risk(df, date(2026, 1, 2)).collect()
    }
    # t4 (the only fraud) is after train_end, so the score must not reflect it
    assert out["t1"] == 0.0 and out["t4"] == 0.0


def test_merchant_rate_only_counts_prior_reports(frame) -> None:
    df = frame.withColumn(
        "is_fraud", F.when(F.col("transaction_id") == "t2", 1).otherwise(0)
    ).withColumn(
        "reported_at", F.when(F.col("transaction_id") == "t2", F.lit(datetime(2026, 1, 2, 9, 0, 0)))
    )
    out = {r["transaction_id"]: r for r in merchant_fraud_lagged(df, prior=0.0).collect()}
    assert out["t2"]["merchant_fraud_rate_lagged"] == 0.0  # same day: nothing known yet
    assert out["t4"]["merchant_txn_count_lagged"] == 1  # t2 on Jan 1
    assert out["t4"]["merchant_fraud_rate_lagged"] > 0  # report dated Jan 2 < Jan 3


def test_haversine_zero_distance(spark) -> None:
    df = spark.createDataFrame([(1.0, 2.0)], "a double, b double")
    assert df.select(haversine_km(F.col("a"), F.col("b"), F.col("a"), F.col("b"))).first()[0] == 0.0


def test_build_features_outputs_all_columns(spark) -> None:
    rows = [
        _txn(f"t{i}", datetime(2026, 1, 1 + i // 3, 9 + i % 3, 0, 0), amt=f"{10 + i}.00")
        for i in range(9)
    ]
    txn = spark.createDataFrame(rows, TXN_SCHEMA)
    labels = spark.createDataFrame(
        [("t5", "card_testing", datetime(2026, 1, 9))],
        "transaction_id string, fraud_type string, reported_at timestamp",
    )
    cust = spark.createDataFrame(
        [("u1", "US", -5)], "customer_id string, home_country string, home_utc_offset int"
    )
    out = build_features(txn, labels, cust, date(2026, 1, 3))
    assert set(FEATURE_COLUMNS) <= set(out.columns)
    assert out.count() == 9 and out.where("is_fraud = 1").count() == 1
    assert out.where("hour_local = 4").count() == 3  # 09:00 UTC at UTC-5
