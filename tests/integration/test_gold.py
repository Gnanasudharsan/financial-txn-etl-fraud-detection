from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from fintxn.gold.features import FEATURE_COLUMNS
from fintxn.gold.job import FEATURES_TABLE

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_one_feature_row_per_silver_transaction(lake, gold_result, silver_result) -> None:
    assert gold_result["features_rows"] == silver_result["transactions"]["rows_out"]
    feats = lake.read("gold", FEATURES_TABLE)
    assert feats.select("transaction_id").distinct().count() == gold_result["features_rows"]
    assert set(FEATURE_COLUMNS) <= set(feats.columns)


def test_labels_and_splits(lake, gold_result) -> None:
    splits = gold_result["splits"]
    assert set(splits) == {"train", "valid", "test"}
    assert sum(s["fraud"] for s in splits.values()) > 0


def test_velocity_counts_never_negative_and_first_txn_zero(lake, gold_result) -> None:
    feats = lake.read("gold", FEATURES_TABLE)
    assert feats.where(F.col("card_txn_cnt_24h") < 0).count() == 0
    first = feats.where(F.col("secs_since_last_txn").isNull())
    assert first.where(F.col("card_txn_cnt_7d") > 0).count() == 0


def test_dq_marts_reconcile_with_silver(lake, gold_result, silver_result) -> None:
    dq = (
        lake.read("gold", "dq_health")
        .agg(F.sum("rows_total").alias("t"), F.sum("rows_quarantined").alias("q"))
        .first()
    )
    assert dq["q"] == silver_result["transactions"]["quarantined"]
    assert dq["t"] == silver_result["transactions"]["rows_in"]
    by_reason = lake.read("gold", "dq_quarantine_by_reason").agg(F.sum("rows")).first()[0]
    assert by_reason == dq["q"]
