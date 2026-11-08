from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from fintxn.ml.score import SCORED_TABLE

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_training_logs_and_registers(train_result) -> None:
    import mlflow

    assert 0 < train_result["threshold"] < 1
    assert train_result["model_version"] >= 1
    run = mlflow.get_run(train_result["mlflow_run_id"])
    assert "test_pr_auc" in run.data.metrics and "valid_recall" in run.data.metrics
    assert run.data.tags["data.gold.features_transactions.version"] == str(
        train_result["features_table_version"]
    )
    # the threshold was tuned to the recall target on validation
    assert train_result["valid"]["recall"] >= train_result["target_recall"]


def test_scoring_covers_every_row_and_flags_alerts(lake, score_result, gold_result) -> None:
    assert score_result["scored_rows"] == gold_result["features_rows"]
    fct = lake.read("gold", SCORED_TABLE)
    assert fct.where(F.col("fraud_score").isNull()).count() == 0
    assert fct.where("fraud_score < 0 OR fraud_score > 1").count() == 0
    alerts = lake.read("gold", "fraud_alerts").count()
    assert alerts == fct.where("alert_flag = 1").count() > 0


def test_marts_built(lake, score_result) -> None:
    kpis = lake.read("gold", "daily_fraud_kpis")
    assert kpis.agg(F.sum("txn_count")).first()[0] == score_result["scored_rows"]
    assert lake.read("gold", "merchant_risk").where("risk_tier IS NULL").count() == 0
    cr = lake.read("gold", "customer_risk")
    assert not {"email", "first_name", "phone"} & set(cr.columns)
