"""Batch scoring with the registered ``champion`` model, then the score-dependent gold marts."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from fintxn.audit import JobContext
from fintxn.gold.features import CATEGORICAL_FEATURES, FEATURE_COLUMNS
from fintxn.gold.job import FEATURES_TABLE, optimize
from fintxn.gold.marts import customer_risk, daily_fraud_kpis, merchant_risk
from fintxn.logging import get_logger
from fintxn.ml.train import configure_mlflow
from fintxn.storage import Lake

log = get_logger("fintxn.ml.score")
SCORED_TABLE = "fct_transactions_scored"


def load_champion(lake: Lake) -> tuple[str, str, float]:
    """Return (LightGBM model string, model version, threshold) for the champion alias."""
    import mlflow

    s = lake.settings
    configure_mlflow(s)
    client = mlflow.MlflowClient()
    mv = client.get_model_version_by_alias(s.mlflow.model_name, "champion")
    booster = mlflow.lightgbm.load_model(f"models:/{s.mlflow.model_name}@champion")
    threshold = float(
        mv.tags.get("threshold") or client.get_run(mv.run_id).data.params["threshold"]
    )
    return booster.model_to_string(), str(mv.version), threshold


def score_frame(
    feats: DataFrame, model_str: str, threshold: float, model_version: str
) -> DataFrame:
    out_schema = StructType(
        [
            StructField("transaction_id", StringType()),
            StructField("fraud_score", DoubleType()),
            StructField("alert_flag", IntegerType()),
        ]
    )

    def predict(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import lightgbm as lgb

        booster = lgb.Booster(model_str=model_str)
        for pdf in batches:
            X = pdf[FEATURE_COLUMNS].astype("float32")
            for c in CATEGORICAL_FEATURES:
                X[c] = X[c].astype("int32")
            s = np.asarray(booster.predict(X))
            yield pd.DataFrame(
                {
                    "transaction_id": pdf["transaction_id"],
                    "fraud_score": s,
                    "alert_flag": (s >= threshold).astype("int32"),
                }
            )

    scores = feats.select("transaction_id", *FEATURE_COLUMNS).mapInPandas(
        predict,  # type: ignore[arg-type]
        out_schema,
    )
    return (
        feats.select(
            "transaction_id",
            "event_ts",
            "event_date",
            "card_token",
            "customer_id",
            "merchant_id",
            "mcc",
            "channel",
            "auth_status",
            "amount_usd",
            "is_fraud",
            "fraud_type",
            "split",
        )
        .join(scores, "transaction_id")
        .withColumn("model_version", F.lit(model_version))
        .withColumn("threshold", F.lit(threshold))
        .withColumn("scored_at", F.current_timestamp())
    )


def run_score(lake: Lake, ctx: JobContext) -> dict[str, Any]:
    model_str, version, threshold = load_champion(lake)
    feats = lake.read("gold", FEATURES_TABLE)
    scored = score_frame(feats, model_str, threshold, version)
    lake.write(
        scored,
        "gold",
        SCORED_TABLE,
        mode="overwrite",
        partition_by=["event_date"],
        overwrite_schema=True,
    )
    rows = int(lake.last_operation_metrics("gold", SCORED_TABLE).get("numOutputRows", 0))
    optimize(lake, "gold", SCORED_TABLE, zorder=["merchant_id", "card_token"])
    ctx.record_write("gold", SCORED_TABLE, lake.version("gold", SCORED_TABLE))

    fct = lake.read("gold", SCORED_TABLE)
    alerts = fct.where("alert_flag = 1").select(
        "transaction_id",
        "event_ts",
        "event_date",
        "card_token",
        "customer_id",
        "merchant_id",
        "amount_usd",
        "fraud_score",
        "model_version",
        "is_fraud",
        "split",
    )
    marts = {
        "fraud_alerts": alerts,
        "daily_fraud_kpis": daily_fraud_kpis(fct),
        "merchant_risk": merchant_risk(fct, lake.read("silver", "merchants")),
        "customer_risk": customer_risk(fct, lake.read("silver", "customers")),
    }
    for name, df in marts.items():
        ctx.record_write(
            "gold", name, lake.write(df, "gold", name, mode="overwrite", overwrite_schema=True)
        )

    summary = (
        fct.groupBy("split")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("alert_flag").alias("alerts"),
            F.sum("is_fraud").alias("fraud"),
            F.sum(((F.col("alert_flag") == 1) & (F.col("is_fraud") == 1)).cast("int")).alias("tp"),
        )
        .collect()
    )
    by_split = {
        r["split"]: {
            "rows": int(r["rows"]),
            "alerts": int(r["alerts"]),
            "fraud": int(r["fraud"]),
            "true_positives": int(r["tp"]),
        }
        for r in summary
    }
    ctx.rows_in = rows
    ctx.rows_out = rows
    ctx.metrics.update({"model_version": version, "threshold": threshold, "by_split": by_split})
    return {
        "scored_rows": rows,
        "model_version": version,
        "threshold": threshold,
        "by_split": by_split,
    }
