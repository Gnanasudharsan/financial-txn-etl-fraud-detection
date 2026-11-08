"""Gold job: point-in-time feature table and data-quality marts.

Model-dependent marts (``fct_transactions_scored``, ``daily_fraud_kpis``, ``merchant_risk``,
``customer_risk``) are produced by the scoring job; ``reconciliation_summary`` by the recon job.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import functions as F

from fintxn.audit import JobContext
from fintxn.gold.features import build_features
from fintxn.gold.marts import dq_health, dq_quarantine_by_reason, split_col
from fintxn.logging import get_logger
from fintxn.storage import Lake

log = get_logger("fintxn.gold")
FEATURES_TABLE = "features_transactions"


def optimize(lake: Lake, layer: str, name: str, zorder: list[str] | None = None) -> None:
    """Compact small files and co-locate rows for selective reads (data skipping)."""
    dt = lake.delta_table(layer, name)
    if zorder:
        dt.optimize().executeZOrderBy(*zorder)
    else:
        dt.optimize().executeCompaction()


def run_gold(lake: Lake, ctx: JobContext) -> dict[str, Any]:
    m = lake.settings.model
    txn = lake.read("silver", "transactions")
    feats = build_features(
        txn, lake.read("silver", "fraud_labels"), lake.read("silver", "customers"), m.train_end
    )
    feats = feats.withColumn("split", split_col(m.train_end, m.valid_end))
    version = lake.write(
        feats,
        "gold",
        FEATURES_TABLE,
        mode="overwrite",
        partition_by=["event_date"],
        overwrite_schema=True,
    )
    rows = int(lake.last_operation_metrics("gold", FEATURES_TABLE).get("numOutputRows", 0))
    optimize(lake, "gold", FEATURES_TABLE, zorder=["card_token"])
    ctx.record_write("gold", FEATURES_TABLE, lake.version("gold", FEATURES_TABLE))
    log.info("features: %d rows (version %d before optimize)", rows, version)

    bronze_txn = lake.read("bronze", "transactions")
    quarantine = lake.read("silver", "transactions_quarantine")
    for name, df in (
        ("dq_health", dq_health(bronze_txn, quarantine)),
        ("dq_quarantine_by_reason", dq_quarantine_by_reason(quarantine)),
    ):
        ctx.record_write(
            "gold", name, lake.write(df, "gold", name, mode="overwrite", overwrite_schema=True)
        )

    split_counts = {
        r["split"]: {"rows": int(r["n"]), "fraud": int(r["f"])}
        for r in lake.read("gold", FEATURES_TABLE)
        .groupBy("split")
        .agg(F.count(F.lit(1)).alias("n"), F.sum("is_fraud").alias("f"))
        .collect()
    }
    ctx.rows_in = int(txn.count())
    ctx.rows_out = rows
    ctx.metrics.update({"features_rows": rows, "splits": split_counts})
    return {"features_rows": rows, "splits": split_counts}
