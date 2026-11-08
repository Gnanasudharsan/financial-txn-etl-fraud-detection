"""Export gold marts for BI.

* ``dashboards/data/*.parquet`` - small, aggregated, PII-free extracts committed to git so the
  Streamlit app deploys to Streamlit Community Cloud with no database behind it.
* ``data/exports/powerbi/`` - star-schema extracts (CSV + Parquet) for Power BI Service.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fintxn.audit import AUDIT_TABLE, JobContext
from fintxn.config import PROJECT_ROOT
from fintxn.logging import get_logger
from fintxn.storage import Lake

log = get_logger("fintxn.export")
TOP_MERCHANTS = 500
MAX_AUDIT_ROWS = 500
PII_LIKE = {"email", "phone", "first_name", "last_name", "card_number", "date_of_birth"}


def _abs(p: str | Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _check_no_pii(name: str, pdf: pd.DataFrame) -> None:
    leaked = PII_LIKE & {c.lower() for c in pdf.columns}
    if leaked or any(c.endswith("_enc") for c in pdf.columns):
        raise ValueError(f"refusing to export {name}: PII-like columns {sorted(leaked)}")


def _save(pdf: pd.DataFrame, out: Path, name: str, csv: bool = False) -> dict[str, Any]:
    _check_no_pii(name, pdf)
    out.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(out / f"{name}.parquet", index=False)
    if csv:
        pdf.to_csv(out / f"{name}.csv", index=False)
    return {"rows": len(pdf), "bytes": (out / f"{name}.parquet").stat().st_size}


def _pd(df: DataFrame) -> pd.DataFrame:
    return df.toPandas()


def dashboard_extracts(lake: Lake) -> dict[str, pd.DataFrame]:
    g = lambda n: lake.read("gold", n)  # noqa: E731
    merchants = (
        g("merchant_risk").orderBy(F.desc("fraud_count"), F.desc("avg_score")).limit(TOP_MERCHANTS)
    )
    audit = (
        lake.read("audit", AUDIT_TABLE)
        .select(
            "run_id",
            "job_name",
            "env",
            "git_sha",
            "status",
            "started_at",
            "ended_at",
            "duration_s",
            "rows_in",
            "rows_out",
            "rows_quarantined",
            "dq_pass_rate",
            "tables_written",
        )
        .orderBy(F.desc("started_at"))
        .limit(MAX_AUDIT_ROWS)
    )
    model_by_split = (
        g("fct_transactions_scored")
        .groupBy("split")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("is_fraud").alias("fraud"),
            F.sum("alert_flag").alias("alerts"),
            F.sum(((F.col("alert_flag") == 1) & (F.col("is_fraud") == 1)).cast("int")).alias(
                "true_positives"
            ),
        )
    )
    fraud_type = (
        g("fct_transactions_scored")
        .where("is_fraud = 1")
        .groupBy("split", "fraud_type")
        .agg(
            F.count(F.lit(1)).alias("fraud"),
            F.sum("alert_flag").alias("caught"),
        )
    )
    return {
        "daily_fraud_kpis": _pd(g("daily_fraud_kpis")),
        "merchant_risk_top": _pd(merchants),
        "reconciliation_summary": _pd(g("reconciliation_summary")),
        "dq_health": _pd(g("dq_health")),
        "dq_quarantine_by_reason": _pd(g("dq_quarantine_by_reason")),
        "pipeline_audit": _pd(audit),
        "model_by_split": _pd(model_by_split),
        "fraud_type_capture": _pd(fraud_type),
    }


def powerbi_extracts(lake: Lake) -> dict[str, pd.DataFrame]:
    """Star schema: fact_daily_merchant (grain: date x merchant) + conformed dimensions."""
    fct = lake.read("gold", "fct_transactions_scored")
    fact = fct.groupBy("event_date", "merchant_id", "channel", "split").agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).alias("amount_usd"),
        F.sum("is_fraud").alias("fraud_count"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount_usd")).otherwise(0.0)), 2).alias(
            "fraud_amount_usd"
        ),
        F.sum("alert_flag").alias("alert_count"),
        F.sum(((F.col("alert_flag") == 1) & (F.col("is_fraud") == 1)).cast("int")).alias(
            "true_positive_count"
        ),
    )
    dates = (
        fct.select("event_date")
        .distinct()
        .select(
            F.col("event_date").alias("date"),
            F.year("event_date").alias("year"),
            F.month("event_date").alias("month"),
            F.date_format("event_date", "MMM").alias("month_name"),
            F.weekofyear("event_date").alias("iso_week"),
            F.date_format("event_date", "E").alias("weekday"),
            (F.dayofweek("event_date").isin(1, 7)).alias("is_weekend"),
        )
    )
    merchants = lake.read("silver", "merchants").select(
        "merchant_id",
        "merchant_name",
        "mcc",
        "mcc_description",
        "category",
        "city",
        "country",
        "online_only",
    )
    recon = lake.read("gold", "reconciliation_summary")
    dq = lake.read("gold", "dq_health")
    return {
        "fact_daily_merchant": _pd(fact),
        "dim_date": _pd(dates),
        "dim_merchant": _pd(merchants),
        "fact_recon_daily": _pd(recon),
        "fact_dq_daily": _pd(dq),
    }


def run_export(lake: Lake, ctx: JobContext) -> dict[str, Any]:
    out: dict[str, Any] = {"dashboard": {}, "powerbi": {}}
    dash_dir = _abs(lake.settings.export_dir)
    for name, pdf in dashboard_extracts(lake).items():
        out["dashboard"][name] = _save(pdf, dash_dir, name)
    pbi_dir = _abs(lake.settings.powerbi_dir)
    for name, pdf in powerbi_extracts(lake).items():
        out["powerbi"][name] = _save(pdf, pbi_dir, name, csv=True)
    ctx.rows_out = sum(v["rows"] for d in out.values() for v in d.values())
    ctx.metrics.update(out)
    log.info(
        "exported %d dashboard + %d Power BI tables", len(out["dashboard"]), len(out["powerbi"])
    )
    return out
