"""Silver job: conformed dimensions, cleaned transactions, quarantine, DQ results.

* Dimensions are rebuilt from bronze each run (small), with PII encrypted (AES-GCM) and
  analyst-safe masked columns alongside.
* Transactions are processed incrementally by bronze ``_batch_id`` watermark and MERGEd on
  ``transaction_id`` (insert-only), so replays across batches never duplicate silver rows.
* Tables are created up front from explicit StructTypes, with Delta CHECK constraints; every
  write is then subject to Delta schema enforcement.
"""

from __future__ import annotations

from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from fintxn.audit import JobContext
from fintxn.generate.catalog import FX_PER_USD
from fintxn.logging import get_logger
from fintxn.quality.gx_suite import run_suite
from fintxn.security.crypto import encrypt_col, load_encryption_key, mask_email_col, mask_phone_col
from fintxn.silver.schemas import (
    LEDGER_CONSTRAINTS,
    MONEY,
    QUARANTINE_SCHEMA,
    SETTLEMENT_CONSTRAINTS,
    SILVER_CARDS_SCHEMA,
    SILVER_CUSTOMERS_SCHEMA,
    SILVER_LABELS_SCHEMA,
    SILVER_LEDGER_SCHEMA,
    SILVER_MERCHANTS_SCHEMA,
    SILVER_SETTLEMENT_SCHEMA,
    SILVER_TXN_SCHEMA,
    TXN_CONSTRAINTS,
)
from fintxn.silver.transform import conform_schema, prepare, to_quarantine, to_silver
from fintxn.spark import on_databricks
from fintxn.storage import Lake

log = get_logger("fintxn.silver")
PII_COLUMNS = ("first_name", "last_name", "email", "phone", "date_of_birth")


# ------------------------------------------------------------------------------------------
# table management
# ------------------------------------------------------------------------------------------
def ensure_table(
    lake: Lake,
    layer: str,
    name: str,
    schema: StructType,
    partition_by: list[str] | None = None,
    constraints: dict[str, str] | None = None,
) -> None:
    from delta.tables import DeltaTable

    builder = DeltaTable.createIfNotExists(lake.spark).addColumns(schema)
    if lake.uses_catalog:
        lake.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lake.schema_name(layer)}")
        builder = builder.tableName(lake.ref(layer, name))
    else:
        builder = builder.location(lake.location(layer, name))
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    builder.execute()
    if constraints:
        props = lake.delta_table(layer, name).detail().select("properties").first()
        existing = set((props["properties"] or {}).keys()) if props else set()
        for cname, expr in constraints.items():
            if f"delta.constraints.{cname}" not in existing:
                lake.sql(
                    f"ALTER TABLE {lake.ref(layer, name)} ADD CONSTRAINT {cname} CHECK ({expr})"
                )


def _overwrite(
    lake: Lake,
    ctx: JobContext,
    df: DataFrame,
    name: str,
    schema: StructType,
    constraints: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
) -> int:
    ensure_table(lake, "silver", name, schema, partition_by, constraints)
    version = lake.write(
        conform_schema(df, schema), "silver", name, mode="overwrite", partition_by=partition_by
    )
    ctx.record_write("silver", name, version)
    return int(lake.last_operation_metrics("silver", name).get("numOutputRows", 0))


# ------------------------------------------------------------------------------------------
# dimensions & reference data
# ------------------------------------------------------------------------------------------
def build_customers(bronze: DataFrame, key: bytes) -> DataFrame:
    df = bronze
    for c in PII_COLUMNS:
        df = df.withColumn(f"{c}_enc", encrypt_col(c, key))
    return (
        df.withColumn("email_masked", mask_email_col("email"))
        .withColumn("phone_masked", mask_phone_col("phone"))
        .withColumn("birth_year", F.year(F.to_date("date_of_birth")))
        .withColumn("home_utc_offset", F.col("home_utc_offset").cast("int"))
        .withColumn("home_lat", F.col("home_lat").cast("double"))
        .withColumn("home_lon", F.col("home_lon").cast("double"))
        .withColumn("signup_date", F.to_date("signup_date"))
    )


def build_cards(bronze: DataFrame) -> DataFrame:
    return bronze.withColumn("issue_date", F.to_date("issue_date")).withColumn(
        "credit_limit", F.col("credit_limit").cast(MONEY)
    )


def build_merchants(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumn("merchant_name", F.trim("merchant_name"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("online_only", F.lower("online_only") == "true")
        .withColumn("onboarded_date", F.to_date("onboarded_date"))
    )


def build_fx(lake: Lake) -> DataFrame:
    rows = [(k, float(v)) for k, v in sorted(FX_PER_USD.items())]
    return lake.spark.createDataFrame(rows, "currency string, units_per_usd double")


# ------------------------------------------------------------------------------------------
# facts
# ------------------------------------------------------------------------------------------
def build_ledger(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumn("amount", F.col("amount").cast(MONEY))
        .withColumn("posted_date", F.to_date("posted_date"))
        .where("amount IS NOT NULL AND posted_date IS NOT NULL AND transaction_id IS NOT NULL")
        .dropDuplicates(["ledger_entry_id"])
    )


def build_settlement(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumn("settled_amount", F.col("settled_amount").cast(MONEY))
        .withColumn("settlement_date", F.to_date("settlement_date"))
        .where("settled_amount IS NOT NULL AND settlement_date IS NOT NULL")
        .dropDuplicates(["settlement_id"])
    )


def build_labels(bronze: DataFrame) -> DataFrame:
    df = bronze.withColumn("reported_at", F.to_timestamp("reported_at", "yyyy-MM-dd'T'HH:mm:ssXXX"))
    return df.where("transaction_id IS NOT NULL AND reported_at IS NOT NULL").dropDuplicates(
        ["transaction_id"]
    )


def _watermark(lake: Lake) -> int:
    wm = 0
    for name in ("transactions", "transactions_quarantine"):
        if lake.exists("silver", name):
            v = lake.read("silver", name).agg(F.max("_batch_id")).first()
            if v and v[0] is not None:
                wm = max(wm, int(v[0]))
    return wm


def _persist(df: DataFrame) -> DataFrame:
    # Serverless Databricks does not support caching; locally it avoids recomputing the lineage.
    return df if on_databricks() else df.persist(StorageLevel.MEMORY_AND_DISK)


def process_transactions(lake: Lake, ctx: JobContext, full_refresh: bool) -> dict[str, Any]:
    ensure_table(lake, "silver", "transactions", SILVER_TXN_SCHEMA, ["event_date"], TXN_CONSTRAINTS)
    ensure_table(lake, "silver", "transactions_quarantine", QUARANTINE_SCHEMA)
    wm = 0 if full_refresh else _watermark(lake)
    bronze = lake.read("bronze", "transactions").where(F.col("_batch_id") > wm)
    cards = lake.read("silver", "cards")
    merchants = lake.read("silver", "merchants")
    customers = lake.read("silver", "customers")

    prepared = _persist(prepare(bronze, cards, merchants, customers))
    rows_in = prepared.count()
    if rows_in == 0:
        log.info("silver: no new bronze batches after %d", wm)
        return {"rows_in": 0, "rows_out": 0, "quarantined": 0, "watermark": wm}

    valid = to_silver(prepared)
    bad = to_quarantine(prepared, ctx.run_id)
    first_load = full_refresh or lake.read("silver", "transactions").limit(1).count() == 0
    if first_load:
        lake.write(valid, "silver", "transactions", mode="overwrite", partition_by=["event_date"])
        rows_out = int(
            lake.last_operation_metrics("silver", "transactions").get("numOutputRows", 0)
        )
    else:
        (
            lake.delta_table("silver", "transactions")
            .alias("t")
            .merge(
                valid.alias("s"),
                "t.transaction_id = s.transaction_id AND t.event_date = s.event_date",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        rows_out = int(
            lake.last_operation_metrics("silver", "transactions").get("numTargetRowsInserted", 0)
        )
    ctx.record_write("silver", "transactions", lake.version("silver", "transactions"))

    lake.write(
        bad, "silver", "transactions_quarantine", mode="overwrite" if full_refresh else "append"
    )
    quarantined = int(
        lake.last_operation_metrics("silver", "transactions_quarantine").get("numOutputRows", 0)
    )
    ctx.record_write(
        "silver", "transactions_quarantine", lake.version("silver", "transactions_quarantine")
    )

    reasons = (
        lake.read("silver", "transactions_quarantine")
        .where(F.col("_run_id") == ctx.run_id)
        .groupBy("primary_reason")
        .count()
        .collect()
    )
    prepared.unpersist()
    return {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "quarantined": quarantined,
        "watermark": wm,
        "quarantine_by_reason": {r["primary_reason"]: int(r["count"]) for r in reasons},
    }


def run_gx(lake: Lake, ctx: JobContext) -> dict[str, Any]:
    published = lake.read("silver", "transactions")
    merchant_ids = [r[0] for r in lake.read("silver", "merchants").select("merchant_id").collect()]
    customer_ids = [r[0] for r in lake.read("silver", "customers").select("customer_id").collect()]
    res = run_suite(published, merchant_ids, customer_ids, min_rows=1)
    rows = [
        (ctx.run_id, res["suite"], r["expectation"], r["column"], r["success"],
         r["element_count"], r["unexpected_count"], r["unexpected_percent"], r["observed_value"])
        for r in res["results"]
    ]  # fmt: skip
    df = lake.spark.createDataFrame(
        rows,
        "run_id string, suite string, expectation string, column string, success boolean, "
        "element_count long, unexpected_count long, unexpected_percent double, "
        "observed_value string",
    ).withColumn("validated_at", F.current_timestamp())
    version = lake.write(df, "silver", "_dq_results", mode="append")
    ctx.record_write("silver", "_dq_results", version)
    failed = [f"{r['expectation']}({r['column']})" for r in res["results"] if not r["success"]]
    return {
        "success": res["success"],
        "evaluated": res["evaluated"],
        "passed": res["passed"],
        "failed": failed,
    }


def run_silver(
    lake: Lake, ctx: JobContext, full_refresh: bool = False, run_gx_suite: bool = True
) -> dict[str, Any]:
    key = load_encryption_key()
    out: dict[str, Any] = {}
    out["customers"] = _overwrite(
        lake,
        ctx,
        build_customers(lake.read("bronze", "customers"), key),
        "customers",
        SILVER_CUSTOMERS_SCHEMA,
    )
    out["cards"] = _overwrite(
        lake, ctx, build_cards(lake.read("bronze", "cards")), "cards", SILVER_CARDS_SCHEMA
    )
    out["merchants"] = _overwrite(
        lake,
        ctx,
        build_merchants(lake.read("bronze", "merchants")),
        "merchants",
        SILVER_MERCHANTS_SCHEMA,
    )
    version = lake.write(build_fx(lake), "silver", "ref_fx_rates", mode="overwrite")
    ctx.record_write("silver", "ref_fx_rates", version)

    txn = process_transactions(lake, ctx, full_refresh)
    out["transactions"] = txn
    out["ledger"] = _overwrite(
        lake,
        ctx,
        build_ledger(lake.read("bronze", "ledger")),
        "ledger",
        SILVER_LEDGER_SCHEMA,
        LEDGER_CONSTRAINTS,
    )
    out["settlement"] = _overwrite(
        lake,
        ctx,
        build_settlement(lake.read("bronze", "settlement")),
        "settlement",
        SILVER_SETTLEMENT_SCHEMA,
        SETTLEMENT_CONSTRAINTS,
    )
    out["fraud_labels"] = _overwrite(
        lake,
        ctx,
        build_labels(lake.read("bronze", "fraud_labels")),
        "fraud_labels",
        SILVER_LABELS_SCHEMA,
    )
    if run_gx_suite and txn["rows_in"]:
        out["gx"] = run_gx(lake, ctx)
        ctx.metrics["gx_pass_rate"] = round(out["gx"]["passed"] / max(out["gx"]["evaluated"], 1), 4)
    ctx.rows_in = int(txn["rows_in"])
    ctx.rows_out = int(txn["rows_out"])
    ctx.rows_quarantined = int(txn["quarantined"])
    if txn["rows_in"]:
        ctx.dq_pass_rate = round(txn["rows_out"] / txn["rows_in"], 6)
    ctx.metrics.update(out)
    return out
