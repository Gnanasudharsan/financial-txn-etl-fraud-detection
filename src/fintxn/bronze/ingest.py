"""Bronze: raw landing files -> Delta, incrementally and idempotently.

* **Processed-file tracking** - ``bronze._ingest_log`` records every (file, size) ingested;
  only unseen files are read on the next run.
* **Idempotent writes** - every append carries Delta ``txnAppId``/``txnVersion`` (= batch id),
  so a retry after a crash between "data written" and "log written" is a no-op for the data.
* **Raw fidelity** - strings everywhere, ``PERMISSIVE`` parsing, broken lines captured in
  ``_corrupt_record``, unknown JSON keys (schema drift) preserved in ``_rescued_data``.
* **Tokenize on land** - card numbers are replaced by an HMAC token + last4 before anything is
  persisted, so raw PANs never reach the lakehouse (PCI-DSS scope reduction).
* **Dimensions** - upserted with ``MERGE`` on the business key, updating only changed rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructType

from fintxn.audit import JobContext
from fintxn.bronze.schemas import (
    CARDS_SCHEMA,
    CUSTOMERS_SCHEMA,
    LABELS_SCHEMA,
    LEDGER_SCHEMA,
    MERCHANTS_SCHEMA,
    SETTLEMENT_SCHEMA,
    TXN_FIELDS,
    raw_txn_schema,
)
from fintxn.logging import get_logger
from fintxn.security.tokenize import hmac_token_col, last4_col, load_token_key, redact_pans_col
from fintxn.storage import Lake, ObjectInfo

log = get_logger("fintxn.bronze")
INGEST_LOG = "_ingest_log"


@dataclass(frozen=True)
class FileDataset:
    name: str
    prefix: str
    schema: StructType
    partition_key: str | None  # hive-style key in the path used as _landing_date


FILE_DATASETS = (
    FileDataset("fraud_labels", "labels/", LABELS_SCHEMA, None),
    FileDataset("ledger", "ledger/", LEDGER_SCHEMA, "posted_date"),
    FileDataset("settlement", "settlement/", SETTLEMENT_SCHEMA, "settlement_date"),
)


@dataclass(frozen=True)
class DimDataset:
    name: str
    key: str
    schema: StructType
    path: str


DIM_DATASETS = (
    DimDataset("customers", "customer_id", CUSTOMERS_SCHEMA, "dims/customers.csv"),
    DimDataset("cards", "card_id", CARDS_SCHEMA, "dims/cards.csv"),
    DimDataset("merchants", "merchant_id", MERCHANTS_SCHEMA, "dims/merchants.csv"),
)


# ------------------------------------------------------------------------------------------
# file tracking
# ------------------------------------------------------------------------------------------
def _processed(lake: Lake) -> tuple[set[tuple[str, int]], int]:
    if not lake.exists("bronze", INGEST_LOG):
        return set(), 0
    df = lake.read("bronze", INGEST_LOG)
    rows = df.select("source_key", "size").collect()
    max_batch = df.agg(F.max("batch_id")).first()
    last = int(max_batch[0]) if max_batch and max_batch[0] is not None else 0
    return {(r["source_key"], int(r["size"])) for r in rows}, last


def discover(
    lake: Lake, prefix: str, seen: set[tuple[str, int]], suffixes: tuple[str, ...]
) -> list[ObjectInfo]:
    objs = lake.store.list_objects("raw", prefix)
    return [o for o in objs if o.key.endswith(suffixes) and (o.key, o.size) not in seen]


def _log_files(
    lake: Lake, ctx: JobContext, dataset: str, files: list[ObjectInfo], batch_id: int
) -> None:
    rows = [
        (dataset, f.key, lake.store.uri("raw", f.key), f.size, batch_id, ctx.run_id) for f in files
    ]
    df = lake.spark.createDataFrame(
        rows,
        "dataset string, source_key string, source_uri string, size long, batch_id long, "
        "run_id string",
    ).withColumn("ingested_at", F.current_timestamp())
    lake.write(df, "bronze", INGEST_LOG, mode="append")


# ------------------------------------------------------------------------------------------
# readers
# ------------------------------------------------------------------------------------------
def _with_meta(
    df: DataFrame, ctx: JobContext, batch_id: int, partition_key: str | None
) -> DataFrame:
    df = (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(ctx.run_id))
        .withColumn("_batch_id", F.lit(batch_id).cast("long"))
    )
    if partition_key:
        landing = F.regexp_extract("_source_file", rf"{partition_key}=(\d{{4}}-\d{{2}}-\d{{2}})", 1)
        df = df.withColumn("_landing_date", F.to_date(landing))
    else:
        df = df.withColumn("_landing_date", F.current_date())
    return df


def read_csv(spark: SparkSession, paths: list[str], schema: StructType) -> DataFrame:
    return (
        spark.read.schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("escape", '"')
        .csv(paths)
        .select("*", "_metadata")
    )


def read_txn_csv(spark: SparkSession, paths: list[str]) -> DataFrame:
    return read_csv(spark, paths, raw_txn_schema(True)).withColumn(
        "_rescued_data", F.lit(None).cast("string")
    )


def read_txn_json(spark: SparkSession, paths: list[str]) -> DataFrame:
    """JSON lines read as text, then parsed, so drifted keys and broken lines are both kept."""
    text = spark.read.text(paths).select("value", "_metadata")
    schema = raw_txn_schema(True)
    parsed = F.from_json(
        "value", schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
    )
    as_map = F.from_json("value", MapType(StringType(), StringType()))  # type: ignore[arg-type]
    known = F.array(*[F.lit(f) for f in TXN_FIELDS])
    extra = F.map_filter(as_map, lambda k, _v: ~F.array_contains(known, k))
    return (
        text.withColumn("_p", parsed)
        .withColumn("_extra", extra)
        .select(
            *[F.col(f"_p.{f}").alias(f) for f in TXN_FIELDS],
            F.col("_p._corrupt_record").alias("_corrupt_record"),
            F.when(F.size("_extra") > 0, F.to_json("_extra")).alias("_rescued_data"),
            "_metadata",
        )
    )


def tokenize_pan(df: DataFrame, key: bytes, col: str = "card_number") -> DataFrame:
    out = df.withColumn("card_token", hmac_token_col(col, key)).withColumn(
        "card_last4", last4_col(col)
    )
    if "_corrupt_record" in df.columns:
        out = out.withColumn("_corrupt_record", redact_pans_col("_corrupt_record"))
    return out.drop(col)


# ------------------------------------------------------------------------------------------
# jobs
# ------------------------------------------------------------------------------------------
def _write_batch(lake: Lake, df: DataFrame, name: str, batch_id: int, full_refresh: bool) -> int:
    version = lake.write(
        df,
        "bronze",
        name,
        mode="overwrite" if full_refresh else "append",
        partition_by=["_landing_date"],
        overwrite_schema=full_refresh,
        options={"txnAppId": f"fintxn_bronze_{name}", "txnVersion": str(batch_id)},
    )
    return version


def ingest_transactions(
    lake: Lake,
    ctx: JobContext,
    key: bytes,
    seen: set[tuple[str, int]],
    batch_id: int,
    full_refresh: bool,
) -> dict[str, Any]:
    new = discover(lake, "transactions/", seen, (".csv", ".json"))
    if not new:
        log.info("transactions: no new files")
        return {"files": 0, "rows": 0}
    csv_paths = [lake.store.uri("raw", o.key) for o in new if o.key.endswith(".csv")]
    json_paths = [lake.store.uri("raw", o.key) for o in new if o.key.endswith(".json")]
    parts: list[DataFrame] = []
    if csv_paths:
        parts.append(read_txn_csv(lake.spark, csv_paths).withColumn("_source_format", F.lit("csv")))
    if json_paths:
        parts.append(
            read_txn_json(lake.spark, json_paths).withColumn("_source_format", F.lit("json"))
        )
    df = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    df = df.withColumn(
        "_source_system",
        F.when(
            F.col("_metadata.file_path").contains("source=card_present"), "card_present"
        ).otherwise("card_not_present"),
    )
    df = tokenize_pan(_with_meta(df, ctx, batch_id, "event_date"), key).drop("_metadata")
    version = _write_batch(lake, df, "transactions", batch_id, full_refresh)
    rows = int(lake.last_operation_metrics("bronze", "transactions").get("numOutputRows", 0))
    ctx.record_write("bronze", "transactions", version)
    _log_files(lake, ctx, "transactions", new, batch_id)
    return {
        "files": len(new),
        "rows": rows,
        "csv_files": len(csv_paths),
        "json_files": len(json_paths),
    }


def ingest_file_dataset(
    lake: Lake,
    ctx: JobContext,
    ds: FileDataset,
    seen: set[tuple[str, int]],
    batch_id: int,
    full_refresh: bool,
) -> dict[str, Any]:
    new = discover(lake, ds.prefix, seen, (".csv",))
    if not new:
        log.info("%s: no new files", ds.name)
        return {"files": 0, "rows": 0}
    df = read_csv(lake.spark, [lake.store.uri("raw", o.key) for o in new], ds.schema)
    df = _with_meta(df, ctx, batch_id, ds.partition_key).drop("_metadata")
    version = _write_batch(lake, df, ds.name, batch_id, full_refresh)
    rows = int(lake.last_operation_metrics("bronze", ds.name).get("numOutputRows", 0))
    ctx.record_write("bronze", ds.name, version)
    _log_files(lake, ctx, ds.name, new, batch_id)
    return {"files": len(new), "rows": rows}


def upsert_dimension(
    lake: Lake, ctx: JobContext, ds: DimDataset, key: bytes, batch_id: int
) -> dict[str, Any]:
    path = lake.store.uri("raw", ds.path)
    df = read_csv(lake.spark, [path], ds.schema)
    df = _with_meta(df, ctx, batch_id, None).drop("_metadata", "_corrupt_record", "_landing_date")
    if ds.name == "cards":
        df = tokenize_pan(df, key)
    business_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn(
        "_row_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in business_cols]), 256),
    )
    if not lake.exists("bronze", ds.name):
        version = lake.write(df, "bronze", ds.name, mode="overwrite")
        metrics = {
            "inserted": int(lake.last_operation_metrics("bronze", ds.name).get("numOutputRows", 0)),
            "updated": 0,
        }
    else:
        (
            lake.delta_table("bronze", ds.name)
            .alias("t")
            .merge(df.alias("s"), f"t.{ds.key} = s.{ds.key}")
            .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        m = lake.last_operation_metrics("bronze", ds.name)
        version = lake.version("bronze", ds.name)
        metrics = {
            "inserted": int(m.get("numTargetRowsInserted", 0)),
            "updated": int(m.get("numTargetRowsUpdated", 0)),
        }
    ctx.record_write("bronze", ds.name, version)
    return metrics


def run_bronze(lake: Lake, ctx: JobContext, full_refresh: bool = False) -> dict[str, Any]:
    key = load_token_key()
    seen, last_batch = _processed(lake)
    if full_refresh:
        seen = set()
    batch_id = last_batch + 1
    results: dict[str, Any] = {"batch_id": batch_id}
    for dim in DIM_DATASETS:
        results[dim.name] = upsert_dimension(lake, ctx, dim, key, batch_id)
    if full_refresh and lake.exists("bronze", INGEST_LOG):
        lake.write(lake.read("bronze", INGEST_LOG).limit(0), "bronze", INGEST_LOG, mode="overwrite")
    results["transactions"] = ingest_transactions(lake, ctx, key, seen, batch_id, full_refresh)
    for ds in FILE_DATASETS:
        results[ds.name] = ingest_file_dataset(lake, ctx, ds, seen, batch_id, full_refresh)
    ctx.rows_out = sum(int(v.get("rows", 0)) for v in results.values() if isinstance(v, dict))
    ctx.rows_in = ctx.rows_out
    ctx.metrics.update(results)
    return results
