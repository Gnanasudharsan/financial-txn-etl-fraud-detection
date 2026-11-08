"""Pure DataFrame transforms for silver (unit-testable without any storage).

Pipeline: ``standardize`` -> ``attach_dimensions`` -> ``validate`` -> ``flag_duplicates`` ->
split into ``to_silver`` (valid rows, explicit schema) and ``to_quarantine`` (reason codes).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from fintxn.generate.catalog import FX_PER_USD, VALID_CURRENCIES
from fintxn.silver.schemas import (
    AUTH_STATUSES,
    CHANNELS,
    MONEY,
    QUARANTINE_SCHEMA,
    SILVER_TXN_SCHEMA,
)

AMOUNT_RE = r"^[0-9]+(\.[0-9]{1,2})?$"
THOUSANDS_RE = r"^[0-9]{1,3}(,[0-9]{3})+(\.[0-9]{1,2})?$"
EPOCH_MS_RE = r"^[0-9]{12,14}$"
NEAR_DUP_SECONDS = 60
FUTURE_TOLERANCE = "INTERVAL 1 DAY"

# Ordered by severity: the first matching code becomes ``primary_reason``.
REASON_CODES = (
    "CORRUPT_RECORD",
    "NULL_TRANSACTION_ID",
    "NULL_EVENT_TS",
    "TS_UNPARSEABLE",
    "TS_FUTURE",
    "NULL_AMOUNT",
    "AMOUNT_MALFORMED",
    "AMOUNT_NON_POSITIVE",
    "NULL_CURRENCY",
    "CURRENCY_INVALID",
    "NULL_CARD",
    "NULL_MERCHANT",
    "NULL_CUSTOMER",
    "CHANNEL_INVALID",
    "AUTH_STATUS_INVALID",
    "RI_CARD_UNKNOWN",
    "RI_MERCHANT_UNKNOWN",
    "RI_CUSTOMER_UNKNOWN",
    "CARD_CUSTOMER_MISMATCH",
    "DUPLICATE_EXACT",
    "DUPLICATE_NEAR",
)


def clean_amount(col: str | Column) -> Column:
    """Strip currency symbols/whitespace and thousands separators; null if still malformed."""
    c = F.col(col) if isinstance(col, str) else col
    s = F.regexp_replace(F.trim(c), r"(\$|USD|\s)", "")
    s = F.when(s.rlike(THOUSANDS_RE), F.regexp_replace(s, ",", "")).otherwise(s)
    return F.when(s.rlike(AMOUNT_RE), s.cast(MONEY))


def parse_event_ts(col: str | Column) -> Column:
    """Parse ISO-8601 (``Z`` or ``+hh:mm``), naive ``yyyy-MM-dd HH:mm:ss`` (UTC) or epoch ms."""
    c = F.col(col) if isinstance(col, str) else col
    s = F.trim(c)
    return F.coalesce(
        F.when(s.rlike(EPOCH_MS_RE), (s.cast("long") / 1000).cast("timestamp")),
        F.try_to_timestamp(s, F.lit("yyyy-MM-dd'T'HH:mm:ssXXX")),
        F.try_to_timestamp(s, F.lit("yyyy-MM-dd HH:mm:ss")),
    )


def fx_rate_col(currency: Column) -> Column:
    pairs: list[Column] = []
    for k, v in sorted(FX_PER_USD.items()):
        pairs.extend([F.lit(k), F.lit(v)])
    return F.element_at(F.create_map(*pairs), currency)


def standardize(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("amount_dec", clean_amount("amount"))
        .withColumn("event_ts_parsed", parse_event_ts("event_ts"))
        .withColumn("currency_std", F.upper(F.trim("currency")))
        .withColumn("channel_std", F.upper(F.trim("channel")))
        .withColumn("entry_mode_std", F.upper(F.trim("entry_mode")))
        .withColumn("auth_status_std", F.upper(F.trim("auth_status")))
        .withColumn("city_std", F.initcap(F.regexp_replace(F.trim("city"), r"\s+", " ")))
        .withColumn("country_std", F.upper(F.trim("country")))
    )


def attach_dimensions(
    df: DataFrame, cards: DataFrame, merchants: DataFrame, customers: DataFrame
) -> DataFrame:
    """Left-join conformed dimensions (broadcast: they are small) for RI checks + master data."""
    c = cards.select(
        F.col("card_token").alias("_d_card_token"), F.col("customer_id").alias("_d_card_customer")
    )
    m = merchants.select(
        F.col("merchant_id").alias("_d_merchant_id"),
        F.col("merchant_name").alias("_d_merchant_name"),
        F.col("mcc").alias("_d_mcc"),
    )
    u = customers.select(F.col("customer_id").alias("_d_customer_id"))
    return (
        df.join(F.broadcast(c), df["card_token"] == c["_d_card_token"], "left")
        .join(F.broadcast(m), df["merchant_id"] == m["_d_merchant_id"], "left")
        .join(F.broadcast(u), df["customer_id"] == u["_d_customer_id"], "left")
    )


def _checks() -> list[tuple[str, Column]]:
    cur = sorted(VALID_CURRENCIES)
    return [
        ("CORRUPT_RECORD", F.col("_corrupt_record").isNotNull()),
        ("NULL_TRANSACTION_ID", F.col("transaction_id").isNull()),
        ("NULL_EVENT_TS", F.col("event_ts").isNull()),
        ("TS_UNPARSEABLE", F.col("event_ts").isNotNull() & F.col("event_ts_parsed").isNull()),
        ("TS_FUTURE", F.col("event_ts_parsed") > F.col("_ingested_at") + F.expr(FUTURE_TOLERANCE)),
        ("NULL_AMOUNT", F.col("amount").isNull()),
        ("AMOUNT_MALFORMED", F.col("amount").isNotNull() & F.col("amount_dec").isNull()),
        ("AMOUNT_NON_POSITIVE", F.col("amount_dec") <= 0),
        ("NULL_CURRENCY", F.col("currency").isNull()),
        ("CURRENCY_INVALID", F.col("currency").isNotNull() & ~F.col("currency_std").isin(cur)),
        ("NULL_CARD", F.col("card_token").isNull()),
        ("NULL_MERCHANT", F.col("merchant_id").isNull()),
        ("NULL_CUSTOMER", F.col("customer_id").isNull()),
        ("CHANNEL_INVALID", ~F.coalesce(F.col("channel_std").isin(list(CHANNELS)), F.lit(False))),
        (
            "AUTH_STATUS_INVALID",
            ~F.coalesce(F.col("auth_status_std").isin(list(AUTH_STATUSES)), F.lit(False)),
        ),
        ("RI_CARD_UNKNOWN", F.col("card_token").isNotNull() & F.col("_d_card_token").isNull()),
        (
            "RI_MERCHANT_UNKNOWN",
            F.col("merchant_id").isNotNull() & F.col("_d_merchant_id").isNull(),
        ),
        (
            "RI_CUSTOMER_UNKNOWN",
            F.col("customer_id").isNotNull() & F.col("_d_customer_id").isNull(),
        ),
        (
            "CARD_CUSTOMER_MISMATCH",
            F.col("_d_card_customer").isNotNull()
            & F.col("customer_id").isNotNull()
            & (F.col("_d_card_customer") != F.col("customer_id")),
        ),
    ]


def validate(df: DataFrame) -> DataFrame:
    """Add ``reason_codes`` (array, possibly empty) from row-level rules."""
    flags = [F.when(F.coalesce(cond, F.lit(False)), F.lit(code)) for code, cond in _checks()]
    return df.withColumn("reason_codes", F.filter(F.array(*flags), lambda x: x.isNotNull()))


def flag_duplicates(df: DataFrame) -> DataFrame:
    """Window-based dedup among otherwise-valid rows.

    * exact: same ``transaction_id`` seen again (replayed message) -> keep the first ingested
    * near:  different id but same card, merchant and amount within 60 s (double submit)
    """
    valid = F.size("reason_codes") == 0
    w_exact = Window.partitionBy("transaction_id", F.when(valid, 1)).orderBy(
        "_ingested_at", "_source_file", "event_ts_parsed"
    )
    df = df.withColumn("_rn", F.when(valid, F.row_number().over(w_exact)))
    df = df.withColumn(
        "reason_codes",
        F.when(
            F.col("_rn") > 1, F.array_union("reason_codes", F.array(F.lit("DUPLICATE_EXACT")))
        ).otherwise(F.col("reason_codes")),
    )
    still_valid = F.size("reason_codes") == 0
    w_near = Window.partitionBy(
        "card_token", "merchant_id", "amount_dec", F.when(still_valid, 1)
    ).orderBy("event_ts_parsed", "transaction_id")
    prev_ts = F.lag("event_ts_parsed").over(w_near)
    gap = F.col("event_ts_parsed").cast("long") - prev_ts.cast("long")
    df = df.withColumn("_near", still_valid & (gap <= NEAR_DUP_SECONDS))
    df = df.withColumn(
        "reason_codes",
        F.when(
            F.col("_near"), F.array_union("reason_codes", F.array(F.lit("DUPLICATE_NEAR")))
        ).otherwise(F.col("reason_codes")),
    )
    return df.drop("_rn", "_near")


def conform_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Project to an explicit StructType (order + types). Missing columns fail loudly."""
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise ValueError(f"cannot conform: missing columns {missing}")
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def to_silver(df: DataFrame) -> DataFrame:
    valid = df.where(F.size("reason_codes") == 0)
    out = (
        valid.withColumn("event_ts", F.col("event_ts_parsed"))
        .withColumn("event_date", F.to_date("event_ts_parsed"))
        .withColumn("amount", F.col("amount_dec"))
        .withColumn("currency", F.col("currency_std"))
        .withColumn(
            "amount_usd", F.round(F.col("amount_dec") / fx_rate_col(F.col("currency_std")), 2)
        )
        .withColumn("merchant_name", F.col("_d_merchant_name"))
        .withColumn("mcc", F.col("_d_mcc"))
        .withColumn("channel", F.col("channel_std"))
        .withColumn("entry_mode", F.col("entry_mode_std"))
        .withColumn("auth_status", F.col("auth_status_std"))
        .withColumn("city", F.col("city_std"))
        .withColumn("country", F.col("country_std"))
        .withColumn("source_system", F.col("_source_system"))
        .withColumn("_processed_at", F.current_timestamp())
    )
    return conform_schema(out, SILVER_TXN_SCHEMA)


def to_quarantine(df: DataFrame, run_id: str) -> DataFrame:
    bad = df.where(F.size("reason_codes") > 0)
    out = (
        bad.withColumn("event_ts_raw", F.col("event_ts"))
        .withColumn("amount_raw", F.col("amount"))
        .withColumn("currency_raw", F.col("currency"))
        .withColumn("source_system", F.col("_source_system"))
        .withColumn("primary_reason", F.element_at("reason_codes", 1))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return conform_schema(out, QUARANTINE_SCHEMA)


def prepare(
    df: DataFrame, cards: DataFrame, merchants: DataFrame, customers: DataFrame
) -> DataFrame:
    """Full row-level pipeline producing ``reason_codes`` for every bronze row."""
    return flag_duplicates(
        validate(attach_dimensions(standardize(df), cards, merchants, customers))
    )
