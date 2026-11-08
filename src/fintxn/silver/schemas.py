"""Explicit silver schemas (schema-on-write) and Delta CHECK constraints."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fintxn.generate.catalog import VALID_CURRENCIES

MONEY = DecimalType(18, 2)
CHANNELS = ("POS", "ONLINE", "ATM")
ENTRY_MODES = ("CHIP", "CONTACTLESS", "SWIPE", "ECOM")
AUTH_STATUSES = ("APPROVED", "DECLINED")


def _f(name: str, dtype: object, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)  # type: ignore[arg-type]


SILVER_TXN_SCHEMA = StructType(
    [
        _f("transaction_id", StringType(), False),
        _f("event_ts", TimestampType(), False),
        _f("event_date", DateType(), False),
        _f("card_token", StringType(), False),
        _f("card_last4", StringType()),
        _f("customer_id", StringType(), False),
        _f("merchant_id", StringType(), False),
        _f("merchant_name", StringType()),
        _f("mcc", StringType(), False),
        _f("amount", MONEY, False),
        _f("currency", StringType(), False),
        _f("amount_usd", MONEY, False),
        _f("channel", StringType(), False),
        _f("entry_mode", StringType()),
        _f("latitude", DoubleType()),
        _f("longitude", DoubleType()),
        _f("city", StringType()),
        _f("country", StringType()),
        _f("device_id", StringType()),
        _f("ip_address", StringType()),
        _f("auth_status", StringType(), False),
        _f("auth_code", StringType()),
        _f("source_system", StringType()),
        _f("_source_file", StringType()),
        _f("_batch_id", LongType()),
        _f("_run_id", StringType()),
        _f("_ingested_at", TimestampType()),
        _f("_processed_at", TimestampType()),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        _f("transaction_id", StringType()),
        _f("event_ts_raw", StringType()),
        _f("amount_raw", StringType()),
        _f("currency_raw", StringType()),
        _f("card_token", StringType()),
        _f("customer_id", StringType()),
        _f("merchant_id", StringType()),
        _f("channel", StringType()),
        _f("source_system", StringType()),
        _f("_corrupt_record", StringType()),
        _f("_rescued_data", StringType()),
        _f("reason_codes", ArrayType(StringType(), True), False),
        _f("primary_reason", StringType(), False),
        _f("_source_file", StringType()),
        _f("_batch_id", LongType()),
        _f("_landing_date", DateType()),
        _f("_run_id", StringType()),
        _f("_quarantined_at", TimestampType(), False),
    ]
)

SILVER_CUSTOMERS_SCHEMA = StructType(
    [
        _f("customer_id", StringType(), False),
        _f("first_name_enc", StringType()),
        _f("last_name_enc", StringType()),
        _f("email_enc", StringType()),
        _f("phone_enc", StringType()),
        _f("date_of_birth_enc", StringType()),
        _f("email_masked", StringType()),
        _f("phone_masked", StringType()),
        _f("birth_year", IntegerType()),
        _f("home_city", StringType()),
        _f("home_country", StringType()),
        _f("home_utc_offset", IntegerType()),
        _f("home_lat", DoubleType()),
        _f("home_lon", DoubleType()),
        _f("signup_date", DateType()),
        _f("segment", StringType()),
    ]
)

SILVER_CARDS_SCHEMA = StructType(
    [
        _f("card_id", StringType(), False),
        _f("customer_id", StringType(), False),
        _f("card_token", StringType(), False),
        _f("card_last4", StringType()),
        _f("network", StringType()),
        _f("card_type", StringType()),
        _f("issue_date", DateType()),
        _f("expiry", StringType()),
        _f("credit_limit", MONEY),
        _f("status", StringType()),
    ]
)

SILVER_MERCHANTS_SCHEMA = StructType(
    [
        _f("merchant_id", StringType(), False),
        _f("merchant_name", StringType()),
        _f("mcc", StringType(), False),
        _f("mcc_description", StringType()),
        _f("category", StringType()),
        _f("city", StringType()),
        _f("country", StringType()),
        _f("latitude", DoubleType()),
        _f("longitude", DoubleType()),
        _f("online_only", BooleanType()),
        _f("acquirer_id", StringType()),
        _f("onboarded_date", DateType()),
    ]
)

SILVER_LEDGER_SCHEMA = StructType(
    [
        _f("ledger_entry_id", StringType(), False),
        _f("transaction_id", StringType(), False),
        _f("account_id", StringType()),
        _f("merchant_id", StringType()),
        _f("auth_code", StringType()),
        _f("amount", MONEY, False),
        _f("currency", StringType()),
        _f("entry_type", StringType()),
        _f("posted_date", DateType(), False),
    ]
)

SILVER_SETTLEMENT_SCHEMA = StructType(
    [
        _f("settlement_id", StringType(), False),
        _f("transaction_ref", StringType()),
        _f("auth_code", StringType()),
        _f("merchant_id", StringType()),
        _f("settled_amount", MONEY, False),
        _f("currency", StringType()),
        _f("settlement_date", DateType(), False),
        _f("processor", StringType()),
    ]
)

SILVER_LABELS_SCHEMA = StructType(
    [
        _f("transaction_id", StringType(), False),
        _f("fraud_type", StringType()),
        _f("reported_at", TimestampType(), False),
    ]
)

_cur = ", ".join(f"'{c}'" for c in sorted(VALID_CURRENCIES))
TXN_CONSTRAINTS: dict[str, str] = {
    "amount_positive": "amount > 0",
    "amount_usd_positive": "amount_usd > 0",
    "currency_iso": f"currency IN ({_cur})",
    "channel_valid": "channel IN ('POS', 'ONLINE', 'ATM')",
    "auth_status_valid": "auth_status IN ('APPROVED', 'DECLINED')",
    "lat_range": "latitude IS NULL OR latitude BETWEEN -90 AND 90",
    "lon_range": "longitude IS NULL OR longitude BETWEEN -180 AND 180",
    "token_format": "length(card_token) = 64",
    "event_date_consistent": "event_date = CAST(event_ts AS DATE)",
}
LEDGER_CONSTRAINTS = {"ledger_amount_positive": "amount > 0"}
SETTLEMENT_CONSTRAINTS = {"settled_amount_positive": "settled_amount > 0"}
