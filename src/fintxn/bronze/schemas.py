"""Explicit raw schemas for bronze.

Raw feeds are read as strings (so nothing is lost before silver decides what is valid), except
``latitude``/``longitude`` which are typed: a delimiter shift in a broken CSV line then fails the
double parse and the line is captured in ``_corrupt_record`` instead of silently mis-aligning.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

TXN_FIELDS = [
    "transaction_id", "event_ts", "card_number", "customer_id", "merchant_id", "merchant_name",
    "mcc", "amount", "currency", "channel", "entry_mode", "latitude", "longitude", "city",
    "country", "device_id", "ip_address", "auth_status", "auth_code",
]  # fmt: skip
_TYPED = {"latitude": DoubleType(), "longitude": DoubleType()}


def raw_txn_schema(with_corrupt: bool = True) -> StructType:
    fields = [StructField(f, _TYPED.get(f, StringType()), True) for f in TXN_FIELDS]
    if with_corrupt:
        fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def string_schema(cols: list[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in cols])


LABELS_SCHEMA = string_schema(["transaction_id", "fraud_type", "reported_at"])
LEDGER_SCHEMA = string_schema(
    ["ledger_entry_id", "transaction_id", "account_id", "merchant_id", "auth_code", "amount",
     "currency", "entry_type", "posted_date"]
)  # fmt: skip
SETTLEMENT_SCHEMA = string_schema(
    ["settlement_id", "transaction_ref", "auth_code", "merchant_id", "settled_amount", "currency",
     "settlement_date", "processor"]
)  # fmt: skip
CUSTOMERS_SCHEMA = string_schema(
    ["customer_id", "first_name", "last_name", "email", "phone", "date_of_birth", "home_city",
     "home_country", "home_utc_offset", "home_lat", "home_lon", "signup_date", "segment"]
)  # fmt: skip
CARDS_SCHEMA = string_schema(
    ["card_id", "customer_id", "card_number", "network", "card_type", "issue_date", "expiry",
     "credit_limit", "status"]
)  # fmt: skip
MERCHANTS_SCHEMA = string_schema(
    ["merchant_id", "merchant_name", "mcc", "mcc_description", "category", "city", "country",
     "latitude", "longitude", "online_only", "acquirer_id", "onboarded_date"]
)  # fmt: skip
