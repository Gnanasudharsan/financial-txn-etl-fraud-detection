"""Point-in-time-correct fraud features.

Leakage rules enforced here:

* every window ends at ``-1`` second (or row) relative to the current transaction, so a feature
  never sees the transaction it describes, nor anything after it;
* the MCC risk score is fit on the **training window only** and then applied everywhere;
* merchant fraud rates only count fraud *reports* dated strictly before the transaction's day
  (labels arrive with a delay in real life) over transactions strictly before that day.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

HOUR = 3600
DAY = 86_400
EARTH_RADIUS_KM = 6371.0
SMOOTHING = 50.0

CHANNEL_CODES = {"POS": 0, "ONLINE": 1, "ATM": 2}
ENTRY_CODES = {"CHIP": 0, "CONTACTLESS": 1, "SWIPE": 2, "ECOM": 3}

FEATURE_COLUMNS = [
    "amount_usd",
    "log_amount",
    "card_txn_cnt_1h",
    "card_amt_sum_1h",
    "card_txn_cnt_24h",
    "card_amt_sum_24h",
    "card_txn_cnt_7d",
    "card_amt_sum_7d",
    "card_declines_24h",
    "card_distinct_merchants_1h",
    "secs_since_last_txn",
    "cust_txn_count_prior",
    "amount_zscore",
    "amount_to_mean_ratio",
    "new_merchant_flag",
    "new_device_flag",
    "geo_distance_km",
    "implied_speed_kmh",
    "country_changed",
    "is_foreign",
    "hour_local",
    "day_of_week",
    "is_night",
    "channel_code",
    "entry_mode_code",
    "mcc_risk_score",
    "merchant_fraud_rate_lagged",
    "merchant_txn_count_lagged",
]
CATEGORICAL_FEATURES = ["channel_code", "entry_mode_code", "day_of_week"]


def _code(col: str, mapping: dict[str, int]) -> Column:
    expr = F.lit(-1)
    for k, v in mapping.items():
        expr = F.when(F.col(col) == k, F.lit(v)).otherwise(expr)
    return expr


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def base_frame(txn: DataFrame, labels: DataFrame, customers: DataFrame) -> DataFrame:
    lab = labels.select("transaction_id", "fraud_type", "reported_at")
    cust = customers.select("customer_id", "home_country", "home_utc_offset")
    return (
        txn.select(
            "transaction_id",
            "event_ts",
            "event_date",
            "card_token",
            "customer_id",
            "merchant_id",
            "mcc",
            "channel",
            "entry_mode",
            "device_id",
            "latitude",
            "longitude",
            "country",
            "auth_status",
            F.col("amount_usd").cast("double").alias("amount_usd"),
        )  # fmt: skip
        .withColumn("ts", F.col("event_ts").cast("long"))
        .join(lab, "transaction_id", "left")
        .withColumn("is_fraud", F.col("reported_at").isNotNull().cast("int"))
        .join(F.broadcast(cust), "customer_id", "left")
    )


def card_velocity(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("card_token").orderBy("ts")
    out = df
    for label, secs in (("1h", HOUR), ("24h", DAY), ("7d", 7 * DAY)):
        win = w.rangeBetween(-secs, -1)
        out = out.withColumn(f"card_txn_cnt_{label}", F.count(F.lit(1)).over(win)).withColumn(
            f"card_amt_sum_{label}", F.coalesce(F.sum("amount_usd").over(win), F.lit(0.0))
        )
    out = out.withColumn(
        "card_declines_24h",
        F.coalesce(
            F.sum((F.col("auth_status") == "DECLINED").cast("int")).over(w.rangeBetween(-DAY, -1)),
            F.lit(0),
        ),
    ).withColumn(
        "card_distinct_merchants_1h",
        F.size(F.collect_set("merchant_id").over(w.rangeBetween(-HOUR, -1))),
    )
    prev = Window.partitionBy("card_token").orderBy("ts", "transaction_id")
    out = (
        out.withColumn("_prev_ts", F.lag("ts").over(prev))
        .withColumn("_prev_lat", F.lag("latitude").over(prev))
        .withColumn("_prev_lon", F.lag("longitude").over(prev))
        .withColumn("_prev_country", F.lag("country").over(prev))
        .withColumn("secs_since_last_txn", (F.col("ts") - F.col("_prev_ts")).cast("double"))
        .withColumn(
            "geo_distance_km",
            haversine_km(
                F.col("_prev_lat"), F.col("_prev_lon"), F.col("latitude"), F.col("longitude")
            ),
        )
        .withColumn(
            "implied_speed_kmh",
            F.col("geo_distance_km")
            / F.greatest(F.col("secs_since_last_txn") / HOUR, F.lit(1.0 / 60)),
        )
        .withColumn("country_changed", (F.col("_prev_country") != F.col("country")).cast("int"))
    )
    return out.drop("_prev_ts", "_prev_lat", "_prev_lon", "_prev_country")


def customer_history(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("customer_id")
        .orderBy("ts", "transaction_id")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    out = (
        df.withColumn("cust_txn_count_prior", F.count(F.lit(1)).over(w))
        .withColumn("_mean", F.avg("amount_usd").over(w))
        .withColumn("_std", F.stddev_samp("amount_usd").over(w))
        .withColumn(
            "amount_zscore",
            F.when(
                (F.col("cust_txn_count_prior") >= 5) & (F.col("_std") > 0),
                (F.col("amount_usd") - F.col("_mean")) / F.col("_std"),
            ),
        )
        .withColumn(
            "amount_to_mean_ratio", F.when(F.col("_mean") > 0, F.col("amount_usd") / F.col("_mean"))
        )
    )
    first_merchant = Window.partitionBy("customer_id", "merchant_id").orderBy(
        "ts", "transaction_id"
    )
    first_device = Window.partitionBy("customer_id", "device_id").orderBy("ts", "transaction_id")
    return (
        out.withColumn("new_merchant_flag", (F.row_number().over(first_merchant) == 1).cast("int"))
        .withColumn(
            "new_device_flag",
            F.when(F.col("device_id").isNull(), F.lit(0)).otherwise(
                (F.row_number().over(first_device) == 1).cast("int")
            ),
        )
        .drop("_mean", "_std")
    )


def calendar_and_codes(df: DataFrame) -> DataFrame:
    local = F.col("event_ts") + F.make_interval(
        hours=F.coalesce(F.col("home_utc_offset"), F.lit(0))
    )
    return (
        df.withColumn("hour_local", F.hour(local))
        .withColumn("day_of_week", F.dayofweek(local) - 1)
        .withColumn("is_night", F.col("hour_local").between(0, 4).cast("int"))
        .withColumn("is_foreign", (F.col("country") != F.col("home_country")).cast("int"))
        .withColumn("channel_code", _code("channel", CHANNEL_CODES))
        .withColumn("entry_mode_code", _code("entry_mode", ENTRY_CODES))
        .withColumn("log_amount", F.log1p("amount_usd"))
    )


def mcc_risk(df: DataFrame, train_end: date) -> DataFrame:
    """Smoothed fraud rate per MCC, fit on ``event_date < train_end`` only."""
    train = df.where(F.col("event_date") < F.lit(train_end))
    prior_row = train.agg(F.avg("is_fraud")).first()
    prior = float(prior_row[0]) if prior_row and prior_row[0] is not None else 0.0
    stats = train.groupBy("mcc").agg(F.sum("is_fraud").alias("_f"), F.count(F.lit(1)).alias("_n"))
    score = stats.select(
        "mcc",
        ((F.col("_f") + F.lit(SMOOTHING * prior)) / (F.col("_n") + F.lit(SMOOTHING))).alias(
            "mcc_risk_score"
        ),
    )
    return df.join(F.broadcast(score), "mcc", "left").fillna({"mcc_risk_score": prior})


def merchant_fraud_lagged(df: DataFrame, prior: float) -> DataFrame:
    """Merchant fraud rate as known at the start of the transaction's day."""
    txn_days = df.groupBy("merchant_id", "event_date").agg(F.count(F.lit(1)).alias("_txns"))
    reports = (
        df.where("is_fraud = 1")
        .withColumn("event_date", F.to_date("reported_at"))
        .groupBy("merchant_id", "event_date")
        .agg(F.count(F.lit(1)).alias("_reports"))
    )
    daily = txn_days.join(reports, ["merchant_id", "event_date"], "full_outer").fillna(
        {"_txns": 0, "_reports": 0}
    )
    w = (
        Window.partitionBy("merchant_id")
        .orderBy("event_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    daily = daily.select(
        "merchant_id",
        "event_date",
        F.coalesce(F.sum("_txns").over(w), F.lit(0)).alias("merchant_txn_count_lagged"),
        F.coalesce(F.sum("_reports").over(w), F.lit(0)).alias("_cum_reports"),
    )
    out = df.join(daily, ["merchant_id", "event_date"], "left")
    return out.withColumn(
        "merchant_fraud_rate_lagged",
        (F.col("_cum_reports") + F.lit(SMOOTHING * prior))
        / (F.col("merchant_txn_count_lagged") + F.lit(SMOOTHING)),
    ).drop("_cum_reports")


def build_features(
    txn: DataFrame, labels: DataFrame, customers: DataFrame, train_end: date
) -> DataFrame:
    df = base_frame(txn, labels, customers)
    df = card_velocity(df)
    df = customer_history(df)
    df = calendar_and_codes(df)
    df = mcc_risk(df, train_end)
    prior_row = df.where(F.col("event_date") < F.lit(train_end)).agg(F.avg("is_fraud")).first()
    prior = float(prior_row[0]) if prior_row and prior_row[0] is not None else 0.0
    df = merchant_fraud_lagged(df, prior)
    keep = [
        "transaction_id", "event_ts", "event_date", "card_token", "customer_id", "merchant_id",
        "mcc", "channel", "auth_status", "is_fraud", "fraud_type",
    ]  # fmt: skip
    return df.select(*keep, *FEATURE_COLUMNS)
