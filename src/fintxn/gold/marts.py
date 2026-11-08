"""Gold marts for BI. All are aggregated or keyed by surrogate ids; none carry raw PII."""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def split_col(train_end: date, valid_end: date) -> Column:
    return (
        F.when(F.col("event_date") < F.lit(train_end), "train")
        .when(F.col("event_date") < F.lit(valid_end), "valid")
        .otherwise("test")
    )


def _safe_div(num: Column, den: Column) -> Column:
    return F.when(den > 0, num / den)


def daily_fraud_kpis(scored: DataFrame) -> DataFrame:
    tp = (F.col("alert_flag") == 1) & (F.col("is_fraud") == 1)
    g = scored.groupBy("event_date", "split").agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).alias("amount_usd"),
        F.sum("is_fraud").alias("fraud_count"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount_usd")).otherwise(0.0)), 2).alias(
            "fraud_amount_usd"
        ),
        F.sum("alert_flag").alias("alert_count"),
        F.sum(tp.cast("int")).alias("true_positives"),
        F.round(F.sum(F.when(tp, F.col("amount_usd")).otherwise(0.0)), 2).alias(
            "caught_fraud_amount_usd"
        ),
        F.round(F.avg("fraud_score"), 6).alias("avg_score"),
    )
    return (
        g.withColumn("false_positives", F.col("alert_count") - F.col("true_positives"))
        .withColumn(
            "alert_precision", F.round(_safe_div(F.col("true_positives"), F.col("alert_count")), 4)
        )
        .withColumn(
            "fraud_recall", F.round(_safe_div(F.col("true_positives"), F.col("fraud_count")), 4)
        )
        .withColumn(
            "fraud_rate_bps", F.round(F.col("fraud_count") / F.col("txn_count") * 10_000, 2)
        )
        .orderBy("event_date")
    )


def merchant_risk(scored: DataFrame, merchants: DataFrame) -> DataFrame:
    m = merchants.select(
        "merchant_id", "merchant_name", "mcc_description", "category", "city", "country"
    )
    agg = scored.groupBy("merchant_id", "mcc").agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).alias("amount_usd"),
        F.sum("is_fraud").alias("fraud_count"),
        F.sum("alert_flag").alias("alert_count"),
        F.round(F.avg("fraud_score"), 6).alias("avg_score"),
        F.round(F.max("fraud_score"), 6).alias("max_score"),
    )
    out = (
        agg.withColumn("fraud_rate", F.round(F.col("fraud_count") / F.col("txn_count"), 6))
        .withColumn("alert_rate", F.round(F.col("alert_count") / F.col("txn_count"), 6))
        .join(F.broadcast(m), "merchant_id", "left")
    )
    return out.withColumn(
        "risk_tier",
        F.when((F.col("fraud_rate") >= 0.05) | (F.col("avg_score") >= 0.2), "high")
        .when((F.col("fraud_rate") >= 0.01) | (F.col("avg_score") >= 0.05), "medium")
        .otherwise("low"),
    )


def customer_risk(scored: DataFrame, customers: DataFrame) -> DataFrame:
    c = customers.select("customer_id", "segment", "home_city", "home_country")
    agg = scored.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("txn_count"),
        F.countDistinct("card_token").alias("cards_used"),
        F.round(F.sum("amount_usd"), 2).alias("amount_usd"),
        F.sum("is_fraud").alias("fraud_count"),
        F.sum("alert_flag").alias("alert_count"),
        F.round(F.max("fraud_score"), 6).alias("max_score"),
        F.max("event_ts").alias("last_txn_ts"),
    )
    return agg.join(F.broadcast(c), "customer_id", "left").withColumn(
        "risk_tier",
        F.when(F.col("fraud_count") > 0, "confirmed_fraud")
        .when(F.col("alert_count") > 0, "alerted")
        .otherwise("normal"),
    )


def dq_health(bronze_txn: DataFrame, quarantine: DataFrame) -> DataFrame:
    total = bronze_txn.groupBy(F.col("_landing_date").alias("landing_date")).agg(
        F.count(F.lit(1)).alias("rows_total")
    )
    bad = quarantine.groupBy(F.col("_landing_date").alias("landing_date")).agg(
        F.count(F.lit(1)).alias("rows_quarantined")
    )
    return (
        total.join(bad, "landing_date", "left")
        .fillna({"rows_quarantined": 0})
        .withColumn("rows_valid", F.col("rows_total") - F.col("rows_quarantined"))
        .withColumn("pass_rate", F.round(F.col("rows_valid") / F.col("rows_total"), 6))
        .orderBy("landing_date")
    )


def dq_quarantine_by_reason(quarantine: DataFrame) -> DataFrame:
    return (
        quarantine.groupBy(
            F.col("_landing_date").alias("landing_date"), "primary_reason", "source_system"
        )
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy("landing_date", "primary_reason")
    )
