"""Ledger vs processor-settlement reconciliation (pure DataFrame logic).

Matching runs in passes, each one only over what is still unmatched:

1. **Duplicates** - repeated settlements for the same reference (or the same
   auth_code + merchant + amount + date when the reference is blank) are set aside.
2. **Pass 1, reference key** - ``settlement.transaction_ref = ledger.transaction_id``.
3. **Pass 2, secondary key** - settlements without a reference matched on
   ``auth_code + merchant_id`` (closest settlement date wins).
4. Every matched pair is classified by amount difference and settlement lag:

   ==================  ==============================================================
   exact_match         amount equal and settled T+0/T+1
   tolerance_match     |diff| <= amount tolerance and settled within the date tolerance
   amount_mismatch     |diff| > amount tolerance
   timing              amount within tolerance but settled outside the date window
   ==================  ==============================================================

5. Leftover ledger rows are ``missing_in_settlement``; leftover settlements are
   ``missing_in_ledger``.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MATCHED = ("exact_match", "tolerance_match")
CATEGORIES = (
    "exact_match",
    "tolerance_match",
    "amount_mismatch",
    "timing",
    "missing_in_settlement",
    "missing_in_ledger",
    "duplicate",
)

RESULT_COLUMNS = [
    "transaction_id", "settlement_id", "category", "match_method", "merchant_id",
    "ledger_amount", "settled_amount", "amount_diff", "currency", "posted_date",
    "settlement_date", "day_diff",
]  # fmt: skip


def classify(amount_diff: Column, day_diff: Column, amount_tol: float, date_tol: int) -> Column:
    within_amt = F.abs(amount_diff) <= F.lit(amount_tol)
    within_date = (day_diff >= 0) & (day_diff <= date_tol)
    return (
        F.when((amount_diff == 0) & day_diff.between(0, 1), "exact_match")
        .when(within_amt & within_date, "tolerance_match")
        .when(~within_amt, "amount_mismatch")
        .otherwise("timing")
    )


def _settlement_prepared(settlement: DataFrame) -> DataFrame:
    s = settlement.select(
        "settlement_id", "transaction_ref", "auth_code", "merchant_id",
        F.col("settled_amount").cast("decimal(18,2)").alias("settled_amount"),
        F.col("currency").alias("s_currency"), "settlement_date",
    )  # fmt: skip
    by_ref = Window.partitionBy("transaction_ref").orderBy("settlement_date", "settlement_id")
    by_key = Window.partitionBy(
        "auth_code", "merchant_id", "settled_amount", "settlement_date"
    ).orderBy("settlement_id")
    return s.withColumn(
        "_dup",
        F.when(F.col("transaction_ref").isNotNull(), F.row_number().over(by_ref) > 1).otherwise(
            F.row_number().over(by_key) > 1
        ),
    )


def _pairs(
    ledger: DataFrame, settle: DataFrame, method: str, amount_tol: float, date_tol: int
) -> DataFrame:
    return (
        ledger.join(settle, ledger["_lkey"] == settle["_skey"], "inner")
        .withColumn("amount_diff", F.col("settled_amount") - F.col("ledger_amount"))
        .withColumn("day_diff", F.datediff("settlement_date", "posted_date"))
        .withColumn(
            "category", classify(F.col("amount_diff"), F.col("day_diff"), amount_tol, date_tol)
        )
        .withColumn("match_method", F.lit(method))
    )


def reconcile(
    ledger: DataFrame, settlement: DataFrame, amount_tol: float = 0.05, date_tol: int = 2
) -> DataFrame:
    led = ledger.select(
        "transaction_id", "auth_code", F.col("merchant_id").alias("l_merchant_id"),
        F.col("amount").cast("decimal(18,2)").alias("ledger_amount"), "currency", "posted_date",
    )  # fmt: skip
    s = _settlement_prepared(settlement)
    dups = s.where("_dup")
    primary = s.where(~F.col("_dup"))

    # pass 1: reference key
    p1 = _pairs(
        led.withColumn("_lkey", F.col("transaction_id")),
        primary.where(F.col("transaction_ref").isNotNull())
        .withColumn("_skey", F.col("transaction_ref"))
        .drop("auth_code"),
        "reference",
        amount_tol,
        date_tol,
    ).drop("merchant_id")

    # pass 2: auth_code + merchant for settlements without a reference
    led_left = led.join(p1.select("transaction_id"), "transaction_id", "left_anti")
    noref = primary.where(F.col("transaction_ref").isNull())
    p2_all = _pairs(
        led_left.withColumn("_lkey", F.concat_ws("|", "auth_code", "l_merchant_id")),
        noref.withColumn("_skey", F.concat_ws("|", "auth_code", "merchant_id")).drop("auth_code"),
        "auth_code+merchant",
        amount_tol,
        date_tol,
    ).drop("merchant_id")
    best = Window.partitionBy("transaction_id").orderBy(F.abs("day_diff"), "settlement_id")
    used = Window.partitionBy("settlement_id").orderBy(F.abs("day_diff"), "transaction_id")
    p2 = (
        p2_all.withColumn("_r1", F.row_number().over(best))
        .where("_r1 = 1")
        .withColumn("_r2", F.row_number().over(used))
        .where("_r2 = 1")
        .drop("_r1", "_r2")
    )

    matched = p1.unionByName(p2)
    matched_out = matched.select(
        "transaction_id", "settlement_id", "category", "match_method",
        F.col("l_merchant_id").alias("merchant_id"), "ledger_amount", "settled_amount",
        "amount_diff", "currency", "posted_date", "settlement_date", "day_diff",
    )  # fmt: skip

    missing_settle = led.join(
        matched.select("transaction_id"), "transaction_id", "left_anti"
    ).select(
        "transaction_id",
        F.lit(None).cast("string").alias("settlement_id"),
        F.lit("missing_in_settlement").alias("category"),
        F.lit(None).cast("string").alias("match_method"),
        F.col("l_merchant_id").alias("merchant_id"),
        "ledger_amount",
        F.lit(None).cast("decimal(18,2)").alias("settled_amount"),
        F.lit(None).cast("decimal(18,2)").alias("amount_diff"),
        "currency",
        "posted_date",
        F.lit(None).cast("date").alias("settlement_date"),
        F.lit(None).cast("int").alias("day_diff"),
    )

    def settle_side(df: DataFrame, category: str) -> DataFrame:
        return df.select(
            F.col("transaction_ref").alias("transaction_id"),
            "settlement_id",
            F.lit(category).alias("category"),
            F.lit(None).cast("string").alias("match_method"),
            "merchant_id",
            F.lit(None).cast("decimal(18,2)").alias("ledger_amount"),
            "settled_amount",
            F.lit(None).cast("decimal(18,2)").alias("amount_diff"),
            F.col("s_currency").alias("currency"),
            F.lit(None).cast("date").alias("posted_date"),
            "settlement_date",
            F.lit(None).cast("int").alias("day_diff"),
        )

    orphans = primary.join(matched.select("settlement_id"), "settlement_id", "left_anti")
    return (
        matched_out.unionByName(missing_settle)
        .unionByName(settle_side(orphans, "missing_in_ledger"))
        .unionByName(settle_side(dups, "duplicate"))
        .withColumn("business_date", F.coalesce("posted_date", "settlement_date"))
        .withColumn("is_break", ~F.col("category").isin(*MATCHED))
    )


def summarize(results: DataFrame) -> DataFrame:
    return (
        results.groupBy("business_date", "category")
        .agg(
            F.count(F.lit(1)).alias("records"),
            F.round(F.sum(F.coalesce("ledger_amount", "settled_amount")), 2).alias("amount"),
            F.round(F.sum(F.abs(F.coalesce("amount_diff", F.lit(0)))), 2).alias("abs_amount_diff"),
        )
        .withColumn("is_break", ~F.col("category").isin(*MATCHED))
        .orderBy("business_date", "category")
    )
