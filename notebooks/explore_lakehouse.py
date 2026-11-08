# Databricks notebook source / Jupytext percent format. Exploration only.
# %% [markdown]
# # Explore the fintxn lakehouse
# Reads tables through the same `Lake` abstraction the jobs use, so it works locally
# (`FINTXN_ENV=local`) and on Databricks (`FINTXN_ENV=databricks`).

# %%
from pyspark.sql import functions as F

from fintxn.config import load_settings
from fintxn.spark import get_spark
from fintxn.storage import Lake

settings = load_settings()
lake = Lake(get_spark(settings), settings)

# %% Quarantine reasons
lake.read("silver", "transactions_quarantine").groupBy("primary_reason").count().orderBy(
    F.desc("count")
).show(truncate=False)

# %% Fraud rate by MCC in the training window
(
    lake.read("gold", "features_transactions")
    .where("split = 'train'")
    .groupBy("mcc")
    .agg(F.count("*").alias("n"), F.avg("is_fraud").alias("fraud_rate"))
    .orderBy(F.desc("fraud_rate"))
    .show(10)
)

# %% Time travel: silver.transactions as of its first version
history = lake.delta_table("silver", "transactions").history().select("version", "operation")
history.show()
print(lake.read("silver", "transactions", version=0).count())
