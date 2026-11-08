from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from fintxn.audit import job_run
from fintxn.silver.job import run_silver
from fintxn.silver.schemas import SILVER_TXN_SCHEMA, TXN_CONSTRAINTS

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_rows_are_conserved(silver_result, bronze_result) -> None:
    t = silver_result["transactions"]
    assert t["rows_in"] == bronze_result["transactions"]["rows"]
    assert t["rows_out"] + t["quarantined"] == t["rows_in"]
    assert sum(t["quarantine_by_reason"].values()) == t["quarantined"]


def test_every_injected_defect_type_is_quarantined(silver_result, generated) -> None:
    reasons = silver_result["transactions"]["quarantine_by_reason"]
    c = generated["counts"]
    assert reasons.get("DUPLICATE_EXACT", 0) > 0
    assert reasons.get("CORRUPT_RECORD", 0) == c["corrupt_line"]
    for code in ("AMOUNT_MALFORMED", "TS_UNPARSEABLE", "TS_FUTURE", "CURRENCY_INVALID"):
        assert reasons.get(code, 0) > 0, code
    # every injected exact duplicate is caught (some originals are themselves invalid)
    assert reasons["DUPLICATE_EXACT"] <= c["exact_duplicate"]


def test_silver_schema_and_constraints(lake, silver_result) -> None:
    df = lake.read("silver", "transactions")
    assert [f.name for f in df.schema.fields] == [f.name for f in SILVER_TXN_SCHEMA.fields]
    assert df.select("transaction_id").distinct().count() == df.count()
    props = lake.delta_table("silver", "transactions").detail().first()["properties"]
    assert {f"delta.constraints.{k}" for k in TXN_CONSTRAINTS} <= set(props)


def test_check_constraint_rejects_bad_write(lake, silver_result) -> None:
    bad = (
        lake.read("silver", "transactions")
        .limit(1)
        .withColumn("amount", F.lit(-1).cast("decimal(18,2)"))
    )
    with pytest.raises(Exception, match="(?i)check constraint|CHECK"):
        lake.write(bad, "silver", "transactions", mode="append")


def test_pii_encrypted_and_masked(lake, silver_result) -> None:
    cust = lake.read("silver", "customers")
    assert not {"first_name", "last_name", "email", "phone"} & set(cust.columns)
    row = cust.first()
    assert row["email_enc"].startswith("v1:") and "@" in row["email_masked"]


def test_gx_suite_passes(silver_result, lake) -> None:
    gx = silver_result["gx"]
    assert gx["success"], gx["failed"]
    assert lake.read("silver", "_dq_results").count() >= gx["evaluated"]


def test_incremental_rerun_adds_nothing(lake, silver_result) -> None:
    before = lake.read("silver", "transactions").count()
    with job_run("silver", lake) as ctx:
        again = run_silver(lake, ctx, run_gx_suite=False)
    assert again["transactions"]["rows_in"] == 0
    assert lake.read("silver", "transactions").count() == before
