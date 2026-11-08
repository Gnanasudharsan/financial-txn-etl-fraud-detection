from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from fintxn.audit import job_run
from fintxn.bronze.ingest import INGEST_LOG, run_bronze
from fintxn.generate.runner import run_generation

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.fixture(scope="module")
def generated(settings):
    return run_generation(settings, rows=settings.data.n_transactions)


@pytest.fixture(scope="module")
def bronze_result(lake, generated):
    with job_run("bronze", lake) as ctx:
        return run_bronze(lake, ctx)


def test_bronze_row_counts_match_raw(lake, generated, bronze_result) -> None:
    counts = generated["counts"]
    txn = bronze_result["transactions"]
    # every physical raw line (incl. corrupt ones) lands exactly once
    assert txn["rows"] == counts["raw_transaction_rows"]
    assert lake.read("bronze", "transactions").count() == counts["raw_transaction_rows"]
    assert bronze_result["ledger"]["rows"] == counts["ledger_rows"]
    assert bronze_result["settlement"]["rows"] == counts["settlement_rows"]
    assert bronze_result["fraud_labels"]["rows"] == counts["fraud_transactions"]
    assert bronze_result["customers"]["inserted"] == lake.settings.data.n_customers


def test_no_raw_pan_in_bronze(lake, bronze_result) -> None:
    for name in ("transactions", "cards"):
        df = lake.read("bronze", name)
        assert "card_number" not in df.columns
        assert "card_token" in df.columns
    corrupt = lake.read("bronze", "transactions").where(F.col("_corrupt_record").isNotNull())
    assert (
        corrupt.where(F.col("_corrupt_record").rlike(r"(?<![0-9])[0-9]{15,16}(?![0-9])")).count()
        == 0
    )


def test_corrupt_and_rescued_captured(lake, generated, bronze_result) -> None:
    df = lake.read("bronze", "transactions")
    assert (
        df.where(F.col("_corrupt_record").isNotNull()).count()
        == generated["counts"]["corrupt_line"]
    )
    if generated["counts"].get("schema_drift_rows"):
        assert df.where(F.col("_rescued_data").contains("wallet_type")).count() > 0
    assert set(df.columns) >= {
        "_source_file",
        "_ingested_at",
        "_run_id",
        "_batch_id",
        "_landing_date",
    }


def test_rerun_is_idempotent(lake, bronze_result) -> None:
    before = lake.read("bronze", "transactions").count()
    with job_run("bronze", lake) as ctx:
        again = run_bronze(lake, ctx)
    assert again["transactions"]["files"] == 0
    assert again["customers"]["inserted"] == 0 and again["customers"]["updated"] == 0
    assert lake.read("bronze", "transactions").count() == before
    assert (
        lake.read("bronze", INGEST_LOG).select("source_key").distinct().count()
        == lake.read("bronze", INGEST_LOG).count()
    )


def test_audit_rows_written(lake, bronze_result) -> None:
    audit = lake.read("audit", "pipeline_runs").where("job_name = 'bronze'")
    assert audit.where("status = 'SUCCEEDED'").count() >= 1
    assert audit.first()["tables_written"].startswith("{")
