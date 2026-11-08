from __future__ import annotations

import pandas as pd
import pytest

from fintxn.audit import job_run
from fintxn.export.exports import run_export
from fintxn.recon.job import run_recon

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.fixture(scope="module")
def recon_result(lake, silver_result, tmp_path_factory):
    with job_run("recon", lake) as ctx:
        return run_recon(lake, ctx, report_dir=tmp_path_factory.mktemp("recon"))


def test_recon_accounts_for_every_record(lake, recon_result, generated) -> None:
    c = generated["counts"]
    cats = recon_result["by_category"]
    ledger_side = sum(v for k, v in cats.items() if k not in ("missing_in_ledger", "duplicate"))
    assert ledger_side == c["ledger_rows"]
    assert (
        cats.get("missing_in_ledger", 0)
        + cats.get("duplicate", 0)
        + (ledger_side - cats.get("missing_in_settlement", 0))
        == c["settlement_rows"]
    )


def test_recon_matches_injected_truth(recon_result) -> None:
    assert recon_result["accuracy_vs_truth"] >= 0.99
    assert recon_result["orphans_exact_set_match"]
    assert recon_result["duplicates_found"] == recon_result["duplicates_expected"]


def test_export_is_small_and_pii_free(lake, score_result, recon_result, settings) -> None:
    with job_run("export", lake) as ctx:
        out = run_export(lake, ctx)
    assert {"daily_fraud_kpis", "merchant_risk_top", "pipeline_audit"} <= set(out["dashboard"])
    from pathlib import Path

    kpis = pd.read_parquet(Path(settings.export_dir) / "daily_fraud_kpis.parquet")
    assert kpis["txn_count"].sum() == score_result["scored_rows"]
    for table in out["powerbi"]:
        assert (Path(settings.powerbi_dir) / f"{table}.csv").exists()
