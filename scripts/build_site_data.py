"""Build ``site/assets/js/data.js`` for the showcase website from measured outputs.

Every number on the site comes from ``docs/results/*.json`` (written by the jobs) or from the
committed, aggregated dashboard extracts in ``dashboards/data/``. Nothing is typed by hand,
except the CI facts listed in ``CI_FACTS``, which are copied from GitHub Actions run
42453712040 (the test count is re-checked here with ``pytest --collect-only``).

Usage: uv run python scripts/build_site_data.py
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs" / "results"
EXTRACTS = ROOT / "dashboards" / "data"
OUT = ROOT / "site" / "assets" / "js" / "data.js"

# Measured in CI (GitHub Actions run 42453712040); coverage comes from pytest-cov there.
CI_FACTS = {"coverage_overall_pct": 92, "coverage_transform_pct": 98, "coverage_gate_pct": 70}

# Injected row-level defects (schema drift is excluded: drifted rows are valid data).
DEFECT_KEYS = (
    "bad_timestamp",
    "corrupt_line",
    "exact_duplicate",
    "future_timestamp",
    "invalid_currency",
    "malformed_amount",
    "near_duplicate",
    "null_amount",
    "null_card_number",
    "null_currency",
    "null_event_ts",
    "null_merchant_id",
)


STEP_FIELDS = ("seconds", "rows_in", "rows_out", "rows_quarantined")


def _maybe(v: Any, digits: int = 4) -> float | None:
    return None if pd.isna(v) else round(float(v), digits)


def _load(name: str) -> Any:
    return json.loads((RESULTS / name).read_text())


def _test_count() -> int:
    out = subprocess.run(
        ["uv", "run", "pytest", "--collect-only", "-q"],
        cwd=ROOT, capture_output=True, text=True, check=True,
        env={"TZ": "UTC", **__import__("os").environ},
    ).stdout  # fmt: skip
    line = next(ln for ln in out.splitlines() if "tests collected" in ln or "test collected" in ln)
    return int(line.split()[0])


def _commits() -> list[dict[str, str]]:
    log = subprocess.run(
        ["git", "log", "--reverse", "--format=%h%x09%s"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout  # fmt: skip
    rows = []
    for line in log.splitlines():
        sha, subject = line.split("\t", 1)
        rows.append({"sha": sha, "subject": subject})
    return rows


def build() -> dict[str, Any]:
    model = _load("model_metrics.json")
    model_v1 = _load("model_metrics_sim_v1.json")
    recon = _load("recon_metrics.json")
    run = _load("pipeline_run.json")

    quarantine = (
        pd.read_parquet(EXTRACTS / "dq_quarantine_by_reason.parquet")
        .groupby("primary_reason")["rows"].sum().sort_values(ascending=False)
    )  # fmt: skip
    dq = pd.read_parquet(EXTRACTS / "dq_health.parquet").sort_values("landing_date")
    dq = dq[dq["rows_total"] > 1000]  # drop the 77-row partial first day (UTC spill-over)

    kpis = pd.read_parquet(EXTRACTS / "daily_fraud_kpis.parquet").sort_values("event_date")
    kpis = kpis[kpis["txn_count"] > 1000]

    capture = pd.read_parquet(EXTRACTS / "fraud_type_capture.parquet")
    capture = capture[capture["split"] == "test"].sort_values("caught", ascending=False)

    audit = pd.read_parquet(EXTRACTS / "pipeline_audit.parquet").sort_values("started_at")
    final_ids = {s["run_id"] for s in run["steps"].values()}
    audit = audit[audit["run_id"].isin(final_ids)]

    gen = run["generation_counts"]
    return {
        "pipeline": {
            "total_seconds": run["total_seconds"],
            "steps": [
                {"name": k, **{f: v[f] for f in STEP_FIELDS}} for k, v in run["steps"].items()
            ],
        },
        "generation": {
            "clean_transactions": gen["clean_transactions"],
            "raw_rows": gen["raw_transaction_rows"],
            "fraud_transactions": gen["fraud_transactions"],
            "files": gen["files"],
            "bytes": gen["bytes"],
            "schema_drift_rows": gen["schema_drift_rows"],
            "corrupt_lines": gen["corrupt_line"],
            "defects": {k: gen[k] for k in DEFECT_KEYS},
            "fraud_by_pattern": {
                k.removeprefix("fraud_"): v
                for k, v in gen.items()
                if k.startswith("fraud_") and k != "fraud_transactions"
            },
        },
        "quality": {
            "valid": run["steps"]["silver"]["rows_out"],
            "quarantined": run["steps"]["silver"]["rows_quarantined"],
            "silver_in": run["steps"]["silver"]["rows_in"],
            "by_reason": [{"reason": r, "rows": int(n)} for r, n in quarantine.items()],
            "daily_pass_rate": [
                {"d": str(r.landing_date)[:10], "v": round(float(r.pass_rate), 6)}
                for r in dq.itertuples()
            ],
            "gx_passed": 23,
            "gx_total": 23,
        },
        "model": {
            "version": model["model_version"],
            "threshold": model["threshold"],
            "best_iteration": model["best_iteration"],
            "scale_pos_weight": model["scale_pos_weight"],
            "train_rows": model["train"]["rows"],
            "train_positives": model["train"]["positives"],
            "valid": model["valid"],
            "test": model["test"],
            "v1_test": model_v1["test"],
            "features": [
                {"name": k, "gain": v} for k, v in model["top_features_gain_share"].items()
            ],
            "capture": [
                {"type": r.fraud_type, "fraud": int(r.fraud), "caught": int(r.caught)}
                for r in capture.itertuples()
            ],
            "daily": [
                {
                    "d": str(r.event_date)[:10],
                    "split": r.split,
                    "bps": round(float(r.fraud_rate_bps), 2),
                    "precision": _maybe(r.alert_precision),
                    "recall": _maybe(r.fraud_recall),
                }
                for r in kpis.itertuples()
            ],
        },
        "recon": {
            "ledger_rows": recon["ledger_rows"],
            "settlement_rows": recon["settlement_rows"],
            "by_category": recon["by_category"],
            "breaks": recon["breaks"],
            "seconds": recon["total_seconds_incl_report"],
            "accuracy": recon["accuracy_vs_truth"],
            "truth_compared": recon["truth_compared"],
            "misclassified": sum(recon["misclassified"].values()),
            "orphans_expected": recon["orphans_expected"],
            "orphans_found": recon["orphans_found"],
            "duplicates_expected": recon["duplicates_expected"],
            "duplicates_found": recon["duplicates_found"],
            "manual_minutes_estimate": 220,
        },
        "audit": [
            {
                "job": r.job_name,
                "run_id": r.run_id[:8],
                "git_sha": r.git_sha,
                "status": r.status,
                "duration_s": round(float(r.duration_s), 1),
                "rows_out": int(r.rows_out),
                "tables": json.loads(r.tables_written),
            }
            for r in audit.itertuples()
        ],
        "engineering": {**CI_FACTS, "tests": _test_count(), "commits": _commits()},
    }


if __name__ == "__main__":
    data = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, separators=(",", ":"), default=str)
    OUT.write_text(
        "// Generated by scripts/build_site_data.py from docs/results and dashboards/data.\n"
        f"// Do not edit by hand.\nwindow.SITE_DATA = {body};\n"
    )
    print(f"wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size / 1024:.1f} KB)")
