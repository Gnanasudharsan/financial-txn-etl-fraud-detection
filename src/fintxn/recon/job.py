"""Reconciliation job: match, classify, publish break reports, and benchmark the run.

Outputs (gold): ``recon_results`` (every record + category), ``recon_breaks`` (exceptions only),
``reconciliation_summary`` (per business date x category), plus a CSV break report per run for
operations teams. If the generator's ground-truth file exists, classification accuracy is
measured against it.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from fintxn.audit import JobContext
from fintxn.bronze.schemas import string_schema
from fintxn.config import PROJECT_ROOT
from fintxn.logging import get_logger
from fintxn.recon.engine import reconcile, summarize
from fintxn.storage import Lake

log = get_logger("fintxn.recon")


def _accuracy(lake: Lake, results_table: str) -> dict[str, Any] | None:
    keys = lake.store.list_keys("raw", "_truth/recon/")
    if not keys:
        return None
    truth = (
        lake.spark.read.option("header", "true")
        .schema(string_schema(["key", "key_type", "expected"]))
        .csv([lake.store.uri("raw", k) for k in keys])
    )
    res = lake.read("gold", results_table)
    ledger_truth = truth.where("key_type = 'transaction_id'")
    ledger_side = res.where("posted_date IS NOT NULL").select(
        F.col("transaction_id").alias("key"), F.col("category").alias("got")
    )
    cmp = ledger_truth.join(ledger_side, "key", "left")
    agg = cmp.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum((F.col("got") == F.col("expected")).cast("int")).alias("ok"),
    ).first()
    confusion = {
        f"{r['expected']}->{r['got']}": int(r["count"])
        for r in cmp.where("got IS NULL OR got <> expected")
        .groupBy("expected", "got")
        .count()
        .collect()
    }
    exp_orphans = {
        r[0] for r in truth.where("expected = 'missing_in_ledger'").select("key").collect()
    }
    got_orphans = {
        r[0] for r in res.where("category = 'missing_in_ledger'").select("settlement_id").collect()
    }
    exp_dups = truth.where("expected = 'duplicate'").count()
    got_dups = res.where("category = 'duplicate'").count()
    n = int(agg["n"]) if agg else 0
    ok = int(agg["ok"] or 0) if agg else 0
    return {
        "truth_compared": n,
        "ledger_side_correct": ok,
        "accuracy_vs_truth": ok / n if n else None,
        "misclassified": confusion,
        "orphans_expected": len(exp_orphans),
        "orphans_found": len(got_orphans),
        "orphans_exact_set_match": exp_orphans == got_orphans,
        "duplicates_expected": exp_dups,
        "duplicates_found": got_dups,
    }


def write_break_report(lake: Lake, ctx: JobContext, out_dir: Path) -> Path:
    """Single CSV of every break, sorted for an operations work queue."""
    out_dir = out_dir if out_dir.is_absolute() else PROJECT_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"recon_breaks_{ctx.run_id[:8]}.csv"
    pdf = (
        lake.read("gold", "recon_breaks")
        .orderBy("business_date", "category", F.desc_nulls_last(F.abs("amount_diff")))
        .toPandas()
    )
    pdf.to_csv(path, index=False)  # type: ignore[attr-defined]
    return path


def run_recon(
    lake: Lake,
    ctx: JobContext,
    results_path: Path | None = None,
    report_dir: Path | None = Path("data/exports/recon"),
) -> dict[str, Any]:
    cfg = lake.settings.data.recon
    t0 = time.perf_counter()
    ledger = lake.read("silver", "ledger")
    settlement = lake.read("silver", "settlement")
    results = reconcile(
        ledger, settlement, cfg.amount_tolerance, cfg.date_tolerance_days
    ).withColumn("_run_id", F.lit(ctx.run_id))
    lake.write(
        results,
        "gold",
        "recon_results",
        mode="overwrite",
        partition_by=["category"],
        overwrite_schema=True,
    )
    ctx.record_write("gold", "recon_results", lake.version("gold", "recon_results"))
    res = lake.read("gold", "recon_results")
    breaks = res.where("is_break")
    ctx.record_write(
        "gold",
        "recon_breaks",
        lake.write(breaks, "gold", "recon_breaks", mode="overwrite", overwrite_schema=True),
    )
    ctx.record_write(
        "gold",
        "reconciliation_summary",
        lake.write(
            summarize(res),
            "gold",
            "reconciliation_summary",
            mode="overwrite",
            overwrite_schema=True,
        ),
    )
    t_match = time.perf_counter() - t0
    report = write_break_report(lake, ctx, report_dir) if report_dir else None
    t_total = time.perf_counter() - t0

    by_cat = {r["category"]: int(r["count"]) for r in res.groupBy("category").count().collect()}
    n_breaks = sum(v for k, v in by_cat.items() if k not in ("exact_match", "tolerance_match"))
    out: dict[str, Any] = {
        "ledger_rows": ledger.count(),
        "settlement_rows": settlement.count(),
        "by_category": by_cat,
        "breaks": n_breaks,
        "match_and_publish_seconds": round(t_match, 1),
        "total_seconds_incl_report": round(t_total, 1),
        "report_file": str(report.relative_to(PROJECT_ROOT))
        if report and report.is_relative_to(PROJECT_ROOT)
        else str(report),
    }
    acc = _accuracy(lake, "recon_results")
    if acc:
        out.update(acc)
    ctx.rows_in = int(out["ledger_rows"]) + int(out["settlement_rows"])
    ctx.rows_out = sum(by_cat.values())
    ctx.metrics.update({k: v for k, v in out.items() if k != "misclassified"})
    if results_path is not None:
        p = results_path if results_path.is_absolute() else PROJECT_ROOT / results_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(out, indent=2, default=str))
    log.info("recon: %s breaks=%d in %.1fs", by_cat, n_breaks, t_total)
    return out
