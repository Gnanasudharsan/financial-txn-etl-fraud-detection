"""End-to-end pipeline in one process: generate -> bronze -> silver -> gold -> train -> score
-> recon -> export, each step audited separately, with a timing/row-count report."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fintxn.audit import JobContext, job_run
from fintxn.config import PROJECT_ROOT
from fintxn.logging import get_logger
from fintxn.storage import Lake

log = get_logger("fintxn.pipeline")
STEPS = ("generate", "bronze", "silver", "gold", "train", "score", "recon", "export")


def run_pipeline(
    lake: Lake,
    rows: int | None = None,
    results_dir: Path | None = None,
    steps: tuple[str, ...] = STEPS,
) -> dict[str, Any]:
    from fintxn.bronze.ingest import run_bronze
    from fintxn.export.exports import run_export
    from fintxn.generate.runner import run_generation
    from fintxn.gold.job import run_gold
    from fintxn.ml.score import run_score
    from fintxn.ml.train import run_train
    from fintxn.recon.job import run_recon
    from fintxn.silver.job import run_silver

    rd = (
        None
        if results_dir is None
        else (results_dir if results_dir.is_absolute() else PROJECT_ROOT / results_dir)
    )

    def gen(ctx: JobContext) -> dict[str, Any]:
        m = run_generation(lake.settings, rows=rows, store=lake.store)
        ctx.rows_out = int(m["counts"]["raw_transaction_rows"])
        ctx.metrics.update({k: v for k, v in m.items() if k != "counts"})
        return m

    runners: dict[str, Callable[[JobContext], dict[str, Any]]] = {
        "generate": gen,
        "bronze": lambda ctx: run_bronze(lake, ctx, full_refresh=True),
        "silver": lambda ctx: run_silver(lake, ctx, full_refresh=True),
        "gold": lambda ctx: run_gold(lake, ctx),
        "train": lambda ctx: run_train(lake, ctx, rd / "model_metrics.json" if rd else None),
        "score": lambda ctx: run_score(lake, ctx),
        "recon": lambda ctx: run_recon(lake, ctx, rd / "recon_metrics.json" if rd else None),
        "export": lambda ctx: run_export(lake, ctx),
    }
    report: dict[str, Any] = {"steps": {}}
    t0 = time.perf_counter()
    for step in steps:
        s0 = time.perf_counter()
        with job_run(step, lake) as ctx:
            result = runners[step](ctx)
        report["steps"][step] = {
            "seconds": round(time.perf_counter() - s0, 1),
            "rows_in": ctx.rows_in,
            "rows_out": ctx.rows_out,
            "rows_quarantined": ctx.rows_quarantined,
            "run_id": ctx.run_id,
        }
        if step == "generate":
            report["generation_counts"] = result["counts"]
        log.info(
            "step %-8s %7.1fs  in=%s out=%s",
            step,
            report["steps"][step]["seconds"],
            ctx.rows_in,
            ctx.rows_out,
        )
    report["total_seconds"] = round(time.perf_counter() - t0, 1)
    report["env"] = lake.settings.env
    if rd is not None:
        rd.mkdir(parents=True, exist_ok=True)
        (rd / "pipeline_run.json").write_text(json.dumps(report, indent=2, default=str))
    return report
