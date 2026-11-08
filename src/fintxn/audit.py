"""Per-run job context and audit trail.

Every job runs inside :func:`job_run`, which assigns a ``run_id``, captures the git SHA, timings,
row counts, data-quality pass rate and the Delta versions written, and appends one row to the
``audit.pipeline_runs`` Delta table on exit (success *or* failure). Because each row records the
exact Delta version of every table written, any past run can be reproduced with time travel::

    spark.read.format("delta").option("versionAsOf", <version>).load(<table path>)
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fintxn.config import PROJECT_ROOT, Settings
from fintxn.logging import get_logger

if TYPE_CHECKING:
    from fintxn.storage import Lake

log = get_logger("fintxn.audit")
AUDIT_TABLE = "pipeline_runs"


def git_sha() -> str:
    if os.environ.get("GIT_SHA"):
        return os.environ["GIT_SHA"]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


@dataclass
class JobContext:
    job_name: str
    settings: Settings
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    dq_pass_rate: float | None = None
    tables_written: dict[str, int] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def record_write(self, layer: str, name: str, version: int) -> None:
        self.tables_written[f"{layer}.{name}"] = version


def _audit_row(ctx: JobContext, status: str, error: str | None) -> dict[str, Any]:
    ended = datetime.now(UTC)
    return {
        "run_id": ctx.run_id,
        "job_name": ctx.job_name,
        "env": ctx.settings.env,
        "git_sha": git_sha(),
        "status": status,
        "error": (error or "")[:2000] or None,
        "started_at": ctx.started_at.replace(tzinfo=None),
        "ended_at": ended.replace(tzinfo=None),
        "duration_s": round((ended - ctx.started_at).total_seconds(), 2),
        "rows_in": int(ctx.rows_in),
        "rows_out": int(ctx.rows_out),
        "rows_quarantined": int(ctx.rows_quarantined),
        "dq_pass_rate": None if ctx.dq_pass_rate is None else float(ctx.dq_pass_rate),
        "tables_written": json.dumps(ctx.tables_written, sort_keys=True),
        "metrics": json.dumps(ctx.metrics, sort_keys=True, default=str),
        "user": os.environ.get("USER") or os.environ.get("USERNAME") or "unknown",
    }


def write_audit_row(lake: Lake, row: dict[str, Any]) -> None:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampNTZType,
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("job_name", StringType(), False),
            StructField("env", StringType(), True),
            StructField("git_sha", StringType(), True),
            StructField("status", StringType(), False),
            StructField("error", StringType(), True),
            StructField("started_at", TimestampNTZType(), False),
            StructField("ended_at", TimestampNTZType(), False),
            StructField("duration_s", DoubleType(), True),
            StructField("rows_in", LongType(), True),
            StructField("rows_out", LongType(), True),
            StructField("rows_quarantined", LongType(), True),
            StructField("dq_pass_rate", DoubleType(), True),
            StructField("tables_written", StringType(), True),
            StructField("metrics", StringType(), True),
            StructField("user", StringType(), True),
        ]
    )
    df = lake.spark.createDataFrame([row], schema=schema)
    lake.write(df, "audit", AUDIT_TABLE, mode="append")


@contextmanager
def job_run(job_name: str, lake: Lake) -> Iterator[JobContext]:
    ctx = JobContext(job_name=job_name, settings=lake.settings)
    log.info("job %s started (run_id=%s)", job_name, ctx.run_id)
    try:
        yield ctx
    except BaseException as exc:
        try:
            write_audit_row(lake, _audit_row(ctx, "FAILED", repr(exc)))
        except Exception:  # pragma: no cover - never mask the original failure
            log.exception("could not write audit row for failed run")
        raise
    write_audit_row(lake, _audit_row(ctx, "SUCCEEDED", None))
    log.info(
        "job %s succeeded: in=%d out=%d quarantined=%d tables=%s",
        job_name,
        ctx.rows_in,
        ctx.rows_out,
        ctx.rows_quarantined,
        ctx.tables_written,
    )
