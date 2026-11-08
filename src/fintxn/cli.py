"""Command line entry point: ``fintxn <command>`` (wired to Makefile targets)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import typer

from fintxn.config import load_settings
from fintxn.logging import get_logger

if TYPE_CHECKING:
    from fintxn.storage import Lake

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    help="Financial transaction ETL",
)
log = get_logger("fintxn.cli")


@app.callback()
def main(
    env: str | None = typer.Option(
        None, "--env", help="Config profile (local, ci, aws, r2, databricks)"
    ),
) -> None:
    """Financial transaction ETL & fraud detection pipeline."""
    import os

    if env:
        os.environ["FINTXN_ENV"] = env


@app.command()
def info() -> None:
    """Print the resolved configuration (secrets redacted)."""
    s = load_settings()
    dumped = s.model_dump(mode="json")
    for key in ("access_key", "secret_key"):
        if dumped["storage"].get(key):
            dumped["storage"][key] = "***"
    typer.echo(json.dumps(dumped, indent=2))


@app.command()
def generate(
    rows: int | None = typer.Option(None, help="Number of transactions (default from config)"),
    chunk_from: int | None = typer.Option(None, help="First chunk index (incremental demo)"),
    chunk_to: int | None = typer.Option(None, help="Last chunk index (inclusive)"),
    append: bool = typer.Option(False, help="Keep existing raw files (incremental loads)"),
) -> None:
    """Generate synthetic transactions, dimensions, labels, ledger and settlements."""
    from fintxn.generate.runner import chunk_windows, run_generation

    s = load_settings()
    chunks = None
    if chunk_from is not None or chunk_to is not None:
        last = len(chunk_windows(s.data)) - 1
        chunks = (chunk_from or 0, last if chunk_to is None else chunk_to)
    manifest = run_generation(s, rows=rows, chunks=chunks, clean=not append)
    typer.echo(json.dumps({k: v for k, v in manifest.items() if k != "counts"}, indent=2))


def _lake() -> Lake:
    from fintxn.spark import get_spark
    from fintxn.storage import Lake

    s = load_settings()
    return Lake(get_spark(s), s)


def _echo(obj: object) -> None:
    typer.echo(json.dumps(obj, indent=2, default=str))


@app.command()
def bronze(
    full_refresh: bool = typer.Option(False, help="Re-ingest every raw file (overwrite bronze)"),
) -> None:
    """Ingest new raw files into bronze Delta tables."""
    from fintxn.audit import job_run
    from fintxn.bronze.ingest import run_bronze

    lake = _lake()
    with job_run("bronze", lake) as ctx:
        _echo(run_bronze(lake, ctx, full_refresh=full_refresh))


@app.command()
def silver(
    full_refresh: bool = typer.Option(False, help="Reprocess all bronze batches"),
    skip_gx: bool = typer.Option(False, help="Skip the Great Expectations suite"),
) -> None:
    """Clean, validate and conform bronze into silver (quarantining bad rows)."""
    from fintxn.audit import job_run
    from fintxn.silver.job import run_silver

    lake = _lake()
    with job_run("silver", lake) as ctx:
        _echo(run_silver(lake, ctx, full_refresh=full_refresh, run_gx_suite=not skip_gx))


@app.command()
def gold() -> None:
    """Build point-in-time fraud features and data-quality marts."""
    from fintxn.audit import job_run
    from fintxn.gold.job import run_gold

    lake = _lake()
    with job_run("gold", lake) as ctx:
        _echo(run_gold(lake, ctx))


@app.command()
def train(
    results_file: str | None = typer.Option(
        None, help="Write metrics JSON here (e.g. docs/results)"
    ),
) -> None:
    """Train LightGBM on the time-based split; log and register the model in MLflow."""
    from pathlib import Path

    from fintxn.audit import job_run
    from fintxn.ml.train import run_train

    lake = _lake()
    with job_run("train", lake) as ctx:
        _echo(run_train(lake, ctx, Path(results_file) if results_file else None))


@app.command()
def score() -> None:
    """Batch-score all transactions with the champion model and build scored marts."""
    from fintxn.audit import job_run
    from fintxn.ml.score import run_score

    lake = _lake()
    with job_run("score", lake) as ctx:
        _echo(run_score(lake, ctx))


@app.command("setup-uc")
def setup_uc() -> None:
    """Create Unity Catalog schemas and the raw landing volume (Databricks only)."""
    lake = _lake()
    st = lake.settings.storage
    for layer in ("raw", "bronze", "silver", "gold", "audit"):
        lake.sql(f"CREATE SCHEMA IF NOT EXISTS {lake.schema_name(layer)}")
    lake.sql(f"CREATE VOLUME IF NOT EXISTS {lake.schema_name('raw')}.landing")
    _echo({"catalog": st.catalog, "volume": st.volume_path})


@app.command()
def recon(
    results_file: str | None = typer.Option(
        None, help="Write metrics JSON here (e.g. docs/results)"
    ),
) -> None:
    """Reconcile the internal ledger against processor settlements."""
    from pathlib import Path

    from fintxn.audit import job_run
    from fintxn.recon.job import run_recon

    lake = _lake()
    with job_run("recon", lake) as ctx:
        _echo(run_recon(lake, ctx, Path(results_file) if results_file else None))


@app.command("audit-log")
def audit_log(limit: int = typer.Option(20, help="Most recent runs to show")) -> None:
    """Show the most recent pipeline runs from the audit trail."""
    from fintxn.audit import AUDIT_TABLE

    lake = _lake()
    lake.read("audit", AUDIT_TABLE).select(
        "started_at", "job_name", "status", "duration_s", "rows_in", "rows_out",
        "rows_quarantined", "dq_pass_rate", "git_sha", "run_id",
    ).orderBy("started_at", ascending=False).show(limit, truncate=False)  # fmt: skip


@app.command("time-travel")
def time_travel(
    run_id: str = typer.Argument(..., help="run_id from the audit trail"),
) -> None:
    """Show the exact Delta versions a past run wrote, and their row counts via time travel."""
    from fintxn.audit import AUDIT_TABLE

    lake = _lake()
    row = lake.read("audit", AUDIT_TABLE).where(f"run_id = '{run_id}'").first()
    if row is None:
        raise typer.BadParameter(f"run_id {run_id} not found")
    out = {}
    for table, version in json.loads(row["tables_written"]).items():
        layer, name = table.split(".", 1)
        out[table] = {
            "version": version,
            "rows_at_version": lake.read(layer, name, version=version).count(),
        }
    _echo({"run_id": run_id, "job": row["job_name"], "git_sha": row["git_sha"], "tables": out})


@app.command("pii-decrypt")
def pii_decrypt(
    customer_id: str = typer.Argument(..., help="Customer to reveal"),
    role: str | None = typer.Option(None, help="Caller role (default $FINTXN_ROLE)"),
    reason: str = typer.Option(..., help="Business justification, recorded in the audit trail"),
) -> None:
    """Decrypt one customer's PII. Allowed for auditor/data_engineer only; every call is audited."""
    from fintxn.audit import job_run
    from fintxn.security.crypto import decrypt_value, load_encryption_key, require_role

    granted = require_role(role)
    lake = _lake()
    with job_run("pii_access", lake) as ctx:
        ctx.metrics.update({"customer_id": customer_id, "role": granted, "reason": reason})
        row = lake.read("silver", "customers").where(f"customer_id = '{customer_id}'").first()
        if row is None:
            raise typer.BadParameter(f"customer {customer_id} not found")
        key = load_encryption_key()
        fields = ("first_name", "last_name", "email", "phone", "date_of_birth")
        _echo(
            {"customer_id": customer_id}
            | {f: decrypt_value(row[f"{f}_enc"], key, f) for f in fields}
        )
        ctx.rows_out = 1


if __name__ == "__main__":
    app()
