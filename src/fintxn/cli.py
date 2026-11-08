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


if __name__ == "__main__":
    app()
