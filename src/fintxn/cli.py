"""Command line entry point: ``fintxn <command>`` (wired to Makefile targets)."""

from __future__ import annotations

import json

import typer

from fintxn.config import load_settings
from fintxn.logging import get_logger

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Financial transaction ETL")
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


if __name__ == "__main__":
    app()
