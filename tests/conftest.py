"""Shared fixtures: one local SparkSession (CI profile) and an isolated lake root per session."""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator

import pytest

os.environ.setdefault("FINTXN_TOKEN_KEY", "0f" * 32)
os.environ.setdefault("FINTXN_ENCRYPTION_KEY", base64.b64encode(b"k" * 32).decode())
os.environ["FINTXN_ENV"] = "ci"


@pytest.fixture(scope="session")
def lake_root(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("lake")


@pytest.fixture(scope="session")
def settings(lake_root):
    from fintxn.config import load_settings

    return load_settings(
        "ci",
        overrides={
            "storage": {"local_root": str(lake_root)},
            "mlflow": {"tracking_uri": f"sqlite:///{lake_root}/mlflow.db"},
            "export_dir": str(lake_root / "exports"),
            "powerbi_dir": str(lake_root / "powerbi"),
        },
    )


@pytest.fixture(scope="session")
def spark(settings) -> Iterator[object]:
    from fintxn.spark import get_spark

    session = get_spark(settings)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def lake(spark, settings):
    from fintxn.storage import Lake

    return Lake(spark, settings)
