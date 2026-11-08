"""Shared fixtures: one local SparkSession (CI profile) and an isolated lake root per session."""

from __future__ import annotations

import base64
import os
import time
from collections.abc import Iterator

import pytest

# Spark collects TimestampType as naive *local* datetimes: pin the driver to UTC.
os.environ["TZ"] = "UTC"
time.tzset()

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


@pytest.fixture(scope="session")
def generated(settings):
    from fintxn.generate.runner import run_generation

    return run_generation(settings, rows=settings.data.n_transactions)


@pytest.fixture(scope="session")
def bronze_result(lake, generated):
    from fintxn.audit import job_run
    from fintxn.bronze.ingest import run_bronze

    with job_run("bronze", lake) as ctx:
        return run_bronze(lake, ctx)


@pytest.fixture(scope="session")
def silver_result(lake, bronze_result):
    from fintxn.audit import job_run
    from fintxn.silver.job import run_silver

    with job_run("silver", lake) as ctx:
        return run_silver(lake, ctx)


@pytest.fixture(scope="session")
def gold_result(lake, silver_result):
    from fintxn.audit import job_run
    from fintxn.gold.job import run_gold

    with job_run("gold", lake) as ctx:
        return run_gold(lake, ctx)


@pytest.fixture(scope="session")
def train_result(lake, gold_result):
    from fintxn.audit import job_run
    from fintxn.ml.train import run_train

    with job_run("train", lake) as ctx:
        return run_train(lake, ctx)


@pytest.fixture(scope="session")
def score_result(lake, train_result):
    from fintxn.audit import job_run
    from fintxn.ml.score import run_score

    with job_run("score", lake) as ctx:
        return run_score(lake, ctx)
