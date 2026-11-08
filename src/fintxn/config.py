"""Typed, layered configuration.

Configuration is resolved as ``configs/base.yaml`` deep-merged with ``configs/<env>.yaml``
(``env`` from ``FINTXN_ENV``, default ``local``). String values may reference environment
variables with ``${VAR}`` or ``${VAR:-default}`` so that secrets never live in YAML.
"""

from __future__ import annotations

import os
import re
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = Path(os.environ.get("FINTXN_CONFIG_DIR", PROJECT_ROOT / "configs"))

LAYERS = ("raw", "bronze", "silver", "gold", "audit")


class StorageConfig(BaseModel):
    """Where raw files and lakehouse tables live.

    backend:
      * ``s3``         - any S3 API endpoint (SeaweedFS locally, AWS S3, Cloudflare R2)
      * ``local``      - local filesystem (CI, unit tests)
      * ``databricks`` - Unity Catalog managed tables + UC Volume for raw files
    """

    backend: Literal["s3", "local", "databricks"] = "s3"
    endpoint: str | None = None
    region: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None
    path_style: bool = True
    ssl: bool = False
    sse: Literal["none", "AES256", "aws:kms"] = "none"
    kms_key_id: str | None = None
    bucket_prefix: str = "fintxn"
    local_root: str = "data"
    # Databricks / Unity Catalog
    catalog: str = "workspace"
    schema_prefix: str = "fintxn"
    volume_path: str = "/Volumes/workspace/fintxn_raw/landing"

    def bucket(self, layer: str) -> str:
        return f"{self.bucket_prefix}-{layer}"


class SparkConfig(BaseModel):
    master: str = "local[*]"
    app_name: str = "fintxn"
    driver_memory: str = "6g"
    shuffle_partitions: int = 64
    extra_conf: dict[str, str] = Field(default_factory=dict)


class FraudConfig(BaseModel):
    """Episode counts are expressed per 1M legitimate transactions so they scale with N."""

    card_testing_per_m: int = 200
    account_takeover_per_m: int = 300
    velocity_burst_per_m: int = 200
    high_amount_night_per_m: int = 800
    high_risk_mcc_per_m: int = 700


class DirtyConfig(BaseModel):
    null_rate: float = 0.006
    exact_dup_rate: float = 0.005
    near_dup_rate: float = 0.003
    malformed_amount_rate: float = 0.004
    bad_timestamp_rate: float = 0.003
    future_timestamp_rate: float = 0.001
    invalid_currency_rate: float = 0.002
    corrupt_line_rate: float = 0.0005
    schema_drift_file_share: float = 0.25


class ReconConfig(BaseModel):
    missing_settlement_rate: float = 0.004
    amount_mismatch_rate: float = 0.002
    cents_diff_rate: float = 0.01
    duplicate_settlement_rate: float = 0.001
    timing_break_rate: float = 0.002
    missing_ref_rate: float = 0.01
    orphan_settlement_rate: float = 0.0005
    amount_tolerance: float = 0.05
    date_tolerance_days: int = 2


class DataConfig(BaseModel):
    n_transactions: int = 5_000_000
    n_customers: int = 50_000
    n_cards: int = 100_000
    n_merchants: int = 20_000
    start_date: date = date(2026, 1, 1)
    days: int = 181
    seed: int = 42
    chunk_days: int = 7
    fraud: FraudConfig = FraudConfig()
    dirty: DirtyConfig = DirtyConfig()
    recon: ReconConfig = ReconConfig()


class ModelConfig(BaseModel):
    train_end: date = date(2026, 5, 1)
    valid_end: date = date(2026, 6, 1)
    target_recall: float = 0.95
    label_delay_days: int = 7
    num_boost_round: int = 600
    early_stopping_rounds: int = 50
    params: dict[str, Any] = Field(
        default_factory=lambda: {
            "objective": "binary",
            "learning_rate": 0.05,
            "num_leaves": 63,
            "min_data_in_leaf": 100,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "lambda_l2": 1.0,
            "verbose": -1,
        }
    )


class MlflowConfig(BaseModel):
    tracking_uri: str = "sqlite:///mlflow.db"
    experiment: str = "fintxn-fraud"
    model_name: str = "fintxn_fraud_lgbm"


class Settings(BaseModel):
    env: str = "local"
    storage: StorageConfig = StorageConfig()
    spark: SparkConfig = SparkConfig()
    data: DataConfig = DataConfig()
    model: ModelConfig = ModelConfig()
    mlflow: MlflowConfig = MlflowConfig()
    export_dir: str = "dashboards/data"
    powerbi_dir: str = "data/exports/powerbi"


def _interpolate(value: Any) -> Any:
    if isinstance(value, str):

        def repl(m: re.Match[str]) -> str:
            return os.environ.get(m.group(1), m.group(2) if m.group(2) is not None else "")

        out = _ENV_PATTERN.sub(repl, value)
        return out if out != "" else None
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a mapping")
    return data


def load_settings(env: str | None = None, overrides: dict[str, Any] | None = None) -> Settings:
    """Load settings for ``env`` (falls back to ``$FINTXN_ENV`` then ``local``)."""
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    env = env or os.environ.get("FINTXN_ENV", "local")
    raw = _deep_merge(_read_yaml(CONFIG_DIR / "base.yaml"), _read_yaml(CONFIG_DIR / f"{env}.yaml"))
    raw = _deep_merge(raw, overrides or {})
    raw["env"] = env
    return Settings.model_validate(_interpolate(raw))


@lru_cache(maxsize=4)
def get_settings(env: str | None = None) -> Settings:
    return load_settings(env)
