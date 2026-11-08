"""Secret lookup: environment variable first (local ``.env`` / GitHub Secrets), then a
Databricks secret scope when running on Databricks. Secrets are never read from config files."""

from __future__ import annotations

import os

SCOPE = os.environ.get("FINTXN_SECRET_SCOPE", "fintxn")
# env var -> key inside the Databricks secret scope
SCOPE_KEYS = {
    "FINTXN_TOKEN_KEY": "token_key",
    "FINTXN_ENCRYPTION_KEY": "encryption_key",
}


def get_secret(env_var: str) -> str:
    value = os.environ.get(env_var, "")
    if value:
        return value
    if "DATABRICKS_RUNTIME_VERSION" in os.environ and env_var in SCOPE_KEYS:
        try:
            from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]

            return str(dbutils.secrets.get(scope=SCOPE, key=SCOPE_KEYS[env_var]))
        except Exception:  # pragma: no cover - only reachable on Databricks
            return ""
    return ""
