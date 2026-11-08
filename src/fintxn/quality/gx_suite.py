"""Great Expectations (GX Core 1.x) suite for silver transactions.

Row-level rules in :mod:`fintxn.silver.transform` decide what is quarantined; this suite is the
independent, dataset-level contract on what was *published* to silver. A failure here means a
bug in the cleaning logic, not dirty input.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame

from fintxn.generate.catalog import VALID_CURRENCIES
from fintxn.silver.schemas import AUTH_STATUSES, CHANNELS, ENTRY_MODES

SUITE_NAME = "silver_transactions"


def build_expectations(
    merchant_ids: list[str], customer_ids: list[str], min_rows: int
) -> list[Any]:
    import great_expectations.expectations as gxe

    not_null = ["transaction_id", "event_ts", "card_token", "customer_id", "merchant_id", "amount",
                "amount_usd", "currency", "channel", "auth_status"]  # fmt: skip
    exps: list[Any] = [gxe.ExpectColumnValuesToNotBeNull(column=c) for c in not_null]
    exps += [
        gxe.ExpectTableRowCountToBeBetween(min_value=min_rows),
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id"),
        gxe.ExpectColumnValuesToBeBetween(column="amount_usd", min_value=0.01, max_value=100_000),
        gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90),
        gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=sorted(VALID_CURRENCIES)),
        gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=list(CHANNELS)),
        gxe.ExpectColumnValuesToBeInSet(column="auth_status", value_set=list(AUTH_STATUSES)),
        gxe.ExpectColumnValuesToBeInSet(column="entry_mode", value_set=list(ENTRY_MODES)),
        gxe.ExpectColumnValuesToMatchRegex(column="mcc", regex=r"^[0-9]{4}$"),
        gxe.ExpectColumnValuesToMatchRegex(column="card_token", regex=r"^[0-9a-f]{64}$"),
        # referential integrity against conformed dimensions
        gxe.ExpectColumnValuesToBeInSet(column="merchant_id", value_set=merchant_ids),
        gxe.ExpectColumnValuesToBeInSet(column="customer_id", value_set=customer_ids),
    ]
    return exps


def run_suite(
    df: DataFrame, merchant_ids: list[str], customer_ids: list[str], min_rows: int = 1
) -> dict[str, Any]:
    import great_expectations as gx

    logging.getLogger("great_expectations").setLevel(logging.ERROR)
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name="silver")
    asset = source.add_dataframe_asset(name="transactions")
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))
    for e in build_expectations(merchant_ids, customer_ids, min_rows):
        suite.add_expectation(e)
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite, result_format="BASIC")
    rows = []
    for r in result.results:
        cfg = r.expectation_config
        res = r.result or {}
        rows.append(
            {
                "expectation": cfg.type if cfg else "unknown",
                "column": (cfg.kwargs.get("column") if cfg else None),
                "success": bool(r.success),
                "element_count": res.get("element_count"),
                "unexpected_count": res.get("unexpected_count"),
                "unexpected_percent": res.get("unexpected_percent"),
                "observed_value": None
                if res.get("observed_value") is None
                else str(res.get("observed_value")),
            }
        )
    passed = sum(1 for r in rows if r["success"])
    return {
        "suite": SUITE_NAME,
        "success": bool(result.success),
        "evaluated": len(rows),
        "passed": passed,
        "pass_rate": round(passed / len(rows), 4) if rows else 1.0,
        "results": rows,
    }
