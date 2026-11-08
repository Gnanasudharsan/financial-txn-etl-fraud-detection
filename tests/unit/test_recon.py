from __future__ import annotations

from datetime import date
from decimal import Decimal as D

import pytest

from fintxn.recon.engine import reconcile, summarize

pytestmark = pytest.mark.spark

LEDGER = (
    "ledger_entry_id string, transaction_id string, account_id string, merchant_id string, "
    "auth_code string, amount decimal(18,2), currency string, entry_type string, posted_date date"
)
SETTLE = (
    "settlement_id string, transaction_ref string, auth_code string, merchant_id string, "
    "settled_amount decimal(18,2), currency string, settlement_date date, processor string"
)
d0 = date(2026, 3, 1)


def _l(tid, amt, auth, merch="M1", posted=d0):
    return (f"L{tid}", tid, "C1", merch, auth, D(amt), "USD", "DEBIT", posted)


def _s(sid, ref, amt, auth, settled, merch="M1"):
    return (sid, ref, auth, merch, D(amt), "USD", settled, "P")


@pytest.fixture
def results(spark):
    ledger = spark.createDataFrame(
        [
            _l("t_exact", "10.00", "A1"),
            _l("t_tol_cents", "10.00", "A2"),
            _l("t_tol_t2", "10.00", "A3"),
            _l("t_mismatch", "10.00", "A4"),
            _l("t_timing", "10.00", "A5"),
            _l("t_missing", "10.00", "A6"),
            _l("t_noref", "25.00", "A7"),
            _l("t_dup", "5.00", "A8"),
        ],
        LEDGER,
    )
    settle = spark.createDataFrame(
        [
            _s("S1", "t_exact", "10.00", "A1", date(2026, 3, 2)),
            _s("S2", "t_tol_cents", "10.03", "A2", date(2026, 3, 2)),
            _s("S3", "t_tol_t2", "10.00", "A3", date(2026, 3, 3)),
            _s("S4", "t_mismatch", "13.00", "A4", date(2026, 3, 2)),
            _s("S5", "t_timing", "10.00", "A5", date(2026, 3, 6)),
            _s("S6", None, "25.00", "A7", date(2026, 3, 2)),
            _s("S7", "t_dup", "5.00", "A8", date(2026, 3, 2)),
            _s("S8", "t_dup", "5.00", "A8", date(2026, 3, 2)),
            _s("S9", "t_unknown", "9.99", "Z9", date(2026, 3, 2)),
        ],
        SETTLE,
    )
    return reconcile(ledger, settle, amount_tol=0.05, date_tol=2)


def test_each_record_classified(results) -> None:
    ledger_side = {
        r["transaction_id"]: r["category"]
        for r in results.where("posted_date IS NOT NULL").collect()
    }
    assert ledger_side == {
        "t_exact": "exact_match",
        "t_tol_cents": "tolerance_match",
        "t_tol_t2": "tolerance_match",
        "t_mismatch": "amount_mismatch",
        "t_timing": "timing",
        "t_missing": "missing_in_settlement",
        "t_noref": "exact_match",
        "t_dup": "exact_match",
    }
    settle_only = {
        r["settlement_id"]: r["category"] for r in results.where("posted_date IS NULL").collect()
    }
    assert settle_only == {"S8": "duplicate", "S9": "missing_in_ledger"}


def test_secondary_key_match_method(results) -> None:
    row = results.where("transaction_id = 't_noref'").first()
    assert row["match_method"] == "auth_code+merchant" and row["settlement_id"] == "S6"


def test_breaks_flag_and_summary(results) -> None:
    breaks = {r["category"] for r in results.where("is_break").collect()}
    assert breaks == {
        "amount_mismatch",
        "timing",
        "missing_in_settlement",
        "missing_in_ledger",
        "duplicate",
    }
    total = sum(r["records"] for r in summarize(results).collect())
    assert total == results.count() == 10
