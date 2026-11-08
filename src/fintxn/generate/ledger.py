"""Internal ledger postings and processor settlement records with deliberate breaks.

The ledger records every APPROVED transaction (posted on its UTC event date). The settlement
file is derived from it, then perturbed so the reconciliation job has real work to do:

=====================  ============================================================
break                  injection
=====================  ============================================================
missing_in_settlement  ledger row never settled
amount_mismatch        settled amount off by 2-50%
tolerance (cents)      settled amount off by 0.01-0.05 (should still match)
timing                 settled 3-6 days after posting (outside the +-2 day window)
duplicate              same settlement emitted twice under different settlement ids
missing reference      ``transaction_ref`` blank; must match on auth_code + merchant
missing_in_ledger      orphan settlement whose reference is unknown to the ledger
=====================  ============================================================

A ground-truth file (``_truth/recon``) lets the reconciliation job be scored.
"""

from __future__ import annotations

from collections import Counter

import numpy as np
import pandas as pd

from fintxn.config import ReconConfig

DAY = 86_400


def build_ledger(df: pd.DataFrame) -> pd.DataFrame:
    appr = df[df["auth_status"] == "APPROVED"]
    posted = pd.to_datetime(appr["event_ts"], unit="s", utc=True).dt.date
    return pd.DataFrame(
        {
            "ledger_entry_id": "LE" + appr["transaction_id"].str[3:],
            "transaction_id": appr["transaction_id"].to_numpy(),
            "account_id": appr["customer_id"].to_numpy(),
            "merchant_id": appr["merchant_id"].to_numpy(),
            "auth_code": appr["auth_code"].to_numpy(),
            "amount": appr["amount"].to_numpy(),
            "currency": appr["currency"].to_numpy(),
            "entry_type": "DEBIT",
            "posted_date": posted.to_numpy(),
        }
    ).reset_index(drop=True)


def build_settlement(
    rng: np.random.Generator,
    ledger: pd.DataFrame,
    cfg: ReconConfig,
    chunk_index: int,
    counts: Counter[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    n = len(ledger)
    truth = np.full(n, "exact_match", dtype=object)
    u = rng.random(n)
    edges = np.cumsum([
        cfg.missing_settlement_rate, cfg.amount_mismatch_rate, cfg.cents_diff_rate,
        cfg.timing_break_rate,
    ])  # fmt: skip
    missing = u < edges[0]
    mismatch = (u >= edges[0]) & (u < edges[1])
    cents = (u >= edges[1]) & (u < edges[2])
    timing = (u >= edges[2]) & (u < edges[3])

    lag = np.where(rng.random(n) < 0.85, 1, 2)
    lag = np.where(timing, rng.integers(3, 7, n), lag)
    amount = ledger["amount"].to_numpy().astype(float).copy()
    sign = np.where(rng.random(n) < 0.5, -1.0, 1.0)
    amount = np.where(
        mismatch, np.round(amount * (1 + sign * rng.uniform(0.02, 0.5, n)), 2), amount
    )
    amount = np.where(cents, np.round(amount + sign * rng.integers(1, 6, n) / 100, 2), amount)
    amount = np.maximum(amount, 0.01)

    truth[lag == 2] = "tolerance_match"
    truth[cents] = "tolerance_match"
    truth[mismatch] = "amount_mismatch"
    truth[timing] = "timing"
    truth[missing] = "missing_in_settlement"

    posted = pd.to_datetime(ledger["posted_date"])
    settle = pd.DataFrame(
        {
            "settlement_id": [f"ST{chunk_index:03d}{i:08d}" for i in range(n)],
            "transaction_ref": ledger["transaction_id"].to_numpy(),
            "auth_code": ledger["auth_code"].to_numpy(),
            "merchant_id": ledger["merchant_id"].to_numpy(),
            "settled_amount": amount,
            "currency": ledger["currency"].to_numpy(),
            "settlement_date": (posted + pd.to_timedelta(lag, unit="D")).dt.date.to_numpy(),
            "processor": "ACME_PROCESSING",
        }
    )[~missing].reset_index(drop=True)

    m = len(settle)
    no_ref = rng.random(m) < cfg.missing_ref_rate
    settle.loc[no_ref, "transaction_ref"] = None
    counts["settlement_missing_ref"] += int(no_ref.sum())

    dup_idx = rng.choice(m, size=int(rng.binomial(m, cfg.duplicate_settlement_rate)), replace=False)
    dups = settle.iloc[dup_idx].copy()
    dups["settlement_id"] = [f"SD{chunk_index:03d}{i:08d}" for i in range(len(dups))]
    k_orphan = int(rng.binomial(m, cfg.orphan_settlement_rate))
    orphans = settle.iloc[rng.choice(m, size=k_orphan, replace=False)].copy()
    raw = rng.integers(0, 256, size=(k_orphan, 8), dtype=np.uint8)
    orphans["transaction_ref"] = ["tx_" + r.tobytes().hex() for r in raw]
    orphans["auth_code"] = [f"Z{rng.integers(10000, 99999)}" for _ in range(k_orphan)]
    orphans["settlement_id"] = [f"SO{chunk_index:03d}{i:08d}" for i in range(k_orphan)]
    settle = pd.concat([settle, dups, orphans], ignore_index=True)

    truth_df = pd.concat(
        [
            pd.DataFrame(
                {
                    "key": ledger["transaction_id"].to_numpy(),
                    "key_type": "transaction_id",
                    "expected": truth,
                }
            ),
            pd.DataFrame(
                {
                    "key": dups["settlement_id"].to_numpy(),
                    "key_type": "settlement_id",
                    "expected": "duplicate",
                }
            ),
            pd.DataFrame(
                {
                    "key": orphans["settlement_id"].to_numpy(),
                    "key_type": "settlement_id",
                    "expected": "missing_in_ledger",
                }
            ),
        ],
        ignore_index=True,
    )
    for label, c in Counter(truth_df["expected"]).items():
        counts[f"recon_{label}"] += int(c)
    return settle, truth_df
