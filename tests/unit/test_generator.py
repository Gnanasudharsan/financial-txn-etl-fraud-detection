from __future__ import annotations

import json
from collections import Counter

import numpy as np
import pandas as pd
import pytest

from fintxn.config import load_settings
from fintxn.generate.dimensions import build_dimensions, luhn_valid, make_pans
from fintxn.generate.dirty import format_feed, inject_defects
from fintxn.generate.ledger import build_ledger, build_settlement
from fintxn.generate.runner import chunk_windows, run_generation
from fintxn.generate.transactions import finalize, simulate_fraud, simulate_legit
from fintxn.storage import LocalObjectStore


@pytest.fixture(scope="module")
def cfg():
    return load_settings("ci").data


@pytest.fixture(scope="module")
def dims(cfg):
    return build_dimensions(cfg)


def test_pans_are_luhn_valid() -> None:
    rng = np.random.default_rng(1)
    pans = make_pans(rng, np.array(["VISA", "MASTERCARD", "AMEX", "DISCOVER"] * 25))
    assert all(luhn_valid(p) for p in pans)
    assert {len(p) for p in pans} == {15, 16}


def test_dimensions_shapes_and_keys(cfg, dims) -> None:
    assert len(dims.customers) == cfg.n_customers
    assert len(dims.cards) == cfg.n_cards
    assert len(dims.merchants) == cfg.n_merchants
    assert dims.customers["customer_id"].is_unique
    assert dims.cards["card_number"].is_unique
    assert set(dims.cards["customer_id"]) == set(dims.customers["customer_id"])


def test_dimensions_are_reproducible(cfg) -> None:
    a = build_dimensions(cfg).cards["card_number"].tolist()
    b = build_dimensions(cfg).cards["card_number"].tolist()
    assert a == b


def _chunk(cfg, dims, n=5000):
    win = chunk_windows(cfg)[3]
    rng = np.random.default_rng([cfg.seed, 99])
    legit = simulate_legit(rng, dims, cfg, win, n)
    fraud = simulate_fraud(rng, dims, cfg.model_copy(update={}), win, 1_000_000)
    return rng, win, finalize(rng, dims, pd.concat([legit, fraud], ignore_index=True))


def test_transactions_inside_window_and_labelled(cfg, dims) -> None:
    _, win, df = _chunk(cfg, dims)
    assert df["event_ts"].between(win.start_ts - 86400, win.end_ts + 86400).all()
    assert df["transaction_id"].is_unique
    assert set(df["fraud_type"].dropna()) == {
        "card_testing",
        "account_takeover",
        "velocity_burst",
        "high_amount_night",
        "high_risk_mcc",
    }
    ct = df[df["fraud_type"] == "card_testing"]
    assert (ct["amount_usd"] < 5).mean() > 0.6  # bursts of tiny probes
    assert df.loc[df["is_fraud"] == 0, "amount_usd"].gt(0).all()


def test_defect_injection_counts(cfg, dims) -> None:
    rng, _, df = _chunk(cfg, dims)
    counts: Counter[str] = Counter()
    feed = format_feed(rng, df, np.zeros(len(df), dtype=int), json_feed=True)
    dirty = inject_defects(rng, feed, cfg.dirty, counts)
    assert len(dirty) == len(df) + counts["exact_duplicate"] + counts["near_duplicate"]
    assert counts["malformed_amount"] > 0
    assert counts["bad_timestamp"] > 0
    assert dirty["transaction_id"].duplicated().sum() >= counts["exact_duplicate"]


def test_settlement_breaks_and_truth(cfg, dims) -> None:
    rng, _, df = _chunk(cfg, dims)
    ledger = build_ledger(df)
    assert (
        ledger["transaction_id"].isin(df.loc[df["auth_status"] == "APPROVED", "transaction_id"])
    ).all()
    counts: Counter[str] = Counter()
    settle, truth = build_settlement(rng, ledger, cfg.recon, 0, counts)
    assert {"exact_match", "tolerance_match", "missing_in_settlement"} <= set(truth["expected"])
    missing = truth.loc[truth["expected"] == "missing_in_settlement", "key"]
    assert not settle["transaction_ref"].isin(missing).any()


def test_run_generation_writes_partitioned_files(tmp_path) -> None:
    s = load_settings("ci", overrides={"data": {"days": 14}})
    store = LocalObjectStore(tmp_path)
    manifest = run_generation(s, rows=2000, store=store)
    keys = store.list_keys("raw")
    assert any(k.endswith(".csv") and "source=card_present" in k for k in keys)
    assert any(k.endswith(".json") and "source=card_not_present" in k for k in keys)
    assert {"dims/customers.csv", "dims/cards.csv", "dims/merchants.csv"} <= set(keys)
    saved = json.loads(store.get_bytes("raw", "_manifest/generation.json"))
    assert saved["counts"]["clean_transactions"] == manifest["counts"]["clean_transactions"]
    assert 0.002 < manifest["fraud_rate"] < 0.03
