"""Generation driver: dimensions once, then one chunk (default 7 days) at a time.

Raw landing layout (bucket/folder ``raw``)::

    dims/{customers,cards,merchants}.csv
    transactions/source=card_present/event_date=YYYY-MM-DD/part-cNNN.csv       (POS + ATM)
    transactions/source=card_not_present/event_date=YYYY-MM-DD/part-cNNN.json  (online, JSONL)
    labels/labels-cNNN.csv                  (fraud reports / chargebacks, delayed)
    ledger/posted_date=YYYY-MM-DD/ledger-cNNN.csv
    settlement/settlement_date=YYYY-MM-DD/settlement-cNNN.csv
    _truth/recon/truth-cNNN.csv             (injected recon breaks, for scoring only)
    _manifest/generation.json
"""

from __future__ import annotations

import io
import json
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from fintxn.config import DataConfig, Settings
from fintxn.generate.dimensions import build_dimensions, public_columns
from fintxn.generate.dirty import format_feed, inject_defects, to_csv_bytes, to_jsonl_bytes
from fintxn.generate.ledger import build_ledger, build_settlement
from fintxn.generate.transactions import DAY, ChunkWindow, finalize, simulate_fraud, simulate_legit
from fintxn.logging import get_logger
from fintxn.storage import ObjectStore, get_object_store

log = get_logger("fintxn.generate")
EXPECTED_FRAUD_SHARE = 0.0086
RAW_PREFIXES = (
    "dims/",
    "transactions/",
    "labels/",
    "ledger/",
    "settlement/",
    "_truth/",
    "_manifest/",
)


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode()


def _epoch(d: Any) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())


def chunk_windows(cfg: DataConfig) -> list[ChunkWindow]:
    start = _epoch(cfg.start_date)
    out = []
    for i, off in enumerate(range(0, cfg.days, cfg.chunk_days)):
        out.append(ChunkWindow(i, start + off * DAY, min(cfg.chunk_days, cfg.days - off)))
    return out


def run_generation(
    settings: Settings,
    rows: int | None = None,
    chunks: tuple[int, int] | None = None,
    clean: bool = True,
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    cfg = settings.data.model_copy(update={"n_transactions": rows} if rows else {})
    store = store or get_object_store(settings)
    windows = chunk_windows(cfg)
    lo, hi = chunks if chunks else (0, len(windows) - 1)
    selected = [w for w in windows if lo <= w.index <= hi]

    if clean:
        removed = sum(store.delete_prefix("raw", p) for p in RAW_PREFIXES)
        log.info("cleared %d existing raw objects", removed)

    dims = build_dimensions(cfg)
    log.info(
        "dimensions: %d customers, %d cards, %d merchants",
        len(dims.customers),
        len(dims.cards),
        len(dims.merchants),
    )
    counts: Counter[str] = Counter()
    futures: list[Future[None]] = []
    pool = ThreadPoolExecutor(max_workers=8)

    def put(key: str, data: bytes) -> None:
        futures.append(pool.submit(store.put_bytes, "raw", key, data))
        counts["files"] += 1
        counts["bytes"] += len(data)

    put("dims/customers.csv", _csv_bytes(public_columns(dims.customers)))
    put("dims/cards.csv", _csv_bytes(public_columns(dims.cards)))
    put("dims/merchants.csv", _csv_bytes(public_columns(dims.merchants)))

    n_legit_total = int(cfg.n_transactions * (1 - EXPECTED_FRAUD_SHARE))
    offsets = dims.customers["home_utc_offset"].to_numpy()
    for win in selected:
        rng = np.random.default_rng([cfg.seed, win.index + 1])
        n_target = round(n_legit_total * win.days / cfg.days)
        legit = simulate_legit(rng, dims, cfg, win, n_target)
        fraud = simulate_fraud(rng, dims, cfg, win, len(legit))
        parts = [p for p in (legit, fraud) if len(p)]
        df = finalize(rng, dims, pd.concat(parts, ignore_index=True))
        counts["clean_transactions"] += len(df)
        counts["fraud_transactions"] += int(df["is_fraud"].sum())
        for ft, c in df.loc[df["is_fraud"] == 1, "fraud_type"].value_counts().items():
            counts[f"fraud_{ft}"] += int(c)

        labels = df.loc[df["is_fraud"] == 1, ["transaction_id", "fraud_type", "event_ts"]].copy()
        delay = rng.integers(1, 46, len(labels)) * DAY + rng.integers(0, DAY, len(labels))
        labels["reported_at"] = pd.to_datetime(
            labels["event_ts"] + delay, unit="s", utc=True
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        put(f"labels/labels-c{win.index:03d}.csv", _csv_bytes(labels.drop(columns="event_ts")))

        ledger = build_ledger(df)
        settle, truth = build_settlement(rng, ledger, cfg.recon, win.index, counts)
        counts["ledger_rows"] += len(ledger)
        counts["settlement_rows"] += len(settle)
        for d, part in ledger.groupby("posted_date"):
            put(f"ledger/posted_date={d}/ledger-c{win.index:03d}.csv", _csv_bytes(part))
        for d, part in settle.groupby("settlement_date"):
            put(f"settlement/settlement_date={d}/settlement-c{win.index:03d}.csv", _csv_bytes(part))
        put(f"_truth/recon/truth-c{win.index:03d}.csv", _csv_bytes(truth))

        day = pd.to_datetime(df["event_ts"], unit="s", utc=True).dt.date.to_numpy()
        is_cnp = (df["channel"] == "ONLINE").to_numpy()
        off = offsets[df["cust"].to_numpy()]
        for d in np.unique(day):
            for cnp in (False, True):
                sel = (day == d) & (is_cnp == cnp)
                if not sel.any():
                    continue
                part = df[sel].reset_index(drop=True)
                feed = format_feed(rng, part, off[sel], json_feed=cnp)
                feed = inject_defects(rng, feed, cfg.dirty, counts)
                counts["raw_transaction_rows"] += len(feed)
                if cnp:
                    drift = bool(rng.random() < cfg.dirty.schema_drift_file_share)
                    data = to_jsonl_bytes(rng, feed, cfg.dirty.corrupt_line_rate, drift, counts)
                    put(
                        f"transactions/source=card_not_present/event_date={d}/part-c{win.index:03d}.json",
                        data,
                    )
                else:
                    data = to_csv_bytes(rng, feed, cfg.dirty.corrupt_line_rate, counts)
                    put(
                        f"transactions/source=card_present/event_date={d}/part-c{win.index:03d}.csv",
                        data,
                    )
        log.info(
            "chunk %02d (%s +%dd): %d txns, %d fraud",
            win.index,
            datetime.fromtimestamp(win.start_ts, UTC).date(),
            win.days,
            len(df),
            int(df["is_fraud"].sum()),
        )

    for f in futures:
        f.result()
    pool.shutdown()
    manifest: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "seed": cfg.seed,
        "requested_transactions": cfg.n_transactions,
        "chunks": [lo, hi],
        "start_date": cfg.start_date.isoformat(),
        "days": cfg.days,
        "customers": len(dims.customers),
        "cards": len(dims.cards),
        "merchants": len(dims.merchants),
        "counts": dict(sorted(counts.items())),
        "fraud_rate": round(counts["fraud_transactions"] / max(counts["clean_transactions"], 1), 5),
        "seconds": round(time.perf_counter() - t0, 1),
    }
    store.put_bytes("raw", "_manifest/generation.json", json.dumps(manifest, indent=2).encode())
    log.info("generation done in %.1fs: %s", manifest["seconds"], json.dumps(manifest["counts"]))
    return manifest
