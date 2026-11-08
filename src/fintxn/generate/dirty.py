"""Raw-feed formatting and dirty-data injection.

Two kinds of mess are produced on purpose:

* **cosmetic variation** that silver must standardise (timestamp formats and offsets, ``$``
  signs and thousands separators, lower-case currency codes, inconsistent casing), and
* **defects** that silver must quarantine (nulls, malformed amounts, unparseable or future
  timestamps, invalid currency codes, exact and near duplicates, corrupt lines, schema drift).

Counts of every injected defect are returned so the generation manifest can be audited.
"""

from __future__ import annotations

import io
import json
from collections import Counter
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from fintxn.config import DirtyConfig

CP_COLUMNS = [
    "transaction_id", "event_ts", "card_number", "customer_id", "merchant_id", "merchant_name",
    "mcc", "amount", "currency", "channel", "entry_mode", "latitude", "longitude", "city",
    "country", "device_id", "ip_address", "auth_status", "auth_code",
]  # fmt: skip
CNP_COLUMNS = CP_COLUMNS
DRIFT_COLUMNS = ["wallet_type", "promo_code"]
REQUIRED_FOR_NULLS = ["amount", "event_ts", "merchant_id", "card_number", "currency"]
FUTURE_YEAR = 2031


def _fmt_ts(
    rng: np.random.Generator, ts: np.ndarray, offset_h: np.ndarray, json_feed: bool
) -> np.ndarray:
    """Render epoch seconds in a mix of formats a real multi-source feed would contain."""
    n = len(ts)
    dt = pd.to_datetime(ts, unit="s", utc=True)
    out = np.empty(n, dtype=object)
    p = [0.6, 0.2, 0.15, 0.05] if json_feed else [0.62, 0.23, 0.15, 0.0]
    choice = rng.choice(4, size=n, p=p)
    a = choice == 0
    out[a] = dt[a].strftime("%Y-%m-%dT%H:%M:%SZ")
    b = choice == 1
    out[b] = dt[b].strftime("%Y-%m-%d %H:%M:%S")
    c = np.flatnonzero(choice == 2)
    if c.size:
        local = dt[c] + pd.to_timedelta(offset_h[c], unit="h")
        sign = np.where(offset_h[c] < 0, "-", "+")
        hh = np.abs(offset_h[c]).astype(int)
        out[c] = [
            f"{t}{s}{h:02d}:00"
            for t, s, h in zip(local.strftime("%Y-%m-%dT%H:%M:%S"), sign, hh, strict=True)
        ]
    d = choice == 3
    out[d] = (ts[d] * 1000).astype(np.int64).astype(str)
    return out


def _fmt_amount(rng: np.random.Generator, amount: np.ndarray) -> np.ndarray:
    out = np.array([f"{a:.2f}" for a in amount], dtype=object)
    cos = np.flatnonzero(rng.random(len(amount)) < 0.02)
    for i in cos:
        a = amount[i]
        k = rng.integers(0, 3)
        out[i] = f"${a:.2f}" if k == 0 else (f" {a:.2f} " if k == 1 else f"{a:,.2f}")
    return out


def format_feed(
    rng: np.random.Generator, df: pd.DataFrame, offset_h: np.ndarray, json_feed: bool
) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "transaction_id": df["transaction_id"].to_numpy(),
            "event_ts": _fmt_ts(rng, df["event_ts"].to_numpy(), offset_h, json_feed),
            "card_number": df["card_number"].to_numpy(),
            "customer_id": df["customer_id"].to_numpy(),
            "merchant_id": df["merchant_id"].to_numpy(),
            "merchant_name": df["merchant_name"].to_numpy(),
            "mcc": df["mcc"].to_numpy(),
            "amount": _fmt_amount(rng, df["amount"].to_numpy()),
            "currency": df["currency"].to_numpy().copy(),
            "channel": df["channel"].to_numpy(),
            "entry_mode": df["entry_mode"].to_numpy(),
            "latitude": df["latitude"].to_numpy(),
            "longitude": df["longitude"].to_numpy(),
            "city": df["city"].to_numpy().copy(),
            "country": df["country"].to_numpy(),
            "device_id": df["device_id"].to_numpy(),
            "ip_address": df["ip_address"].to_numpy(),
            "auth_status": df["auth_status"].to_numpy(),
            "auth_code": df["auth_code"].to_numpy(),
        }
    )
    n = len(out)
    # cosmetic: currency casing / padding, text casing
    cur = np.flatnonzero(rng.random(n) < 0.03)
    out.loc[cur, "currency"] = [
        f" {c.lower()} " if i % 2 else c.lower() for i, c in enumerate(out.loc[cur, "currency"])
    ]
    txt = np.flatnonzero(rng.random(n) < 0.03)
    out.loc[txt, "city"] = out.loc[txt, "city"].str.upper()
    txt = np.flatnonzero(rng.random(n) < 0.03)
    out.loc[txt, "merchant_name"] = "  " + out.loc[txt, "merchant_name"].str.lower() + " "
    return out


def inject_defects(
    rng: np.random.Generator, feed: pd.DataFrame, cfg: DirtyConfig, counts: Counter[str]
) -> pd.DataFrame:
    n = len(feed)
    if n == 0:
        return feed
    feed = feed.copy()

    def pick(rate: float) -> np.ndarray:
        k = int(rng.binomial(n, rate))
        return rng.choice(n, size=k, replace=False) if k else np.array([], dtype=np.int64)

    idx = pick(cfg.null_rate)
    cols = rng.choice(REQUIRED_FOR_NULLS, size=len(idx))
    for i, col in zip(idx, cols, strict=True):
        feed.iat[i, feed.columns.get_loc(col)] = None
        counts[f"null_{col}"] += 1

    idx = pick(cfg.malformed_amount_rate)
    bad = np.array(["abc", "N/A", "12,34", "1.2.3", "-45.10", "0.00", "--", "9e9999"], dtype=object)
    feed.loc[idx, "amount"] = bad[rng.integers(0, len(bad), len(idx))]
    counts["malformed_amount"] += len(idx)

    idx = pick(cfg.bad_timestamp_rate)
    badts = np.array(
        ["2026-13-45 25:61:00", "not_a_date", "31/02/2026 10:00", "2026-02-30T10:00:00Z", "??"],
        dtype=object,
    )
    feed.loc[idx, "event_ts"] = badts[rng.integers(0, len(badts), len(idx))]
    counts["bad_timestamp"] += len(idx)

    idx = pick(cfg.future_timestamp_rate)
    feed.loc[idx, "event_ts"] = [
        datetime(FUTURE_YEAR, int(m), int(d), 12, 0, tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        for m, d in zip(rng.integers(1, 13, len(idx)), rng.integers(1, 28, len(idx)), strict=True)
    ]
    counts["future_timestamp"] += len(idx)

    idx = pick(cfg.invalid_currency_rate)
    badc = np.array(["XXX", "US$", "EURO", "ZZZ", "123"], dtype=object)
    feed.loc[idx, "currency"] = badc[rng.integers(0, len(badc), len(idx))]
    counts["invalid_currency"] += len(idx)

    # exact duplicates (replayed messages)
    idx = pick(cfg.exact_dup_rate)
    dups = feed.iloc[idx]
    counts["exact_duplicate"] += len(idx)
    # near duplicates: double-submit -> new id, same card/merchant/amount, a few seconds later
    idx = pick(cfg.near_dup_rate)
    near = feed.iloc[idx].copy()
    ts = pd.to_datetime(near["event_ts"], utc=True, errors="coerce", format="ISO8601")
    ok = ts.notna().to_numpy() & near["amount"].notna().to_numpy()
    near = near[ok]
    shifted = ts[ok] + pd.to_timedelta(rng.integers(1, 30, ok.sum()), unit="s")
    near["event_ts"] = shifted.dt.strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()
    raw = rng.integers(0, 256, size=(len(near), 8), dtype=np.uint8)
    near["transaction_id"] = ["tx_" + r.tobytes().hex() for r in raw]
    counts["near_duplicate"] += len(near)

    out = pd.concat([feed, dups, near], ignore_index=True)
    order = rng.permutation(len(out))  # feeds are not perfectly time-ordered
    return out.iloc[order].reset_index(drop=True)


def to_csv_bytes(
    rng: np.random.Generator, feed: pd.DataFrame, corrupt_rate: float, counts: Counter[str]
) -> bytes:
    """CSV with a few corrupt lines: unescaped delimiter in merchant_name shifts columns."""
    n = len(feed)
    k = int(rng.binomial(n, corrupt_rate)) if n else 0
    bad_idx = set(rng.choice(n, size=k, replace=False).tolist()) if k else set()
    good = feed.drop(index=list(bad_idx)) if bad_idx else feed
    buf = io.StringIO()
    good[CP_COLUMNS].to_csv(buf, index=False)
    for i in bad_idx:
        row = feed.iloc[i]
        vals = ["" if pd.isna(row[c]) else str(row[c]) for c in CP_COLUMNS]
        vals[CP_COLUMNS.index("merchant_name")] = f"{vals[CP_COLUMNS.index('merchant_name')]}, LLC"
        buf.write(",".join(vals) + "\n")
    counts["corrupt_line"] += len(bad_idx)
    return buf.getvalue().encode()


def to_jsonl_bytes(
    rng: np.random.Generator,
    feed: pd.DataFrame,
    corrupt_rate: float,
    drift: bool,
    counts: Counter[str],
) -> bytes:
    records = feed[CNP_COLUMNS].to_dict(orient="records")
    lines: list[str] = []
    for rec in records:
        clean = {k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in rec.items()}
        if drift:
            clean["wallet_type"] = str(rng.choice(["APPLE_PAY", "GOOGLE_PAY", "NONE"]))
            clean["promo_code"] = None if rng.random() < 0.8 else f"PROMO{rng.integers(10, 99)}"
        line = json.dumps(clean, separators=(",", ":"))
        if rng.random() < corrupt_rate:
            line = line[: max(10, len(line) // 2)]  # truncated message
            counts["corrupt_line"] += 1
        lines.append(line)
    if drift:
        counts["schema_drift_rows"] += len(records)
    return ("\n".join(lines) + "\n").encode()
