"""Legitimate transaction simulation and fraud-episode injection for one time chunk.

Everything is vectorised with numpy; a chunk (default one week, ~190k rows at 5M scale) fits
comfortably in memory. Each chunk has its own RNG stream ``default_rng([seed, chunk + 1])`` so
chunks are reproducible and independent.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from fintxn.config import DataConfig
from fintxn.generate.catalog import (
    CITIES,
    CITY_UTC_OFFSET,
    COUNTRY_CURRENCY,
    FRAUD_ORIGIN_COUNTRIES,
    FX_PER_USD,
    HIGH_RISK_MCCS,
    MCC_BY_CODE,
)
from fintxn.generate.dimensions import N_FAVORITES, Dimensions

DAY = 86_400
# Relative transaction volume by local hour (0-23).
HOUR_WEIGHTS = np.array(
    [0.6, 0.35, 0.2, 0.15, 0.15, 0.3, 0.9, 2.0, 3.6, 4.3, 4.8, 5.5,
     6.6, 6.2, 5.4, 5.2, 5.6, 6.3, 6.8, 6.0, 4.8, 3.6, 2.4, 1.3]
)  # fmt: skip
NIGHT_WEIGHTS = np.roll(HOUR_WEIGHTS, 5)
DOW_WEIGHTS = np.array([0.94, 0.95, 0.97, 1.0, 1.12, 1.10, 0.92])  # Mon..Sun

CITY_OFFSET = np.array([CITY_UTC_OFFSET[c.name] for c in CITIES])
CITY_LAT = np.array([c.lat for c in CITIES])
CITY_LON = np.array([c.lon for c in CITIES])
CITY_COUNTRY = np.array([c.country for c in CITIES], dtype=object)
CITY_NAME = np.array([c.name for c in CITIES], dtype=object)
US_CITY_IDX = np.flatnonzero(CITY_COUNTRY == "US")
FRAUD_CITY_IDX = np.flatnonzero(np.isin(CITY_COUNTRY, FRAUD_ORIGIN_COUNTRIES))
_IP_PREFIX = {"NG": "102.89", "RO": "86.120", "PH": "112.198", "UA": "91.200", "MX": "187.190",
              "IN": "49.36"}  # fmt: skip

COLUMNS = [
    "event_ts", "cust", "card", "merch", "channel", "entry_mode", "amount_usd", "device_id",
    "ip_address", "latitude", "longitude", "city", "country", "auth_status", "is_fraud",
    "fraud_type",
]  # fmt: skip


@dataclass
class ChunkWindow:
    index: int
    start_ts: int  # epoch seconds, inclusive
    days: int

    @property
    def end_ts(self) -> int:
        return self.start_ts + self.days * DAY


def _home_ip(cust: np.ndarray) -> np.ndarray:
    a = 24 + (cust * 7919) % 180
    b = (cust * 104729) % 256
    c = (cust * 1299709) % 256
    d = 1 + (cust * 15485863) % 254
    return np.array(
        [f"{w}.{x}.{y}.{z}" for w, x, y, z in zip(a, b, c, d, strict=True)], dtype=object
    )


def _random_ip(rng: np.random.Generator, countries: np.ndarray) -> np.ndarray:
    out = np.empty(len(countries), dtype=object)
    r = rng.integers(0, 256, size=(len(countries), 4))
    for i, cc in enumerate(countries):
        pre = _IP_PREFIX.get(str(cc))
        out[i] = (
            f"{pre}.{r[i, 2]}.{r[i, 3]}"
            if pre
            else f"{r[i, 0] % 200 + 20}.{r[i, 1]}.{r[i, 2]}.{r[i, 3]}"
        )
    return out


def _new_devices(rng: np.random.Generator, n: int) -> np.ndarray:
    raw = rng.integers(0, 256, size=(n, 5), dtype=np.uint8)
    return np.array(["dev_" + r.tobytes().hex() for r in raw], dtype=object)


def _lognormal_amount(rng: np.random.Generator, mcc: np.ndarray, mult: np.ndarray) -> np.ndarray:
    med = np.array([MCC_BY_CODE[m].median_usd for m in mcc])
    sig = np.array([MCC_BY_CODE[m].sigma for m in mcc])
    amt = rng.lognormal(np.log(med * mult), sig)
    return np.round(np.clip(amt, 0.5, 25_000), 2)


def _local_to_utc(
    day_start: np.ndarray, local_seconds: np.ndarray, offset_h: np.ndarray
) -> np.ndarray:
    return day_start + local_seconds - offset_h * 3600


def simulate_legit(
    rng: np.random.Generator, dims: Dimensions, cfg: DataConfig, win: ChunkWindow, n_target: int
) -> pd.DataFrame:
    """Simulate ``~n_target`` legitimate transactions inside ``win``."""
    cust_df = dims.customers
    merch_df = dims.merchants
    nc = len(cust_df)

    # --- day allocation with weekday seasonality ---
    day_starts = win.start_ts + DAY * np.arange(win.days)
    dow = ((day_starts // DAY) + 3) % 7  # 1970-01-01 was a Thursday -> Monday=0
    w = DOW_WEIGHTS[dow] * rng.normal(1.0, 0.03, win.days)
    n_per_day = rng.multinomial(n_target, w / w.sum())
    day_idx = np.repeat(np.arange(win.days), n_per_day)
    n = len(day_idx)

    act = cust_df["_activity"].to_numpy()
    cust = rng.choice(nc, size=n, p=act / act.sum())
    offs = dims.card_offsets
    card = offs[cust] + (rng.random(n) * (offs[cust + 1] - offs[cust])).astype(np.int64)

    # --- channel ---
    online_share = cust_df["_online_share"].to_numpy()[cust]
    u = rng.random(n)
    channel = np.where(u < 0.07, "ATM", np.where(u < 0.07 + online_share * 0.93, "ONLINE", "POS"))

    # --- travel: some travellers spend part of this chunk in another city ---
    home_city = cust_df["_city_idx"].to_numpy()
    traveler = cust_df["_traveler"].to_numpy()
    on_trip = traveler & (rng.random(nc) < 0.35)
    trip_city = np.where(on_trip, rng.integers(0, len(CITIES) - len(FRAUD_CITY_IDX), nc), -1)
    trip_start = rng.integers(0, max(win.days - 1, 1), nc)
    trip_len = rng.integers(2, 6, nc)
    in_trip = (
        (trip_city[cust] >= 0)
        & (day_idx >= trip_start[cust])
        & (day_idx < trip_start[cust] + trip_len[cust])
    )
    cur_city = np.where(in_trip, trip_city[cust], home_city[cust])

    # --- merchant choice ---
    merch = np.empty(n, dtype=np.int64)
    pop = merch_df["_popularity"].to_numpy()
    fav = dims.favorites
    is_pos = channel == "POS"
    is_onl = channel == "ONLINE"
    is_atm = channel == "ATM"
    pos_fav = is_pos & ~in_trip & (rng.random(n) < 0.82)
    merch[pos_fav] = fav[cust[pos_fav], rng.integers(0, N_FAVORITES - 6, pos_fav.sum())]
    pos_rand = is_pos & ~pos_fav
    onl_fav = is_onl & (rng.random(n) < 0.85)
    merch[onl_fav] = fav[cust[onl_fav], rng.integers(N_FAVORITES - 6, N_FAVORITES, onl_fav.sum())]
    onl_rand = is_onl & ~onl_fav
    om = dims.online_merchants
    merch[onl_rand] = rng.choice(om, size=onl_rand.sum(), p=pop[om] / pop[om].sum())
    for ci in np.unique(cur_city):
        sel = pos_rand & (cur_city == ci)
        if sel.any():
            pool = dims.city_merchants[int(ci)]
            merch[sel] = rng.choice(pool, size=sel.sum(), p=pop[pool] / pop[pool].sum())
        sel = is_atm & (cur_city == ci)
        if sel.any():
            merch[sel] = rng.choice(dims.city_atms[int(ci)], size=sel.sum())

    mcc = merch_df["mcc"].to_numpy()[merch]
    amount = _lognormal_amount(rng, mcc, cust_df["_spend_mult"].to_numpy()[cust])
    amount = np.where(is_atm, np.round(np.clip(amount, 20, 800) / 20) * 20, amount)
    # small legitimate purchases (parking, vending, transit top-ups)
    tiny = rng.random(n) < 0.01
    amount = np.where(tiny, np.round(rng.uniform(0.5, 4.99, n), 2), amount)

    # --- time of day (local) -> UTC ---
    night = cust_df["_night_owl"].to_numpy()[cust]
    hp_day = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()
    hp_night = NIGHT_WEIGHTS / NIGHT_WEIGHTS.sum()
    hour = np.where(night, rng.choice(24, n, p=hp_night), rng.choice(24, n, p=hp_day))
    local_sec = hour * 3600 + rng.integers(0, 3600, n)
    loc_city = np.where(is_onl, home_city[cust], merch_df["_city_idx"].to_numpy()[merch])
    event_ts = _local_to_utc(day_starts[day_idx], local_sec, CITY_OFFSET[loc_city])

    # --- device / ip / geo ---
    dev_choice = rng.choice(3, size=n, p=[0.72, 0.2, 0.08])
    device = np.where(is_onl, dims.customer_devices[cust, dev_choice], None)
    new_dev = is_onl & (rng.random(n) < 0.012)
    device[new_dev] = _new_devices(rng, int(new_dev.sum()))
    ip = np.where(is_onl, _home_ip(cust), None)
    mobile_ip = is_onl & (rng.random(n) < 0.05)
    ip[mobile_ip] = _random_ip(rng, CITY_COUNTRY[home_city[cust[mobile_ip]]])

    m_lat = merch_df["latitude"].to_numpy()[merch]
    m_lon = merch_df["longitude"].to_numpy()[merch]
    lat = np.where(is_onl, CITY_LAT[home_city[cust]] + rng.normal(0, 0.08, n), m_lat)
    lon = np.where(is_onl, CITY_LON[home_city[cust]] + rng.normal(0, 0.08, n), m_lon)
    city = np.where(is_onl, CITY_NAME[home_city[cust]], merch_df["city"].to_numpy()[merch])
    country = np.where(is_onl, CITY_COUNTRY[home_city[cust]], merch_df["country"].to_numpy()[merch])

    card_present_mode = rng.choice(["CHIP", "CONTACTLESS", "SWIPE"], n, p=[0.55, 0.37, 0.08])
    entry = np.where(is_onl, "ECOM", np.where(is_atm, "CHIP", card_present_mode))
    status = np.where(rng.random(n) < 0.985, "APPROVED", "DECLINED")

    return pd.DataFrame(
        {
            "event_ts": event_ts.astype(np.int64),
            "cust": cust,
            "card": card,
            "merch": merch,
            "channel": channel,
            "entry_mode": entry,
            "amount_usd": amount,
            "device_id": device,
            "ip_address": ip,
            "latitude": np.round(lat, 5),
            "longitude": np.round(lon, 5),
            "city": city,
            "country": country,
            "auth_status": status,
            "is_fraud": np.zeros(n, dtype=np.int8),
            "fraud_type": None,
        }
    )


# --------------------------------------------------------------------------------------------
# Fraud episodes
# --------------------------------------------------------------------------------------------
class _FraudBuilder:
    def __init__(self, rng: np.random.Generator, dims: Dimensions, win: ChunkWindow) -> None:
        self.rng = rng
        self.dims = dims
        self.win = win
        self.rows: list[dict[str, object]] = []
        self.ncards = len(dims.cards)
        self.card_owner = dims.cards["customer_id"].str[1:].astype(int).to_numpy() - 1

    def _victim(self) -> tuple[int, int]:
        card = int(self.rng.integers(0, self.ncards))
        return card, int(self.card_owner[card])

    def _start(self, margin: int = 0) -> int:
        return int(self.rng.integers(self.win.start_ts, self.win.end_ts - margin))

    def _merchant(self, mccs: tuple[str, ...], city: int | None = None) -> int:
        pools = [self.dims.mcc_merchants[m] for m in mccs if self.dims.mcc_merchants[m].size]
        pool = np.concatenate(pools)
        if city is not None:
            mc = self.dims.merchants["_city_idx"].to_numpy()[pool]
            local = pool[mc == city]
            if local.size:
                pool = local
        return int(self.rng.choice(pool))

    def _add(
        self,
        *,
        ts: int,
        cust: int,
        card: int,
        merch: int,
        channel: str,
        entry: str,
        amount: float,
        device: str | None,
        ip: str | None,
        city_idx: int,
        declined_p: float,
        ftype: str,
    ) -> None:
        r = self.rng
        self.rows.append(
            {
                "event_ts": ts,
                "cust": cust,
                "card": card,
                "merch": merch,
                "channel": channel,
                "entry_mode": entry,
                "amount_usd": round(float(amount), 2),
                "device_id": device,
                "ip_address": ip,
                "latitude": round(float(CITY_LAT[city_idx] + r.normal(0, 0.05)), 5),
                "longitude": round(float(CITY_LON[city_idx] + r.normal(0, 0.05)), 5),
                "city": CITY_NAME[city_idx],
                "country": CITY_COUNTRY[city_idx],
                "auth_status": "DECLINED" if r.random() < declined_p else "APPROVED",
                "is_fraud": 1,
                "fraud_type": ftype,
            }
        )

    def _fraud_origin(self) -> tuple[int, str, str]:
        ci = int(self.rng.choice(FRAUD_CITY_IDX))
        dev = str(_new_devices(self.rng, 1)[0])
        ip = str(_random_ip(self.rng, np.array([CITY_COUNTRY[ci]]))[0])
        return ci, dev, ip

    def card_testing(self) -> None:
        r = self.rng
        card, cust = self._victim()
        ci, dev, ip = self._fraud_origin()
        t = self._start()
        merchants = [
            self._merchant(("5815", "5999", "5967", "5964")) for _ in range(r.integers(1, 4))
        ]
        for _ in range(int(r.integers(5, 21))):
            t += int(r.integers(20, 180))
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=int(r.choice(merchants)),
                channel="ONLINE",
                entry="ECOM",
                amount=r.uniform(0.5, 4.99),
                device=dev,
                ip=ip,
                city_idx=ci,
                declined_p=0.35,
                ftype="card_testing",
            )
        if r.random() < 0.7:  # cash-out after successful probes
            for _ in range(int(r.integers(1, 3))):
                t += int(r.integers(1800, 7200))
                self._add(
                    ts=t,
                    cust=cust,
                    card=card,
                    merch=self._merchant(("5732", "5964", "5944")),
                    channel="ONLINE",
                    entry="ECOM",
                    amount=r.uniform(250, 1500),
                    device=dev,
                    ip=ip,
                    city_idx=ci,
                    declined_p=0.1,
                    ftype="card_testing",
                )

    def account_takeover(self) -> None:
        r = self.rng
        card, cust = self._victim()
        ci, dev, ip = self._fraud_origin()
        t = self._start()
        for _ in range(int(r.integers(2, 7))):
            t += int(r.integers(600, 4 * 3600))
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=self._merchant(("5732", "5944", "4829", "6051", "5964")),
                channel="ONLINE",
                entry="ECOM",
                amount=r.lognormal(np.log(600), 0.6),
                device=dev,
                ip=ip,
                city_idx=ci,
                declined_p=0.1,
                ftype="account_takeover",
            )

    def velocity_burst(self) -> None:
        r = self.rng
        card, cust = self._victim()
        home = int(self.dims.customers["_city_idx"].iat[cust])
        ci = int(r.choice(US_CITY_IDX[home != US_CITY_IDX]))
        t = self._start()
        for _ in range(int(r.integers(6, 16))):
            t += int(r.integers(120, 900))
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=self._merchant(("5541", "5732", "5311", "5944", "5999"), city=ci),
                channel="POS",
                entry="SWIPE",
                amount=r.lognormal(np.log(150), 0.8),
                device=None,
                ip=None,
                city_idx=ci,
                declined_p=0.08,
                ftype="velocity_burst",
            )

    def high_amount_night(self) -> None:
        r = self.rng
        card, cust = self._victim()
        c = self.dims.customers
        home = int(c["_city_idx"].iat[cust])
        day0 = self.win.start_ts + DAY * int(r.integers(0, self.win.days))
        t = int(day0 + int(r.integers(0, 4 * 3600)) - CITY_OFFSET[home] * 3600)
        online = r.random() < 0.5
        amount = 60 * float(c["_spend_mult"].iat[cust]) * r.uniform(8, 30)
        if online:
            dev = (
                str(_new_devices(r, 1)[0])
                if r.random() < 0.5
                else str(self.dims.customer_devices[cust, 0])
            )
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=self._merchant(("5964", "4511", "5944")),
                channel="ONLINE",
                entry="ECOM",
                amount=amount,
                device=dev,
                ip=str(_home_ip(np.array([cust]))[0]),
                city_idx=home,
                declined_p=0.05,
                ftype="high_amount_night",
            )
        else:
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=self._merchant(("5944", "5732", "7011"), city=home),
                channel="POS",
                entry=str(r.choice(["CHIP", "SWIPE"])),
                amount=amount,
                device=None,
                ip=None,
                city_idx=home,
                declined_p=0.05,
                ftype="high_amount_night",
            )

    def high_risk_mcc(self) -> None:
        r = self.rng
        card, cust = self._victim()
        home = int(self.dims.customers["_city_idx"].iat[cust])
        foreign = r.random() < 0.5
        if foreign:
            ci, dev, ip = self._fraud_origin()
        else:
            ci = home
            dev = (
                str(_new_devices(r, 1)[0])
                if r.random() < 0.6
                else str(self.dims.customer_devices[cust, 0])
            )
            ip = str(_home_ip(np.array([cust]))[0])
        t = self._start()
        for _ in range(int(r.integers(1, 4))):
            t += int(r.integers(300, 3 * 3600))
            self._add(
                ts=t,
                cust=cust,
                card=card,
                merch=self._merchant(HIGH_RISK_MCCS),
                channel="ONLINE",
                entry="ECOM",
                amount=r.lognormal(np.log(350), 0.7),
                device=dev,
                ip=ip,
                city_idx=ci,
                declined_p=0.15,
                ftype="high_risk_mcc",
            )


def simulate_fraud(
    rng: np.random.Generator, dims: Dimensions, cfg: DataConfig, win: ChunkWindow, n_legit: int
) -> pd.DataFrame:
    fb = _FraudBuilder(rng, dims, win)
    scale = n_legit / 1_000_000
    fc = cfg.fraud
    plan = [
        (fb.card_testing, fc.card_testing_per_m),
        (fb.account_takeover, fc.account_takeover_per_m),
        (fb.velocity_burst, fc.velocity_burst_per_m),
        (fb.high_amount_night, fc.high_amount_night_per_m),
        (fb.high_risk_mcc, fc.high_risk_mcc_per_m),
    ]
    for fn, per_m in plan:
        for _ in range(int(rng.poisson(per_m * scale))):
            fn()
    if not fb.rows:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(fb.rows, columns=COLUMNS)
    return df[(df["event_ts"] >= win.start_ts) & (df["event_ts"] < win.end_ts)]


def finalize(rng: np.random.Generator, dims: Dimensions, df: pd.DataFrame) -> pd.DataFrame:
    """Attach ids, currency, local amount, auth codes; sort by time."""
    df = df.astype(
        {
            "event_ts": np.int64,
            "cust": np.int64,
            "card": np.int64,
            "merch": np.int64,
            "is_fraud": np.int8,
            "amount_usd": float,
        }
    )
    df = df.sort_values("event_ts", kind="stable").reset_index(drop=True)
    n = len(df)
    m = dims.merchants
    c = dims.customers
    raw = rng.integers(0, 256, size=(n, 8), dtype=np.uint8)
    df.insert(0, "transaction_id", ["tx_" + r.tobytes().hex() for r in raw])
    df["customer_id"] = c["customer_id"].to_numpy()[df["cust"].to_numpy()]
    df["card_number"] = dims.cards["card_number"].to_numpy()[df["card"].to_numpy()]
    df["merchant_id"] = m["merchant_id"].to_numpy()[df["merch"].to_numpy()]
    df["merchant_name"] = m["merchant_name"].to_numpy()[df["merch"].to_numpy()]
    df["mcc"] = m["mcc"].to_numpy()[df["merch"].to_numpy()]
    home_cc = c["home_country"].to_numpy()[df["cust"].to_numpy()]
    cc = np.where(
        df["channel"].to_numpy() == "ONLINE",
        home_cc,
        m["country"].to_numpy()[df["merch"].to_numpy()],
    )
    df["currency"] = [COUNTRY_CURRENCY.get(str(x), "USD") for x in cc]
    fx = df["currency"].map(FX_PER_USD).to_numpy()
    df["amount"] = np.round(df["amount_usd"].to_numpy() * fx, 2)
    alnum = np.array(list("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"))
    codes = alnum[rng.integers(0, len(alnum), size=(n, 6))]
    df["auth_code"] = np.where(
        df["auth_status"].to_numpy() == "APPROVED", ["".join(r) for r in codes], None
    )
    return df
