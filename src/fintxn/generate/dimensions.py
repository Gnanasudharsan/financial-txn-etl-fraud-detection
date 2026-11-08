"""Customer, card and merchant dimensions (seeded, reproducible, vectorised)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from fintxn.config import DataConfig
from fintxn.generate.catalog import CARD_NETWORKS, CITIES, CITY_UTC_OFFSET, MCCS

N_FAVORITES = 24


@dataclass
class Dimensions:
    customers: pd.DataFrame
    cards: pd.DataFrame
    merchants: pd.DataFrame
    # Fast lookup structures used by the transaction simulator (not persisted)
    card_offsets: np.ndarray  # CSR offsets into cards (sorted by customer)
    favorites: np.ndarray  # (n_customers, N_FAVORITES) merchant row indices
    customer_devices: np.ndarray  # (n_customers, 3) device ids
    city_merchants: dict[int, np.ndarray]
    city_atms: dict[int, np.ndarray]
    online_merchants: np.ndarray
    mcc_merchants: dict[str, np.ndarray]


def luhn_check_digit(partial: np.ndarray) -> np.ndarray:
    """Vectorised Luhn check digit for rows of digits (shape: n x k)."""
    digits = partial[:, ::-1].copy()
    doubled = digits[:, 0::2] * 2
    doubled = np.where(doubled > 9, doubled - 9, doubled)
    total = doubled.sum(axis=1) + digits[:, 1::2].sum(axis=1)
    return np.asarray((10 - total % 10) % 10)


def luhn_valid(pan: str) -> bool:
    nums = [int(c) for c in pan][::-1]
    s = sum(nums[0::2]) + sum(sum(divmod(2 * d, 10)) for d in nums[1::2])
    return s % 10 == 0


def make_pans(rng: np.random.Generator, networks: np.ndarray) -> np.ndarray:
    n = len(networks)
    out = np.empty(n, dtype=object)
    for net, (prefix, _) in CARD_NETWORKS.items():
        idx = np.flatnonzero(networks == net)
        if idx.size == 0:
            continue
        length = 15 if net == "AMEX" else 16
        body_len = length - len(prefix) - 1
        pre = np.tile(np.array([int(c) for c in prefix]), (idx.size, 1))
        body = rng.integers(0, 10, size=(idx.size, body_len))
        partial = np.hstack([pre, body])
        check = luhn_check_digit(partial)
        full = np.hstack([partial, check[:, None]])
        out[idx] = ["".join(map(str, row)) for row in full]
    return out


def _hex_ids(rng: np.random.Generator, n: int, prefix: str, nbytes: int = 6) -> np.ndarray:
    raw = rng.integers(0, 256, size=(n, nbytes), dtype=np.uint8)
    return np.array([prefix + r.tobytes().hex() for r in raw], dtype=object)


def build_dimensions(cfg: DataConfig) -> Dimensions:
    rng = np.random.default_rng([cfg.seed, 0])
    fake = Faker("en_US")
    Faker.seed(cfg.seed)

    city_w = np.array([c.weight for c in CITIES])
    city_p = city_w / city_w.sum()

    # ---------------- customers ----------------
    nc = cfg.n_customers
    first_pool = np.array([fake.first_name() for _ in range(1500)], dtype=object)
    last_pool = np.array([fake.last_name() for _ in range(2500)], dtype=object)
    first = first_pool[rng.integers(0, len(first_pool), nc)]
    last = last_pool[rng.integers(0, len(last_pool), nc)]
    domains = np.array(["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me"])
    email = [
        f"{f.lower()}.{lname.lower()}{n}@{d}"
        for f, lname, n, d in zip(
            first,
            last,
            rng.integers(1, 999, nc),
            domains[rng.integers(0, len(domains), nc)],
            strict=True,
        )
    ]
    phone = [
        f"+1-{a}-{b:03d}-{c:04d}"
        for a, b, c in zip(
            rng.integers(201, 989, nc),
            rng.integers(0, 1000, nc),
            rng.integers(0, 10000, nc),
            strict=True,
        )
    ]
    home_city = rng.choice(len(CITIES), size=nc, p=city_p)
    signup_offsets = rng.integers(30, 3650, nc)
    customers = pd.DataFrame(
        {
            "customer_id": [f"C{i:07d}" for i in range(1, nc + 1)],
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": phone,
            "date_of_birth": [
                (date(2006, 1, 1) - timedelta(days=int(d))).isoformat()
                for d in rng.integers(0, 365 * 60, nc)
            ],
            "home_city": [CITIES[i].name for i in home_city],
            "home_country": [CITIES[i].country for i in home_city],
            "home_utc_offset": [CITY_UTC_OFFSET[CITIES[i].name] for i in home_city],
            "home_lat": np.round([CITIES[i].lat for i in home_city], 4),
            "home_lon": np.round([CITIES[i].lon for i in home_city], 4),
            "signup_date": [
                (cfg.start_date - timedelta(days=int(d))).isoformat() for d in signup_offsets
            ],
            "segment": rng.choice(["retail", "premium", "business"], nc, p=[0.78, 0.17, 0.05]),
        }
    )
    # Behavioural traits (kept on the dataframe; not exported as dimension columns)
    customers["_city_idx"] = home_city
    customers["_activity"] = rng.lognormal(0.0, 0.7, nc)
    customers["_spend_mult"] = rng.lognormal(0.0, 0.35, nc) * np.where(
        customers["segment"] == "premium", 1.8, 1.0
    )
    customers["_night_owl"] = rng.random(nc) < 0.07
    customers["_traveler"] = rng.random(nc) < 0.08
    customers["_gambler"] = rng.random(nc) < 0.03
    customers["_online_share"] = np.clip(rng.normal(0.30, 0.12, nc), 0.03, 0.85)

    # ---------------- cards ----------------
    ncard = max(cfg.n_cards, nc)
    extra = ncard - nc
    owner = np.concatenate(
        [
            np.arange(nc),
            rng.choice(nc, size=extra, p=customers["_activity"] / customers["_activity"].sum()),
        ]
    )
    owner.sort()
    net_names = np.array(list(CARD_NETWORKS))
    net_p = np.array([v[1] for v in CARD_NETWORKS.values()])
    networks = rng.choice(net_names, size=ncard, p=net_p / net_p.sum())
    issue = [
        (cfg.start_date - timedelta(days=int(d))).isoformat() for d in rng.integers(10, 1400, ncard)
    ]
    cards = pd.DataFrame(
        {
            "card_id": [f"K{i:08d}" for i in range(1, ncard + 1)],
            "customer_id": customers["customer_id"].to_numpy()[owner],
            "card_number": make_pans(rng, networks),
            "network": networks,
            "card_type": rng.choice(["debit", "credit"], ncard, p=[0.45, 0.55]),
            "issue_date": issue,
            "expiry": [
                f"{m:02d}/{y}"
                for m, y in zip(
                    rng.integers(1, 13, ncard), rng.integers(2027, 2032, ncard), strict=True
                )
            ],
            "credit_limit": np.round(rng.choice([1000, 2500, 5000, 10000, 25000], ncard), 0),
            "status": rng.choice(
                ["active", "active", "active", "frozen"], ncard, p=[0.33, 0.33, 0.33, 0.01]
            ),
        }
    )
    counts = np.bincount(owner, minlength=nc)
    card_offsets = np.concatenate([[0], np.cumsum(counts)])

    # ---------------- merchants ----------------
    nm = cfg.n_merchants
    mcc_codes = np.array([m.code for m in MCCS])
    mcc_share = np.array([max(m.share, 0.0) for m in MCCS])
    is_atm_mcc = mcc_codes == "6011"
    n_atm = max(int(nm * 0.06), len(CITIES))
    n_reg = nm - n_atm
    reg_p = np.where(is_atm_mcc, 0.0, mcc_share + 0.15)
    merchant_mcc = np.concatenate(
        [rng.choice(mcc_codes, size=n_reg, p=reg_p / reg_p.sum()), np.full(n_atm, "6011")]
    )
    mcc_online = {m.code: m.online for m in MCCS}
    online_only = np.array([mcc_online[m] for m in merchant_mcc]) & (rng.random(nm) < 0.85)
    m_city = rng.choice(len(CITIES), size=nm, p=city_p)
    m_city[n_reg:] = np.resize(np.arange(len(CITIES)), n_atm)  # at least one ATM per city
    company = np.array([fake.company() for _ in range(3000)], dtype=object)
    mdesc = {m.code: m.description for m in MCCS}
    mcat = {m.code: m.category for m in MCCS}
    names = np.where(
        merchant_mcc == "6011",
        "ATM " + rng.integers(1000, 9999, nm).astype(str),
        company[rng.integers(0, len(company), nm)],
    )
    merchants = pd.DataFrame(
        {
            "merchant_id": [f"M{i:06d}" for i in range(1, nm + 1)],
            "merchant_name": names,
            "mcc": merchant_mcc,
            "mcc_description": [mdesc[m] for m in merchant_mcc],
            "category": [mcat[m] for m in merchant_mcc],
            "city": [CITIES[i].name for i in m_city],
            "country": [CITIES[i].country for i in m_city],
            "latitude": np.round([CITIES[i].lat for i in m_city] + rng.normal(0, 0.05, nm), 5),
            "longitude": np.round([CITIES[i].lon for i in m_city] + rng.normal(0, 0.05, nm), 5),
            "online_only": online_only,
            "acquirer_id": rng.choice(["ACQ01", "ACQ02", "ACQ03", "ACQ04"], nm),
            "onboarded_date": [
                (cfg.start_date - timedelta(days=int(d))).isoformat()
                for d in rng.integers(30, 3000, nm)
            ],
        }
    )
    merchants["_city_idx"] = m_city
    merchants["_popularity"] = rng.pareto(1.6, nm) + 1.0

    # ---------------- lookup structures ----------------
    pop = merchants["_popularity"].to_numpy()
    is_atm = merchant_mcc == "6011"
    is_high_risk = np.isin(merchant_mcc, [m.code for m in MCCS if m.high_risk])
    city_merchants: dict[int, np.ndarray] = {}
    city_atms: dict[int, np.ndarray] = {}
    for ci in range(len(CITIES)):
        sel = np.flatnonzero((m_city == ci) & ~online_only & ~is_atm & ~is_high_risk)
        city_merchants[ci] = sel if sel.size else np.flatnonzero(~online_only & ~is_atm)
        atm = np.flatnonzero((m_city == ci) & is_atm)
        city_atms[ci] = atm if atm.size else np.flatnonzero(is_atm)
    online_merchants = np.flatnonzero(online_only & ~is_high_risk)
    mcc_merchants = {code: np.flatnonzero(merchant_mcc == code) for code in mcc_codes}

    favorites = np.empty((nc, N_FAVORITES), dtype=np.int64)
    n_online_fav = 6
    for ci in range(len(CITIES)):
        cust_idx = np.flatnonzero(home_city == ci)
        if cust_idx.size == 0:
            continue
        pool = city_merchants[ci]
        p = pop[pool] / pop[pool].sum()
        favorites[cust_idx, : N_FAVORITES - n_online_fav] = rng.choice(
            pool, size=(cust_idx.size, N_FAVORITES - n_online_fav), p=p
        )
    po = pop[online_merchants] / pop[online_merchants].sum()
    favorites[:, N_FAVORITES - n_online_fav :] = rng.choice(
        online_merchants, size=(nc, n_online_fav), p=po
    )
    gamblers = np.flatnonzero(customers["_gambler"].to_numpy())
    if gamblers.size:
        favorites[gamblers, -1] = rng.choice(mcc_merchants["7995"], size=gamblers.size)

    customer_devices = np.array(
        [f"dev_{h}" for h in _hex_ids(rng, nc * 3, "", 5)], dtype=object
    ).reshape(nc, 3)

    return Dimensions(
        customers=customers,
        cards=cards,
        merchants=merchants,
        card_offsets=card_offsets,
        favorites=favorites,
        customer_devices=customer_devices,
        city_merchants=city_merchants,
        city_atms=city_atms,
        online_merchants=online_merchants,
        mcc_merchants=mcc_merchants,
    )


def public_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop simulator-internal columns (prefixed with ``_``)."""
    return df[[c for c in df.columns if not c.startswith("_")]]
