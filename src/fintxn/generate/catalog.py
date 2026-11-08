"""Static reference data for the simulator: cities, currencies, MCC codes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    country: str
    lat: float
    lon: float
    weight: float  # share of customers / merchants


CITIES: tuple[City, ...] = (
    City("New York", "US", 40.7128, -74.0060, 9.0),
    City("Los Angeles", "US", 34.0522, -118.2437, 6.5),
    City("Chicago", "US", 41.8781, -87.6298, 5.0),
    City("Houston", "US", 29.7604, -95.3698, 4.5),
    City("Phoenix", "US", 33.4484, -112.0740, 3.2),
    City("Philadelphia", "US", 39.9526, -75.1652, 3.0),
    City("San Antonio", "US", 29.4241, -98.4936, 2.6),
    City("San Diego", "US", 32.7157, -117.1611, 2.6),
    City("Dallas", "US", 32.7767, -96.7970, 2.8),
    City("San Jose", "US", 37.3382, -121.8863, 2.0),
    City("Austin", "US", 30.2672, -97.7431, 2.2),
    City("Seattle", "US", 47.6062, -122.3321, 2.6),
    City("Denver", "US", 39.7392, -104.9903, 2.2),
    City("Boston", "US", 42.3601, -71.0589, 2.6),
    City("Miami", "US", 25.7617, -80.1918, 2.6),
    City("Atlanta", "US", 33.7490, -84.3880, 2.6),
    City("San Francisco", "US", 37.7749, -122.4194, 2.4),
    City("Minneapolis", "US", 44.9778, -93.2650, 1.6),
    City("Portland", "US", 45.5152, -122.6784, 1.5),
    City("Las Vegas", "US", 36.1699, -115.1398, 1.6),
    City("Detroit", "US", 42.3314, -83.0458, 1.5),
    City("Nashville", "US", 36.1627, -86.7816, 1.4),
    City("Charlotte", "US", 35.2271, -80.8431, 1.4),
    City("Columbus", "US", 39.9612, -82.9988, 1.3),
    City("Toronto", "CA", 43.6532, -79.3832, 1.8),
    City("Vancouver", "CA", 49.2827, -123.1207, 1.0),
    City("London", "GB", 51.5074, -0.1278, 2.0),
    City("Manchester", "GB", 53.4808, -2.2426, 0.6),
    City("Paris", "FR", 48.8566, 2.3522, 1.0),
    City("Berlin", "DE", 52.5200, 13.4050, 0.9),
    City("Madrid", "ES", 40.4168, -3.7038, 0.6),
    City("Amsterdam", "NL", 52.3676, 4.9041, 0.5),
    City("Mumbai", "IN", 19.0760, 72.8777, 0.8),
    City("Bengaluru", "IN", 12.9716, 77.5946, 0.7),
    City("Tokyo", "JP", 35.6762, 139.6503, 0.6),
    City("Singapore", "SG", 1.3521, 103.8198, 0.5),
    City("Sydney", "AU", -33.8688, 151.2093, 0.5),
    City("Mexico City", "MX", 19.4326, -99.1332, 0.6),
    # Cities that appear mostly as fraud origins (low legitimate weight)
    City("Lagos", "NG", 6.5244, 3.3792, 0.05),
    City("Bucharest", "RO", 44.4268, 26.1025, 0.05),
    City("Manila", "PH", 14.5995, 120.9842, 0.05),
    City("Kyiv", "UA", 50.4501, 30.5234, 0.05),
)

FRAUD_ORIGIN_COUNTRIES = ("NG", "RO", "PH", "UA", "MX", "IN")

# Standard-time UTC offsets in hours (DST ignored for the simulation).
CITY_UTC_OFFSET: dict[str, int] = {
    "New York": -5, "Philadelphia": -5, "Boston": -5, "Miami": -5, "Atlanta": -5,
    "Detroit": -5, "Charlotte": -5, "Columbus": -5, "Toronto": -5,
    "Chicago": -6, "Houston": -6, "San Antonio": -6, "Dallas": -6, "Austin": -6,
    "Minneapolis": -6, "Nashville": -6, "Mexico City": -6,
    "Phoenix": -7, "Denver": -7,
    "Los Angeles": -8, "San Diego": -8, "San Jose": -8, "Seattle": -8, "San Francisco": -8,
    "Portland": -8, "Las Vegas": -8, "Vancouver": -8,
    "London": 0, "Manchester": 0, "Lagos": 1, "Paris": 1, "Berlin": 1, "Madrid": 1,
    "Amsterdam": 1, "Bucharest": 2, "Kyiv": 2, "Mumbai": 5, "Bengaluru": 5,
    "Singapore": 8, "Manila": 8, "Tokyo": 9, "Sydney": 10,
}  # fmt: skip

COUNTRY_CURRENCY: dict[str, str] = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "FR": "EUR",
    "DE": "EUR",
    "ES": "EUR",
    "NL": "EUR",
    "IN": "INR",
    "JP": "JPY",
    "SG": "SGD",
    "AU": "AUD",
    "MX": "MXN",
    "NG": "NGN",
    "RO": "RON",
    "PH": "PHP",
    "UA": "UAH",
}

# Units of currency per 1 USD (fixed reference rates for the simulation window).
FX_PER_USD: dict[str, float] = {
    "USD": 1.0,
    "CAD": 1.37,
    "GBP": 0.79,
    "EUR": 0.92,
    "INR": 83.5,
    "JPY": 151.0,
    "SGD": 1.35,
    "AUD": 1.52,
    "MXN": 17.1,
    "NGN": 1450.0,
    "RON": 4.6,
    "PHP": 56.5,
    "UAH": 39.5,
}

VALID_CURRENCIES: frozenset[str] = frozenset(FX_PER_USD)


@dataclass(frozen=True)
class Mcc:
    code: str
    description: str
    category: str
    median_usd: float
    sigma: float
    share: float  # share of legitimate card-present/online volume
    high_risk: bool = False
    online: bool = False


MCCS: tuple[Mcc, ...] = (
    Mcc("5411", "Grocery Stores, Supermarkets", "grocery", 48.0, 0.75, 16.0),
    Mcc("5812", "Eating Places, Restaurants", "dining", 38.0, 0.65, 11.0),
    Mcc("5814", "Fast Food Restaurants", "dining", 13.0, 0.55, 9.0),
    Mcc("5541", "Service Stations", "fuel", 42.0, 0.45, 7.0),
    Mcc("5311", "Department Stores", "retail", 70.0, 0.85, 4.0),
    Mcc("5912", "Drug Stores, Pharmacies", "health", 24.0, 0.70, 4.0),
    Mcc("5732", "Electronics Stores", "electronics", 140.0, 1.00, 2.0),
    Mcc("5999", "Miscellaneous Retail", "retail", 35.0, 0.90, 4.0),
    Mcc("4111", "Local Commuter Transport", "transport", 4.5, 0.45, 4.0),
    Mcc("4121", "Taxicabs, Rideshare", "transport", 22.0, 0.55, 4.0),
    Mcc("4511", "Airlines", "travel", 380.0, 0.60, 1.0, online=True),
    Mcc("7011", "Hotels, Motels, Resorts", "travel", 210.0, 0.60, 1.2),
    Mcc("5651", "Family Clothing Stores", "retail", 65.0, 0.75, 3.0),
    Mcc("5942", "Book Stores", "retail", 22.0, 0.55, 0.8),
    Mcc("5815", "Digital Goods: Media", "digital", 11.0, 0.65, 4.0, online=True),
    Mcc("4899", "Cable, Streaming Services", "digital", 16.0, 0.35, 3.0, online=True),
    Mcc("4900", "Utilities", "bills", 110.0, 0.50, 2.0, online=True),
    Mcc("5964", "Direct Marketing: Catalog Merchant", "ecommerce", 55.0, 0.90, 8.0, online=True),
    Mcc("8062", "Hospitals", "health", 180.0, 1.00, 0.6),
    Mcc("5944", "Jewelry Stores", "luxury", 260.0, 0.90, 0.4),
    Mcc("6011", "ATM Cash Disbursement", "cash", 120.0, 0.60, 0.0),
    Mcc("5993", "Cigar Stores", "retail", 15.0, 0.50, 0.5),
    Mcc("7995", "Betting, Casino Gambling", "gambling", 60.0, 1.00, 0.4, True, True),
    Mcc("4829", "Wire Transfer, Money Orders", "money_transfer", 220.0, 0.90, 0.3, True, True),
    Mcc("6051", "Quasi-Cash, Crypto", "quasi_cash", 180.0, 1.00, 0.3, True, True),
    Mcc(
        "5967",
        "Direct Marketing: Inbound Teleservices",
        "teleservices",
        40.0,
        0.80,
        0.2,
        True,
        True,
    ),
    Mcc("7273", "Dating, Escort Services", "dating", 35.0, 0.70, 0.1, True, True),
)

MCC_BY_CODE: dict[str, Mcc] = {m.code: m for m in MCCS}
HIGH_RISK_MCCS: tuple[str, ...] = tuple(m.code for m in MCCS if m.high_risk)

CARD_NETWORKS: dict[str, tuple[str, float]] = {
    # network: (BIN prefix, share)
    "VISA": ("4", 0.52),
    "MASTERCARD": ("5", 0.33),
    "AMEX": ("37", 0.09),
    "DISCOVER": ("6011", 0.06),
}
