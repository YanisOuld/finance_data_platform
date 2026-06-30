"""
Any important information will be stored there
"""

from enum import Enum

# Used when a ticker has no ingestion_watermark row yet (first-ever backfill).
DEFAULT_BACKFILL_START = "2015-01-01"

# Canonical mapping: our internal series code -> FRED series_id.
# This is the single source of truth (the old duplicate table that used to
# live in src/ingestion/clients/fred_client.py has been removed).
FRED_COLUMN_SERIES = {
    "interest_rate_10y": "GS10",
    "inflation": "CPIAUCNS",
    "cpi": "CPIAUCSL",
    "gdp": "GDP",
    "unemployment": "UNRATE",
    "yield_curve": "T10Y2Y",
    "fed_funds": "FEDFUNDS",
    "usd/eur": "DEXUSEU",
    "usd/cad": "DEXCAUS",
    "usd/jpy": "DEXJPUS",
    "usd/gbp": "DEXUSUK",
}

BASE_URL_FIGI = "https://api.openfigi.com"


class SecEdgarRequestType(Enum):
    SUBMISSIONS = "submissions"
    COMPANY_FACTS = "companyfacts"
