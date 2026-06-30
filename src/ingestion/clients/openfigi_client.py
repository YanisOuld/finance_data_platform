import os

import requests
from dotenv import load_dotenv

from src.ingestion.writers.write_bronze import write_bronze_to_s3

"""
OpenFIGI /v3/mapping job: maps a ticker to its FIGI(s).

We deliberately do NOT filter by exchange (micCode/exchCode): this codebase
has no ticker->exchange mapping source of its own (that's the chicken-and-egg
problem OpenFIGI is meant to solve), so the previous version of this client
tried to look one up via an unimplemented `_find_min_code()` stub and always
crashed (the function had no body, plus "minCode" isn't even a valid OpenFIGI
field -- it's "micCode"/"exchCode"). Querying by TICKER + marketSecDes="Equity"
alone returns every matching listing across exchanges; an ambiguous ticker
will yield multiple FIGIs, and disambiguating against
universe_instruments.exchange/currency is left to a later identifiers/mapping
silver step.
"""


load_dotenv()

API_KEY = os.getenv("OPENFIGI_API_KEY", "")
BUCKET_BRONZE_URL = os.getenv("BUCKET_ID")

HEADERS = {
    "Content-Type": "application/json",
    "X-OPENFIGI-APIKEY": API_KEY,
}

BASE_URL = "https://api.openfigi.com/v3/mapping"


def _create_job(ticker: str) -> dict:
    ticker = ticker.upper()
    return {"idType": "TICKER", "idValue": ticker, "marketSecDes": "Equity"}


def fetch_map(symbol: str) -> dict:
    job = _create_job(symbol)
    res = requests.post(BASE_URL, headers=HEADERS, json=[job], timeout=30)
    res.raise_for_status()
    data = res.json()

    # OpenFIGI returns a list with one entry per submitted job; ours has exactly one.
    result = data[0] if isinstance(data, list) and data else {}
    if "error" in result:
        raise ValueError(f"OpenFIGI mapping failed for {symbol}: {result['error']}")

    return result


def ingest_openfigi_financial_to_bronze(
    bucket: str,
    symbol: str,
):
    data = fetch_map(symbol)
    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="openfigi",
        dataset="mapping",
        payload=data,
        partitions={"symbol": symbol.upper()},
        schema_version=1,
    )

    return f"s3://{bucket}/{res.key}"


if __name__ == "__main__":
    res = ingest_openfigi_financial_to_bronze(BUCKET_BRONZE_URL, symbol="AAPL")
    print(res)
