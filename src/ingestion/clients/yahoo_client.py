import os
from dotenv import load_dotenv

from typing import Any, Dict, List

import yfinance as yf

from ingestion.writers.write_bronze import write_bronze_to_s3


load_dotenv()

def _fetch_ticker(symbol: str) -> yf.Ticker:
    """
    Initialize and return a Yahoo Finance ticker object.
    """
    if not symbol:
        raise ValueError("Symbol must not be empty")

    return yf.Ticker(symbol)


def fetch_stock_price(
    symbol: str,
    start: str,
    end: str
) -> List[Dict[str, Any]]:
    """
    Fetch historical stock prices between start and end date.
    Returns JSON-compatible list.
    """
    try:
        ticker = _fetch_ticker(symbol)
        hist = ticker.history(start=start, end=end)

        if hist.empty:
            return []

        hist = hist.reset_index()
        return hist.to_dict(orient="records")

    except Exception as e:
        raise RuntimeError(f"Failed to fetch stock price: {e}")


def ingest_yahoo_history_to_bronze(
    bucket: str,
    symbol: str,
    start: str,
    end: str,
) -> str:
    records = fetch_stock_price(symbol, start, end)

    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="yahoo",
        dataset="history",
        payload=records,
        partitions={"symbol": symbol.upper()},
        params={"start": start, "end": end},
        schema_version=1,
    )

    return f"s3://{res.bucket}/{res.key}"

URL_BRONZE = os.getenv("BRONZE_BUCKET_ID")

if __name__ == "__main__":
    res = ingest_yahoo_history_to_bronze(URL_BRONZE, symbol="SOFI", start="2026-02-01", end="2026-02-15")
    print(res)