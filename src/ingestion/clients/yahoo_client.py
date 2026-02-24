import os
from dotenv import load_dotenv

from typing import Any, Dict, List, Union
from datetime import datetime, timezone

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
    ticker: yf.Ticker,
    start: str,
    end: str = None
) -> List[Dict[str, Any]]:
    """
    Fetch historical stock prices between start and end date.
    Returns JSON-compatible list.
    """
    try:
        if end is None:
            # Price for one day
            end = start
        hist = ticker.history(start=start, end=end)

        if hist.empty:
            return []

        hist = hist.reset_index()
        return hist.to_dict(orient="records")

    except Exception as e:
        raise RuntimeError(f"Failed to fetch stock price: {e}")
    
def fetch_info(ticker: yf.Ticker):
    return ticker.info


def ingest_yahoo_history_to_bronze(
    bucket: str,
    symbols: Union[List[str], str],
    start: str,
    end: str,
) -> str:
    '''
    Will be called daily 
    '''
    
    if isinstance(symbols, str):
        symbols = [symbols]
    
    payload = []
    for symbol in symbols:
        ticker = _fetch_ticker(symbol=symbol)
        records = fetch_stock_price(ticker=ticker, start=start, end=end)
        data = {"symbol": symbol, "data": records}
        payload.append(data)
    
    params = {"start": start, "end": end}

    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="yahoo",
        dataset="history",
        dt=datetime.now(timezone.utc),
        payload=payload,
        partitions=None,
        params=params,
        schema_version=1,
    )

    return f"s3://{res.bucket}/{res.key}"


def ingest_yahoo_info_to_bronze(bucket: str, symbol: str):
    '''
    We call this function the fisrt time we create data for this stock ! 
    '''
    ticker = _fetch_ticker(symbol=symbol)
    info = fetch_info(ticker=ticker)
    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="yahoo",
        dataset="info",
        payload=info,
        partitions={"symbol": symbol},
        params={"symbol": symbol},
        schema_version=1
    )

    return f"s3//{res.bucket}/{res.key}"

URL_BRONZE = os.getenv("BRONZE_BUCKET_ID")

if __name__ == "__main__":
    symbol = "SOFI"
    # info = ingest_yahoo_info_to_bronze(URL_BRONZE, symbol=symbol)
    # print(info)

    data = ingest_yahoo_history_to_bronze(URL_BRONZE, symbol, start="2026-01-01", end="2026-02-24")
    print(data)