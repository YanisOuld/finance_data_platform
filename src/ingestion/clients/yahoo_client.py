# src/ingestion/clients/yahoo_client.py
from __future__ import annotations

import os
import random
import time
from datetime import UTC, datetime
from typing import Any

import pendulum
import requests
import yfinance as yf
from dotenv import load_dotenv

from src.ingestion.writers.write_bronze import write_bronze_to_s3

load_dotenv()


# -----------------------------
# Debug helper (optional)
# -----------------------------
def _debug_yahoo_chart(symbol: str) -> None:
    """
    Hits Yahoo chart endpoint directly to diagnose:
    - 429 rate limit
    - HTML responses / blocking
    """
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d"
        r = requests.get(url, timeout=20)
        ct = (r.headers.get("content-type") or "").lower()
        snippet = (r.text or "")[:140].replace("\n", " ")
        print(
            f"[DEBUG] yahoo_chart symbol={symbol} status={r.status_code} content-type={ct} first140={snippet}"
        )
    except Exception as e:
        print(f"[DEBUG] yahoo_chart failed symbol={symbol} err={type(e).__name__}: {e}")


# -----------------------------
# Recommended price fetcher
# -----------------------------
def fetch_prices_1d_safe(
    symbols: list[str] | str,
    start: str,
    end: str,
    *,
    max_retries: int = 6,
    debug_on_empty: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetch daily OHLCV prices using yf.download (more stable than Ticker().history()).

    - end is treated as inclusive (we convert to exclusive end for yfinance).
    - threads=False reduces rate-limit issues.
    - returns FLAT list of dict rows:
        {ts, open, high, low, close, volume, symbol}

    NOTE:
    - This intentionally does NOT call .info
    - Designed for "just prices" reliability
    """
    if isinstance(symbols, str):
        symbols_list = [symbols]
    else:
        symbols_list = list(symbols)

    syms = [s.strip().upper() for s in symbols_list if s and s.strip()]
    if not syms:
        return []

    if not isinstance(start, str):
        raise TypeError(f"start must be YYYY-MM-DD str, got {type(start)}")
    if not isinstance(end, str):
        raise TypeError(f"end must be YYYY-MM-DD str, got {type(end)}")

    start_p = pendulum.parse(start).to_date_string()
    end_excl = pendulum.parse(end).add(days=1).to_date_string()

    last_err: Exception | None = None

    for attempt in range(max_retries):
        try:
            df = yf.download(
                tickers=syms,
                start=start_p,
                end=end_excl,
                interval="1d",
                auto_adjust=False,
                group_by="ticker",
                threads=False,  # IMPORTANT: less aggressive
                progress=False,
            )

            if df is None or df.empty:
                if debug_on_empty:
                    print(
                        f"[WARN] yf.download returned empty. symbols={syms} start={start_p} end_excl={end_excl}"
                    )
                    # Debug 1 symbol only to keep noise down
                    _debug_yahoo_chart(syms[0])
                return []

            rows: list[dict[str, Any]] = []

            # MultiIndex columns for multiple tickers: (TICKER, Field)
            is_multi = hasattr(df.columns, "levels") and len(getattr(df.columns, "levels", [])) == 2

            if is_multi:
                for sym in syms:
                    if sym not in df.columns.levels[0]:
                        continue
                    sub = df[sym].dropna(how="all")
                    if sub.empty:
                        continue
                    sub = sub.reset_index()  # adds Date
                    for r in sub.to_dict(orient="records"):
                        ts = r.get("Date")
                        if ts is None:
                            continue
                        rows.append(
                            {
                                "ts": ts,
                                "open": r.get("Open"),
                                "high": r.get("High"),
                                "low": r.get("Low"),
                                "close": r.get("Close"),
                                "volume": r.get("Volume"),
                                "symbol": sym,
                            }
                        )
            else:
                # single ticker sometimes returns flat columns
                sub = df.dropna(how="all").reset_index()
                sym = syms[0]
                for r in sub.to_dict(orient="records"):
                    ts = r.get("Date")
                    if ts is None:
                        continue
                    rows.append(
                        {
                            "ts": ts,
                            "open": r.get("Open"),
                            "high": r.get("High"),
                            "low": r.get("Low"),
                            "close": r.get("Close"),
                            "volume": r.get("Volume"),
                            "symbol": sym,
                        }
                    )

            if not rows and debug_on_empty:
                print(f"[WARN] yf.download produced dataframe but no rows extracted. symbols={syms}")
                _debug_yahoo_chart(syms[0])

            return rows

        except Exception as e:
            last_err = e
            sleep_s = min(60, (2**attempt) + random.random())
            print(f"[WARN] yf.download failed attempt={attempt+1}/{max_retries} err={e} sleep={sleep_s:.2f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"yfinance download failed after retries: {last_err}")


# -----------------------------
# Optional: info fetcher (fragile)
# -----------------------------
def fetch_info(symbol: str, *, max_retries: int = 5) -> dict[str, Any]:
    """
    Fetch ticker.info (more fragile than prices). Includes retries.
    Avoid using this in the daily path if you want maximum reliability.
    """
    symbol = symbol.strip().upper()
    last_err: Exception | None = None

    for attempt in range(max_retries):
        try:
            t = yf.Ticker(symbol)
            info = t.info
            if not isinstance(info, dict) or not info:
                print(f"[WARN] empty info for symbol={symbol}")
                _debug_yahoo_chart(symbol)
                return {}
            return info
        except Exception as e:
            last_err = e
            sleep_s = min(60, (2**attempt) + random.random())
            print(
                f"[WARN] info failed symbol={symbol} attempt={attempt+1}/{max_retries} err={e} sleep={sleep_s:.2f}s"
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch ticker info for {symbol}: {last_err}")


# -----------------------------
# Bronze ingestion (prices only)
# -----------------------------
def ingest_yahoo_history_to_bronze(
    bucket: str,
    symbols: list[str] | str,
    start: str,
    end: str,
) -> str:
    """
    Write bronze payload for daily prices.

    Payload shape (prices-only):
      {
        "start": "...",
        "end": "...",
        "rows": [
          {"ts": ..., "open": ..., "high": ..., "low": ..., "close": ..., "volume": ..., "symbol": "..."},
          ...
        ],
        "errors": [{"symbol": "...", "error": "..."}]   # optional
      }

    This keeps silver normalization MUCH simpler.
    """
    if not bucket:
        raise RuntimeError("bucket is required (BUCKET_ID env var likely missing)")

    # normalize symbols
    if isinstance(symbols, str):
        symbols_list = [symbols]
    else:
        symbols_list = list(symbols)

    syms = [s.strip().upper() for s in symbols_list if s and s.strip()]
    if not syms:
        raise ValueError("symbols must contain at least one ticker")

    # fetch prices (batch)
    rows: list[dict[str, Any]] = fetch_prices_1d_safe(syms, start=start, end=end)

    # We can detect missing tickers by checking which symbols appear in rows
    seen = {r.get("symbol") for r in rows if isinstance(r, dict)}
    errors: list[dict[str, Any]] = []
    for sym in syms:
        if sym not in seen:
            errors.append({"symbol": sym, "error": "no_rows_returned"})

    payload: dict[str, Any] = {
        "start": start,
        "end": end,
        "rows": rows,
        "errors": errors,
    }

    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="yahoo",
        dataset="history",  # keep name if you want, but it's really prices_1d
        dt=datetime.now(UTC),
        payload=payload,
        partitions=None,
        params={"start": start, "end": end, "symbols": ",".join(syms)},
        schema_version=2,  # bumped because payload shape changed
    )

    s3_path = f"s3://{res.bucket}/{res.key}"
    print(f"bronze data written to: {s3_path}")
    return s3_path


# -----------------------------
# One-time info ingestion (optional)
# -----------------------------
def ingest_yahoo_info_to_bronze(bucket: str, symbol: str) -> str:
    if not bucket:
        raise RuntimeError("bucket is required (BUCKET_ID env var likely missing)")

    symbol = symbol.strip().upper()
    info = fetch_info(symbol=symbol)

    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="yahoo",
        dataset="info",
        dt=datetime.now(UTC),
        payload=info,
        partitions={"symbol": symbol},
        params={"symbol": symbol},
        schema_version=1,
    )

    return f"s3://{res.bucket}/{res.key}"


if __name__ == "__main__":
    BUCKET = os.getenv("BUCKET_ID", "")
    print(ingest_yahoo_history_to_bronze(BUCKET, ["AMZN"], start="2026-01-01", end="2026-01-02"))
