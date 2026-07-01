"""
Historical backfill for prices_1d.

run_prices_pipeline() is built for the daily DAG's one-day-at-a-time volume;
pointed at a multi-year `start` for a new ticker it hands yfinance a single
huge multi-ticker request (tens of thousands of rows) and a single huge
Parquet write. This script instead chunks the backfill along two axes and
calls run_prices_pipeline() once per chunk:

  - by ticker: at most `ticker_batch_size` symbols per yf.download() call
  - by time: at most `years_per_chunk` calendar years per call

Each chunk goes through the normal bronze -> silver -> gold path (and gets
its own ingestion_runs row), so a failure partway through a multi-year
backfill only loses that one chunk, not the whole run -- it can be re-run
without redoing everything already loaded (the Gold upsert is idempotent).
"""

from __future__ import annotations

import argparse
import time
from datetime import date, timedelta

from src.core.constants import DEFAULT_BACKFILL_START
from src.core.logger import get_logger
from src.orchestration.pipelines.run_prices import run_prices_pipeline

logger = get_logger(__name__)


def chunk_symbols(symbols: list[str], batch_size: int) -> list[list[str]]:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    return [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]


def chunk_date_range(start: str, end: str, years_per_chunk: int) -> list[tuple[str, str]]:
    """Split [start, end] into consecutive, inclusive (chunk_start, chunk_end)
    windows of at most `years_per_chunk` calendar years each.
    """
    if years_per_chunk < 1:
        raise ValueError("years_per_chunk must be >= 1")

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if start_d > end_d:
        raise ValueError(f"start ({start}) must be <= end ({end})")

    chunks: list[tuple[str, str]] = []
    cur = start_d
    while cur <= end_d:
        try:
            chunk_end = cur.replace(year=cur.year + years_per_chunk)
        except ValueError:
            # cur is Feb 29 and the shifted year has no such day
            chunk_end = cur.replace(month=2, day=28, year=cur.year + years_per_chunk)
        chunk_end = min(chunk_end, end_d)
        chunks.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)

    return chunks


def backfill_prices(
    symbols: list[str],
    start: str = DEFAULT_BACKFILL_START,
    end: str | None = None,
    ticker_batch_size: int = 5,
    years_per_chunk: int = 1,
    sleep_seconds: float = 1.0,
) -> int:
    symbols = [s.strip().upper() for s in symbols if s and s.strip()]
    if not symbols:
        raise ValueError("symbols must contain at least one ticker")

    resolved_end = end or date.today().isoformat()
    symbol_batches = chunk_symbols(symbols, ticker_batch_size)
    date_chunks = chunk_date_range(start, resolved_end, years_per_chunk)

    total_rows = 0
    failures: list[str] = []

    for batch in symbol_batches:
        for chunk_start, chunk_end in date_chunks:
            logger.info("symbols=%s start=%s end=%s", batch, chunk_start, chunk_end)
            try:
                total_rows += run_prices_pipeline(batch, start=chunk_start, end=chunk_end)
            except Exception as e:
                logger.error("FAILED symbols=%s start=%s end=%s: %s", batch, chunk_start, chunk_end, e)
                failures.append(f"{batch}@{chunk_start}..{chunk_end}: {e}")
            time.sleep(sleep_seconds)

    if failures:
        logger.warning("finished with %s failed chunk(s):", len(failures))
        for f in failures:
            logger.warning("  - %s", f)

    return total_rows


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill prices_1d over a historical date range in chunks.")
    parser.add_argument("--symbols", required=True, help="Comma-separated tickers, e.g. AAPL,MSFT,SOFI")
    parser.add_argument("--start", default=DEFAULT_BACKFILL_START, help="YYYY-MM-DD (default: %(default)s)")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--ticker-batch-size", type=int, default=5)
    parser.add_argument("--years-per-chunk", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    rows = backfill_prices(
        symbols=args.symbols.split(","),
        start=args.start,
        end=args.end,
        ticker_batch_size=args.ticker_batch_size,
        years_per_chunk=args.years_per_chunk,
        sleep_seconds=args.sleep_seconds,
    )
    logger.info("backfill finished, total gold rows: %s", rows)
