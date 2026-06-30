"""
Minimal data-quality gate run on Silver dataframes before they reach Gold/Postgres.

This is intentionally cheap (row counts, null thresholds, duplicate keys,
sane value ranges) rather than a full validation framework -- the goal is
to stop a corrupted/empty batch from silently overwriting good data in the
serving layer, not to build a generic rules engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from src.core.exceptions import AppException


class DataQualityError(AppException):
    pass


@dataclass
class QualityReport:
    dataset: str
    row_count: int
    warnings: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.row_count > 0


def _fail(dataset: str, message: str) -> None:
    raise DataQualityError(f"[{dataset}] {message}")


def check_prices_1d(df: pl.DataFrame, *, max_null_close_ratio: float = 0.05) -> QualityReport:
    """Validate a Silver prices_1d dataframe before it is fed to the Gold writer.

    Hard failures (raise DataQualityError): empty frame, duplicate (symbol, ts),
    too many null closes, negative prices/volume.
    Soft findings (logged on the report): OHLC ordering violations.
    """
    dataset = "prices_1d"
    warnings: list[str] = []

    row_count = df.height
    if row_count == 0:
        _fail(dataset, "silver dataframe is empty, refusing to load into gold")

    required_cols = {"symbol", "ts", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        _fail(dataset, f"missing required columns: {sorted(missing)}")

    dup_count = row_count - df.unique(subset=["symbol", "ts"]).height
    if dup_count > 0:
        _fail(dataset, f"{dup_count} duplicate (symbol, ts) rows found")

    null_close = df.filter(pl.col("close").is_null()).height
    null_ratio = null_close / row_count
    if null_ratio > max_null_close_ratio:
        _fail(
            dataset,
            f"{null_close}/{row_count} ({null_ratio:.1%}) rows have a null close, "
            f"exceeds threshold of {max_null_close_ratio:.1%}",
        )
    elif null_close > 0:
        warnings.append(f"{null_close} rows have a null close")

    negative_price = df.filter(
        (pl.col("close") < 0) | (pl.col("open") < 0) | (pl.col("high") < 0) | (pl.col("low") < 0)
    ).height
    if negative_price > 0:
        _fail(dataset, f"{negative_price} rows have a negative OHLC value")

    negative_volume = df.filter(pl.col("volume") < 0).height
    if negative_volume > 0:
        _fail(dataset, f"{negative_volume} rows have negative volume")

    bad_ohlc = df.filter(
        (pl.col("high") < pl.col("low"))
        | (pl.col("high") < pl.col("close"))
        | (pl.col("low") > pl.col("close"))
    ).height
    if bad_ohlc > 0:
        warnings.append(f"{bad_ohlc} rows violate low<=close<=high<=... ordering")

    return QualityReport(dataset=dataset, row_count=row_count, warnings=warnings)
