from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict

from src.core.constants import DEFAULT_BACKFILL_START


class InstrumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ticker: str
    name: str
    exchange: str | None
    currency: str
    timezone: str
    is_active: bool
    is_scheduled: bool


class InstrumentCreate(BaseModel):
    ticker: str
    is_scheduled: bool = True
    backfill_start: str = DEFAULT_BACKFILL_START
    backfill_end: str | None = None


class ScheduledUpdate(BaseModel):
    is_scheduled: bool


class PriceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    ts: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    close_returns: float | None


class FundamentalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    concept: str
    unit: str
    period_end: date
    fy: int | None
    fp: str
    form: str
    val: float


class MacroSeriesResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    series: str
    ts: date
    value: float | None


class FigiResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    figi: str
    composite_figi: str | None
    share_class_figi: str | None
    security_type: str | None
    market_sector: str | None
    exch_code: str | None
    name: str | None
