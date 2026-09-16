import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.data.models.universal_instruments import UniversalInstrument


def get_scheduled_universe(session: Session) -> list[str]:
    stmt = select(UniversalInstrument.ticker).where(
        UniversalInstrument.is_active == True,
        UniversalInstrument.is_scheduled == True,
    )

    result = session.execute(stmt)
    return [row[0] for row in result.fetchall()]


def get_instrument(session: Session, ticker: str) -> UniversalInstrument | None:
    stmt = select(UniversalInstrument).where(UniversalInstrument.ticker == ticker.upper())
    return session.execute(stmt).scalar_one_or_none()


def _instruments_filter(stmt, is_active: bool | None, is_scheduled: bool | None):
    if is_active is not None:
        stmt = stmt.where(UniversalInstrument.is_active == is_active)
    if is_scheduled is not None:
        stmt = stmt.where(UniversalInstrument.is_scheduled == is_scheduled)
    return stmt


def list_instruments(
    session: Session,
    *,
    is_active: bool | None = None,
    is_scheduled: bool | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[UniversalInstrument]:
    """List the instrument universe, ordered by ticker.

    `limit=None` returns every matching row (the historical behaviour, fine
    while the universe is small); pass a limit to page through it once it grows.
    """
    stmt = _instruments_filter(select(UniversalInstrument), is_active, is_scheduled)
    stmt = stmt.order_by(UniversalInstrument.ticker)
    if offset:
        stmt = stmt.offset(offset)
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.execute(stmt).scalars().all())


def count_instruments(
    session: Session,
    *,
    is_active: bool | None = None,
    is_scheduled: bool | None = None,
) -> int:
    stmt = _instruments_filter(
        select(sa.func.count()).select_from(UniversalInstrument), is_active, is_scheduled
    )
    return session.execute(stmt).scalar_one()


def get_or_create_instrument(
    session: Session,
    ticker: str,
    *,
    name: str | None = None,
    exchange: str | None = None,
    currency: str = "USD",
    timezone: str = "America/New_York",
    is_active: bool = True,
    is_scheduled: bool = True,
) -> UniversalInstrument:
    """Idempotent create: returns the existing row if the ticker is already registered."""
    ticker = ticker.upper()
    existing = get_instrument(session, ticker)
    if existing is not None:
        return existing

    instrument = UniversalInstrument(
        ticker=ticker,
        name=name or ticker,
        exchange=exchange,
        currency=currency,
        timezone=timezone,
        is_active=is_active,
        is_scheduled=is_scheduled,
    )
    session.add(instrument)
    session.commit()
    session.refresh(instrument)
    return instrument


def set_scheduled(session: Session, ticker: str, is_scheduled: bool) -> UniversalInstrument | None:
    """Toggle whether the daily ETL DAG auto-picks up this ticker, without
    touching any other metadata. Returns None if the ticker isn't registered.
    """
    instrument = get_instrument(session, ticker)
    if instrument is None:
        return None

    instrument.is_scheduled = is_scheduled
    session.commit()
    session.refresh(instrument)
    return instrument
