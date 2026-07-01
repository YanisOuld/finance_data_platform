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
