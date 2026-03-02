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