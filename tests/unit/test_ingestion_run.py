from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data.crud.ingestion_run import finish_run, start_run
from src.data.models.ingestion_run import IngestionRun


@pytest.fixture
def session():
    # SQLite in-memory: exercises the real SQL (insert/update/PK lookup),
    # just without needing a live Postgres connection for a unit test.
    engine = create_engine("sqlite:///:memory:")
    IngestionRun.__table__.create(engine)
    SessionLocal = sessionmaker(bind=engine)
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()


def test_start_run_creates_running_row(session):
    run_id = start_run(session, dataset="prices_1d", run_date=date(2026, 6, 30))

    row = session.get(IngestionRun, run_id)
    assert row is not None
    assert row.status == "running"
    assert row.dataset == "prices_1d"
    assert row.run_date == date(2026, 6, 30)
    assert row.finished_at is None


def test_finish_run_marks_success_with_counts(session):
    run_id = start_run(session, dataset="prices_1d", run_date=date(2026, 6, 30))

    finish_run(session, run_id, status="success", items_total=5, items_success=5, items_failed=0)

    row = session.get(IngestionRun, run_id)
    assert row.status == "success"
    assert row.items_total == 5
    assert row.items_success == 5
    assert row.items_failed == 0
    assert row.finished_at is not None


def test_finish_run_records_failure_notes(session):
    run_id = start_run(session, dataset="macro", run_date=date(2026, 6, 30))

    finish_run(session, run_id, status="failed", items_total=1, items_failed=1, notes="boom")

    row = session.get(IngestionRun, run_id)
    assert row.status == "failed"
    assert row.items_failed == 1
    assert row.notes == "boom"


def test_finish_run_unknown_run_id_raises(session):
    with pytest.raises(ValueError):
        finish_run(session, "does-not-exist", status="failed")


def test_start_run_generates_unique_ids(session):
    run_id_1 = start_run(session, dataset="prices_1d", run_date=date(2026, 6, 30))
    run_id_2 = start_run(session, dataset="prices_1d", run_date=date(2026, 6, 30))

    assert run_id_1 != run_id_2
