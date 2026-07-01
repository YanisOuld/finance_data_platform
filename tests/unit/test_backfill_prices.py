import pytest

from src.orchestration.pipelines.backfill_prices import chunk_date_range, chunk_symbols


def test_chunk_symbols_splits_into_batches():
    assert chunk_symbols(["A", "B", "C", "D", "E"], 2) == [["A", "B"], ["C", "D"], ["E"]]


def test_chunk_symbols_batch_size_larger_than_list():
    assert chunk_symbols(["A", "B"], 10) == [["A", "B"]]


def test_chunk_symbols_rejects_invalid_batch_size():
    with pytest.raises(ValueError):
        chunk_symbols(["A"], 0)


def test_chunk_date_range_splits_by_year():
    chunks = chunk_date_range("2015-01-01", "2017-06-01", years_per_chunk=1)

    assert chunks == [
        ("2015-01-01", "2016-01-01"),
        ("2016-01-02", "2017-01-02"),
        ("2017-01-03", "2017-06-01"),
    ]


def test_chunk_date_range_single_chunk_when_range_smaller_than_chunk_size():
    chunks = chunk_date_range("2024-01-01", "2024-03-01", years_per_chunk=1)

    assert chunks == [("2024-01-01", "2024-03-01")]


def test_chunk_date_range_rejects_start_after_end():
    with pytest.raises(ValueError):
        chunk_date_range("2024-01-01", "2023-01-01", years_per_chunk=1)


def test_chunk_date_range_rejects_invalid_years_per_chunk():
    with pytest.raises(ValueError):
        chunk_date_range("2024-01-01", "2024-06-01", years_per_chunk=0)
