"""
Regression test for a real bug found via live end-to-end testing: polars'
native `write_parquet("s3://...")` / `scan_parquet("s3://...")` do not
reliably pick up AWS credentials in this environment (confirmed broken both
natively and via pyarrow, on Windows and in the Linux Docker image) --
store_to_s3()/fetch_parquet_from_silver() now go through an explicit boto3
client instead (the same pattern write_bronze.py already used successfully).
This test guards against silently reverting to the broken polars-native path.
"""

from __future__ import annotations

import polars as pl

import src.transformers.gold.writers.fetch_silver as fetch_silver_mod
import src.transformers.silver.write_silver as write_silver_mod


class _FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body):  # noqa: N803 (matches boto3's signature)
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):  # noqa: N803
        class _Body:
            def __init__(self, data: bytes):
                self._data = data

            def read(self):
                return self._data

        return {"Body": _Body(self.objects[(Bucket, Key)])}


def test_store_to_s3_uploads_via_explicit_client_not_native_polars_cloud_io(monkeypatch):
    fake_s3 = _FakeS3Client()
    monkeypatch.setattr(write_silver_mod, "get_s3_client", lambda: fake_s3)

    df = pl.DataFrame({"symbol": ["AAPL", "AAPL"], "close": [190.0, 191.5]})
    s3_path = write_silver_mod.store_to_s3(
        bucket="test-bucket", df=df, s3_key="silver/prices_1d/test.parquet"
    )

    assert s3_path == "s3://test-bucket/silver/prices_1d/test.parquet"
    assert ("test-bucket", "silver/prices_1d/test.parquet") in fake_s3.objects
    # the uploaded bytes are a real, readable parquet file, not a placeholder
    uploaded_bytes = fake_s3.objects[("test-bucket", "silver/prices_1d/test.parquet")]
    assert pl.read_parquet(uploaded_bytes).to_dicts() == df.to_dicts()


def test_fetch_parquet_from_silver_roundtrips_with_store_to_s3(monkeypatch):
    fake_s3 = _FakeS3Client()
    monkeypatch.setattr(write_silver_mod, "get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(fetch_silver_mod, "get_s3_client", lambda: fake_s3)

    df = pl.DataFrame({"series": ["cpi", "cpi"], "value": [3.1, 3.2]})
    write_silver_mod.store_to_s3(bucket="test-bucket", df=df, s3_key="silver/macro/test.parquet")

    result = fetch_silver_mod.fetch_parquet_from_silver(bucket="test-bucket", key="silver/macro/test.parquet")

    assert isinstance(result, pl.LazyFrame)  # existing call sites rely on .collect() working
    assert result.collect().to_dicts() == df.to_dicts()
