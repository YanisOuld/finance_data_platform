from __future__ import annotations

import io

import polars as pl

from src.core.bucket_utils import get_s3_client


def create_silver_key(type: str, dt: str):
    return f"silver/yahoo/{type}/dt={dt}"


def fetch_parquet_from_silver(bucket: str, key: str) -> pl.LazyFrame:
    """Downloads via an explicit boto3 client and reads the bytes with
    pl.read_parquet() -- NOT polars' native `scan_parquet("s3://...")`, which
    depends on the same broken credential auto-detection as write_silver.py's
    store_to_s3() (see its docstring). Returns a LazyFrame (.lazy()) purely
    so every existing call site's `.collect()` keeps working unchanged --
    the file is already fully in memory by this point either way.
    """
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(obj["Body"].read())

    df = pl.read_parquet(buffer)
    return df.lazy()
