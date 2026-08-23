from __future__ import annotations

import io
from datetime import UTC, datetime

import polars as pl

from src.core.bucket_utils import get_s3_client


def create_silver_key(type: str, dt: str, vendor: str = "yahoo"):
    run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%SZ")
    return f"silver/{vendor}/{type}/dt={dt}/run_id={run_id}.parquet"


def store_to_s3(bucket: str, df: pl.DataFrame, s3_key: str) -> str:
    """Serializes to Parquet in memory, then uploads via an explicit boto3
    client (get_s3_client()) -- NOT polars' native `write_parquet("s3://...")`.
    That path depends on polars' built-in object_store cloud writer picking
    up AWS credentials from the process environment; confirmed broken here on
    both Windows and Linux (native: "invalid path" errors -- object_store
    isn't even recognizing the URI as cloud; use_pyarrow=True: picks up the
    URI but resolves as an anonymous/unauthenticated request, ignoring
    AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY). boto3 with explicit credentials
    is the same pattern already used for the Bronze layer and known to work.
    """
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3 = get_s3_client()
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())

    return f"s3://{bucket}/{s3_key}"
