"""
Single place that knows how to build an S3 client. Previously every
ingestion/transform module called `boto3.client("s3")` with no arguments,
relying on boto3's implicit credential chain -- fixed here by passing
credentials from Settings explicitly.
"""

from __future__ import annotations

import boto3

from src.core.config import settings


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
    )
