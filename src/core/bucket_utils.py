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
    kwargs = {
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
        "region_name": settings.aws_region,
    }
    # When pointed at an S3-compatible store like MinIO (local stack), we must
    # also force path-style addressing -- MinIO doesn't do the
    # <bucket>.<host> virtual-hosted style AWS uses by default.
    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url
        kwargs["config"] = boto3.session.Config(s3={"addressing_style": "path"})
    return boto3.client("s3", **kwargs)
