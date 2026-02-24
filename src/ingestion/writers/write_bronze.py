'''
Convention: s3://<bucket>/bronze/{vendor}/{dataset}/dt=YYYY-MM-DD/{partitions}/run_id=YYYYMMDDTHHMMSSZ.json.gz

'''

from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import date, datetime, timezone
import io
import json
import os
from typing import Any, Dict, Optional

import boto3


@dataclass(frozen=True)
class BronzeWriteResult:
    bucket: str
    key: str
    fetched_at: str
    record_count: int

def _utc_now():
	return datetime.now(timezone.utc)

def _iso(dt: datetime):
	if dt is None:
		return None
	
	return dt.isoformat()

def _dt_partition(dt: datetime):
	if dt is None:
		return None
	
	return dt.strftime("%Y-%m-%d")


def _run_partition(dt: datetime):
	if dt is None:
		return None
	
	return dt.strftime("%Y%m%d%H%M%SZ")

def _normalize_partitions(partitions: Optional[Dict[str, Any]]) -> Dict[str, str]: 
	if not partitions:
		return {}
	
	out: Dict[str, str] = {}
	for key, value in partitions.items():
		if value is None:
			continue
		s = str(value).strip()
		if not s:
			continue
		out[key] = s
	return out

def make_bronze_key(
		vendor: str, 
		dataset: str,
		fetched_at: datetime | None = None,
		partitions: Optional[Dict[str, Any]] = None,
		ext: str = "json.gz"
	):
	
    vendor = vendor.strip().lower()
    dataset = dataset.strip().lower()
    now = _utc_now()

    dt = _dt_partition(fetched_at)
    run = _run_partition(now)

    parts = _normalize_partitions(partitions) 
    partition_path = "/".join([f"{k}={v}" for k, v in parts.items()]) 
	
    base = f"bronze/{vendor}/{dataset}"
    if dt:
    	base = f"{base}/dt={dt}"
	
    if partition_path:
        base = f"{base}/{partition_path}"
	
    return f"{base}/run_id={run}.{ext}"

def _gzip_json_bytes(obj: Any):
	raw = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
	buf = io.BytesIO()
	with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
		gz.write(raw)
	return buf.getvalue()

def write_bronze_to_s3(
    bucket: str,
    vendor: str,
    dataset: str,
    payload: Any,
    partitions: Optional[Dict[str, Any]] = None,
	dt: datetime = None,
    params: Optional[Dict[str, Any]] = None,
    schema_version: int = 1,
    s3_client=None,
) -> BronzeWriteResult:
    """
    """
    now = _utc_now()
    fetched_at = _iso(now)

    if isinstance(payload, list):
        record_count = len(payload)
    elif isinstance(payload, dict) and "records" in payload and isinstance(payload["records"], list):
        record_count = len(payload["records"])
    else:
        record_count = 1

    key = make_bronze_key(
        vendor=vendor,
        dataset=dataset,
        fetched_at=dt,
        partitions=partitions,
    )

    envelope = {
        "meta": {
            "source": vendor,
            "dataset": dataset,
            "fetched_at": fetched_at,
            "run_id": _run_partition(now),
            "dt": _dt_partition(dt),
            "schema_version": schema_version,
            "partitions": _normalize_partitions(partitions),
            "params": params or {},
            "record_count": record_count,
        },
        "payload": payload,
    }

    body = _gzip_json_bytes(envelope)

    if s3_client is None:
        s3_client = boto3.client("s3")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    return BronzeWriteResult(bucket=bucket, key=key, fetched_at=fetched_at, record_count=record_count)


def append_manifest_line_s3(
    bucket: str,
    manifest_key: str,
    line_obj: Dict[str, Any],
    s3_client=None,
) -> None:
    """
    
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    body = _gzip_json_bytes(line_obj)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
		