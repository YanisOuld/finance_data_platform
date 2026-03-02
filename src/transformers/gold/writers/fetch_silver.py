from datetime import datetime, timezone
import polars as pl


def create_silver_key(type: str, dt: str):
	return f"silver/yahoo/{type}/dt={dt}"


def _create_s3_path(bucket: str, s3_key: str):
	return "s3://" +  bucket + "/" + s3_key


def fetch_parquet_from_silver(bucket: str, key: str) -> dict:
	'''
	fetch from the s3 bucket
	unzip if neeeded 
	encode and becore a dict
	'''
	silver_path = _create_s3_path(bucket=bucket, s3_key=key)
	df = pl.scan_parquet(
		silver_path,
	)
	

	return df