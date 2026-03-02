import polars as pl

from datetime import datetime , timezone


def create_silver_key(type: str, dt: str):
	run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%SZ")
	return f"silver/yahoo/{type}/dt={dt}/run_id={run_id}.parquet"

def store_to_s3(bucket: str, df: pl.DataFrame, s3_key: str):
	s3_path = "s3://" +  bucket + "/" + s3_key
	df.write_parquet(s3_path)
	return s3_path