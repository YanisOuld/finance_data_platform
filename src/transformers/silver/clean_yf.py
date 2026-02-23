'''
Fetch the json

'''
import boto3
import os
import json
import gzip

from dotenv import load_dotenv
from typing import List
import polars as pl

load_dotenv()


def _get_s3():
	return boto3.client("s3")

def _create_key():
	...

def fetch_json_from_bronze(bucket: str, key: str) -> dict:
	'''
	fetch from the s3 bucket
	unzip if neeeded 
	encode and becore a dict
	'''
	s3 = _get_s3()
	obj = s3.get_object(Bucket=bucket, Key=key)
	raw = obj["Body"].read()
	if key.endswith("gz"):
		raw = gzip.decompress(raw)

	return json.loads(raw.decode("utf-8"))

def normalize_json(raw: dict) -> List[dict]:
	meta = raw["meta"]
	params = meta["params"] if meta and "params" in meta else None
	symbol = meta["partitions"]["symbol"] if meta and "partitions" in meta and "symbol" in meta["partitions"] else None

	if not params or not symbol:
		raise ValueError("We are missing dates and symbols in the data ")
	
	payload = raw["payload"]
	print(payload)

	out = []
	for bar in payload:
		out.append({
			"symbol" : symbol,
			"ts": bar["Date"],
			"open": bar["Open"],
			"high": bar["High"],
			"Low": bar["Low"],
			"close": bar["Close"],
			"volume": bar["Volume"],
			"dividends": bar["Dividends"],
			"stock_split": bar["Stock Splits"]
			# There is some fuck it
		})

	return out

def clean_bronze(raw: dict) -> pl.DataFrame:
	df = pl.DataFrame(raw)

	return df

	...

def store_to_local():
	
	...

def store_to_s3():

	...


BRONZE_BUCKET_ID= os.getenv("BRONZE_BUCKET_ID")

KEY = "bronze/yahoo/history/dt=2026-02-20/symbol=SOFI/run_id=20260220053445Z.json.gz"


if __name__ == "__main__":
	...
	res = fetch_json_from_bronze(BRONZE_BUCKET_ID, KEY)
	table = normalize_json(res)
	df = clean_bronze(table)
	print(df)
	