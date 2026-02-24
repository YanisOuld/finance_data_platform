'''
Fetch the json

'''
import boto3
import os
import json
import gzip

from datetime import date
from dotenv import load_dotenv
from typing import List
import polars as pl

load_dotenv()


def _get_s3():
	return boto3.client("s3")

def _create_key(type: str, run_id: str, dt: str = None, symbol: str = None):
	'''
	'''
	base =  f"bronze/yahoo/{type}"

	if not dt and not symbol:
		raise ValueError("We need at least a dt or a symbol for the key!")
	
	if dt:
		base = f"{base}/dt={dt}"

	if symbol:
		symbol = symbol.upper()
		base = f"{base}/symbol={symbol}"
	
	url = f"{base}/run_id={run_id}.json.gz"
	print(url)
	return url

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

def normalize_info(info: dict ):
    res = { 
        "exchange": info["exchange"], 
        "quote_type": info["quoteType"],
        "timezone": info["timezone"],
        "currency": info["currency"]
    }
    return res

def normalize_history(raw: dict) -> List[dict]:
	'''
	'''
	meta = raw["meta"]
	params = meta["params"] if meta and "params" in meta else None

	if not params:
		raise ValueError("We are missing dates and symbols in the data ")
	
	payload = raw["payload"]


	out = []
	for stock in payload:
		symbol = stock["symbol"],
		data = stock["data"]
		for daily in data:
			out.append({
				"symbol" : symbol,
				"ts": daily["Date"],
				"open": daily["Open"],
				"high": daily["High"],
				"low": daily["Low"],
				"close": daily["Close"],
				"volume": daily["Volume"],
				"dividends": daily["Dividends"],
				"stock_split": daily["Stock Splits"]
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

if __name__ == "__main__":
	...
	key =  _create_key(type="history", run_id="20260224180059Z", dt="2026-02-24")
	res = fetch_json_from_bronze(BRONZE_BUCKET_ID, key)
	table = normalize_history(res)
	df = clean_bronze(table)
	print(df)
	