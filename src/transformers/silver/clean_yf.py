'''
Fetch the json

'''
import os

from dotenv import load_dotenv
from typing import List
import polars as pl

from .write_silver import store_to_s3, create_silver_key
from .fetch_bronze import create_bronze_key, fetch_json_from_bronze

load_dotenv()


def normalize_info(info: dict):
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

def _convert_str_date_to_date(column: str):
	col = pl.col(column).str.strptime(
		pl.Datetime,
		format="%Y-%m-%d %H:%M:%S%z"
	).dt.convert_time_zone("UTC").cast(pl.Date)
	return col

def clean_bronze(raw: dict) -> pl.DataFrame:
	df = pl.DataFrame(raw)
	df = df.with_columns(
		_convert_str_date_to_date("ts").alias("ts"),
		pl.col("symbol").list.first().alias("symbol")
	)

	return df


BUCKET_ID= os.getenv("BUCKET_ID")


if __name__ == "__main__":
	...
	key =  create_bronze_key(type="history", run_id="20260224180059Z", dt="2026-02-24")
	res = fetch_json_from_bronze(BUCKET_ID, key)
	table = normalize_history(res)
	df = clean_bronze(table)
	silver_key = create_silver_key(type="history", dt="2026-02-24")
	store_to_s3(bucket=BUCKET_ID, df=df, s3_key=silver_key)
	print(silver_key)