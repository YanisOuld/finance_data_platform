import os

from .writers.fetch_silver import create_silver_key, fetch_parquet_from_silver
from .features.returns import add_return
from .writers.write_gold import write_gold_price1D
from dotenv import load_dotenv

load_dotenv()


BUCKET_ID = os.getenv("BUCKET_ID")


if __name__ == "__main__":
	silver_key = create_silver_key(type="history", dt="2026-02-24") + "/*.parquet"
	df = fetch_parquet_from_silver(BUCKET_ID, silver_key)
	df = add_return(df, "close")
	write_gold_price1D(df.collect())
	print(df.collect())
	...