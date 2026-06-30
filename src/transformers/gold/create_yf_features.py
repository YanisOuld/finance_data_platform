from src.core.config import settings

from .features.returns import add_return
from .writers.fetch_silver import create_silver_key, fetch_parquet_from_silver
from .writers.write_gold import write_gold_price1D

if __name__ == "__main__":
    silver_key = create_silver_key(type="history", dt="2026-02-24") + "/*.parquet"
    df = fetch_parquet_from_silver(settings.bucket_id, silver_key)
    df = add_return(df, "close")
    write_gold_price1D(df.collect())
    print(df.collect())
