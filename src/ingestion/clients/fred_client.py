import os
import requests

from dotenv import load_dotenv
from src.core.constants import FRED_COLUMN_SERIES
from src.ingestion.writers.write_bronze import write_bronze_to_s3

load_dotenv()

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def _get_fred_api_key() -> str:
	api_key = os.getenv("FRED_API_KEY")
	if not api_key:
		raise ValueError("FRED_API_KEY env var is missing")

	return api_key


def _find_series(macro: str) -> str | None:
	return FRED_COLUMN_SERIES.get(macro.lower())


def fetch_series(macro: str, start: str, end: str) -> dict:
	series_id = _find_series(macro)

	if not series_id:
		raise ValueError(
			f"Unknown macro series '{macro}'. Known series: {sorted(FRED_COLUMN_SERIES)}"
		)

	params = {
		"series_id": series_id,
		"api_key": _get_fred_api_key(),
		"file_type": "json",
		"observation_start": start,
		"observation_end": end,
	}

	res = requests.get(url=BASE_URL, params=params, timeout=30)
	res.raise_for_status()
	return res.json()


def ingest_fred_to_bronze(bucket: str, macro: str, start: str, end: str) -> str:
	macro = macro.lower()
	data = fetch_series(macro=macro, start=start, end=end)
	res = write_bronze_to_s3(
		bucket=bucket,
		vendor="fred",
		dataset="macros",
		payload=data,
		partitions={"series": macro},
		params={"start": start, "end": end},
		schema_version=1,
	)

	return f"s3://{bucket}/{res.key}"

URL_BRONZE = os.getenv("BUCKET_ID")

if __name__ == "__main__":
	res = ingest_fred_to_bronze(URL_BRONZE, macro="cpi", start="2025-01-01", end="2025-12-31")
	print(res)

