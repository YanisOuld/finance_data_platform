import os
import requests

from dotenv import load_dotenv
from ingestion.writers.write_bronze import write_bronze_to_s3


load_dotenv()

BASE_URL="https://api.stlouisfed.org/fred/series/observations"


def _get_fred_instance():
	api_access = os.getenv("FRED_API_KEY")
	if not api_access:
		raise ValueError("The API key was not available")

	return api_access

def _find_series(macro: str):
	macro = macro.lower()
	
	table = {
		"cpi": "CPIAUCSL",
		"gdp": "GDP",
		"unemployement_rate": "UNRATE",
		"fed_funds": "FEDFUNDS",
		"usd/eur": "DEXUSEU",
		"usd/cad": "DEXCAUS",
		"usd/jpy": "DEXJPUS",
		"usd/gbp": "DEXUSUK"
	}

	return table.get(macro)


def fetch_series(macro: str, start: str, end: str):
	url = f"https://api.stlouisfed.org/fred/series/observations"

	series_id = _find_series(macro)

	if not series_id:
		raise ValueError("There is a missing ")

	params= {
		"series_id":series_id,
		"api_key":_get_fred_instance(),
		"file_type":"json",
		"observation_start": start,
		"observation_end": end 
	}

	res = requests.get(url=url, params=params, timeout=30)
	res.raise_for_status()
	return res.json()


def ingest_fred_to_bronze(bucket: str, macro: str, start: str, end: str):
	data = fetch_series(macro=macro, start=start, end=end)
	res = write_bronze_to_s3(
		bucket=bucket,
		vendor="fred",
		dataset="macros", 
		payload=data,
		partitions={"Macro": macro},
		params={"start": start, "end": end},
		schema_version=1 
	)

	return f"s3://{bucket}/{res.key}"

URL_BRONZE = os.getenv("BRONZE_BUCKET_ID")

if __name__ == "__main__":
	res = ingest_fred_to_bronze(URL_BRONZE, macro="cpi", start="2025-01-01", end="2025-12-31")
	print(res)

