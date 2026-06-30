import requests
import os
from dotenv import load_dotenv

from src.ingestion.writers.write_bronze import write_bronze_to_s3


load_dotenv()

'''
Theory:
CIK: Central index Key
SEC fournit un json complet pour mapper tiker avec son CIK

Voici un example de format:
  - Comment on va stocker cela
{
  "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
  "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
  ...
}


'''

HEADERS = {
    "User-Agent": "finance-data-platform/0.1 (contact: ouldmayanis@gmail.com)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

def _get_json(url: str) -> dict:
	r = requests.get(url, headers=HEADERS, timeout=30)
	r.raise_for_status()
	return r.json()


def _create_url(cik: str, request_type: str):
	"""
	Understand how can 
	"""
	request_type = request_type.lower()
	if request_type == "submissions":
		return f"https://data.sec.gov/submissions/CIK{cik}.json"
	
	if request_type == "companyfacts":
		return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
	
	raise ValueError(
        f"request_type must be 'submissions' or 'companyfacts', got: {request_type}"
    )


# Module-level cache: company_tickers.json is a single ~10MB file covering
# every ticker. Re-downloading it on every _find_CIK() call (the previous
# behavior) made each ingestion call pay that cost again for no reason.
_CIK_BY_TICKER: dict[str, str] | None = None


def _load_cik_table() -> dict[str, str]:
	global _CIK_BY_TICKER
	if _CIK_BY_TICKER is not None:
		return _CIK_BY_TICKER

	url = "https://www.sec.gov/files/company_tickers.json"
	r = requests.get(url, headers=HEADERS, timeout=30)
	r.raise_for_status()
	data = r.json()

	_CIK_BY_TICKER = {
		stock["ticker"].upper(): str(stock["cik_str"]).zfill(10)
		for stock in data.values()
	}
	return _CIK_BY_TICKER


def _find_CIK(ticker: str):
	table = _load_cik_table()
	cik = table.get(ticker.upper())
	if not cik:
		raise ValueError(f"No CIK is associated to the ticker: {ticker}")
	return cik


def fetch_data(ticker: str, request_type: str):
	cik = _find_CIK(ticker)
	url = _create_url(cik, request_type)
	return _get_json(url)


def ingest_edgar_financial_to_bronze(
	bucket: str,
	ticker: str,
	start: str,
	end: str,
):
	# companyfacts carries the actual XBRL financial facts (revenue, EPS, etc.);
	# "submissions" (the previous default here) is just filing metadata and has
	# no financial figures at all, despite the function name.
	data = fetch_data(ticker, "companyfacts")
	res = write_bronze_to_s3(
		bucket=bucket,
		vendor="sec_edgar",
		dataset="companyfacts",
		payload=data,
		partitions={"symbol": ticker.upper()},
		params={"start": start, "end": end},
		schema_version=1
	)

	return f"s3://{res.bucket}/{res.key}"


BUCKET_BRONZE_URL=os.getenv("BUCKET_ID")

if __name__ == "__main__":
	res = ingest_edgar_financial_to_bronze(
		bucket=BUCKET_BRONZE_URL,
		ticker="SOFI",
		start="2026-02-01",
		end="2026-02-15"
	)
	print(res)