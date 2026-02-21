import requests
import os
from dotenv import load_dotenv

from core.constants import SecEdgarRequestType
from ingestion.writers.write_bronze import write_bronze_to_s3

import boto3

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


def _find_CIK(ticker: str):
	url = "https://www.sec.gov/files/company_tickers.json"
	r = requests.get(url, headers=HEADERS, timeout=30)
	r.raise_for_status()
	json = r.json()
	
	ticker = ticker.upper()
	for stock_dict in json.values():
		if stock_dict["ticker"] == ticker:
			return str(stock_dict["cik_str"]).zfill(10)
	
	raise ValueError(f"No CIk is associated to the ticker: {ticker} ")

def fetch_data(ticker: str, request_type: str):
	cik = _find_CIK(ticker)
	url = _create_url(cik, request_type)
	return _get_json(url)
	

def ingest_edgar_financial_to_bronze(
	bucket: str,
	ticker: str,
	start : str,
	end: str
):
	data = fetch_data(ticker, "submissions")
	res = write_bronze_to_s3(
		bucket=bucket,
		vendor="sec_edgar",
		dataset="financial",
		payload=data,
		partitions={"symbol": ticker.upper()},
		params={"start": start, "end": end},
		schema_version=1
	)

	return f"s3://{res.bucket}/{res.key}"


BUCKET_BRONZE_URL=os.getenv("BRONZE_BUCKET_ID")

if __name__ == "__main__":
	res = ingest_edgar_financial_to_bronze(
		bucket=BUCKET_BRONZE_URL,
		ticker="SOFI",
		start="2026-02-01",
		end="2026-02-15"
	)
	print(res)