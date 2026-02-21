import os
import requests
from dotenv import load_dotenv

from ingestion.writers.write_bronze import write_bronze_to_s3


'''
# Example: AAPL on NASDAQ (MIC = XNAS)
jobs = [
    {
        "idType": "TICKER",
        "idValue": "AAPL",
        "micCode": "XNAS",       # NASDAQ
        "marketSector": "Equity" # helps narrow results
    }
]


Concept un peu plus complexe pour OpenFIGI

On doit donner des specs pour avoir un résulat concluent:
- Ils faut donc savoir quel Stock exchange il utilise ?

'''


load_dotenv()

API_KEY=os.getenv("OPENFIGI_API_KEY", "")
BUCKET_BRONZE_URL=os.getenv("BRONZE_BUCKET_ID")

HEADERS = {
  	"Content-Type" : "application/json",
  	"X-OPENFIGI-APIKEY": API_KEY,
}

BASE_URL = "https://api.openfigi.com/v3/mapping"


def _find_min_code(ticker: str):
	'''
		MinCode est le exchange associé à la compagnie 
	'''

def _create_job(ticker: str):
	ticker = ticker.upper()
	min_code = _find_min_code(ticker)
	job = [{"idType": "TICKER", "idValue": ticker, "minCode": min_code, "marketSector": "Equity"}]
	return job


def fetch_map(symbol: str) -> dict:
	job = _create_job(symbol)
	res = requests.post(BASE_URL, headers=HEADERS, json=[job], timeout=30)
	res.raise_for_status()
	return res.json()


def ingest_openfigi_financial_to_bronze(
	bucket: str,
	symbol: str,
):
	data = fetch_map(symbol)
	res = write_bronze_to_s3(
		bucket=bucket,
		dataset="openfigi",
		payload=data,
		partitions={"symbol": symbol.upper()},
		schema_version=1
	)

	return f"s3://{bucket}/{res.key}"
