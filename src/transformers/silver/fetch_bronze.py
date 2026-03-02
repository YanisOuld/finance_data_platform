import boto3
import json
import gzip


def _get_s3():
	return boto3.client("s3")

def create_bronze_key(type: str, run_id: str, dt: str = None, symbol: str = None):
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