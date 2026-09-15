import requests

from src.core.config import settings
from src.core.logger import get_logger
from src.core.retry import call_with_backoff
from src.ingestion.writers.write_bronze import write_bronze_to_s3

logger = get_logger(__name__)

"""
OpenFIGI /v3/mapping job: maps a ticker to its FIGI(s).

We deliberately do NOT filter by exchange (micCode/exchCode): this codebase
has no ticker->exchange mapping source of its own (that's the chicken-and-egg
problem OpenFIGI is meant to solve), so the previous version of this client
tried to look one up via an unimplemented `_find_min_code()` stub and always
crashed (the function had no body, plus "minCode" isn't even a valid OpenFIGI
field -- it's "micCode"/"exchCode"). Querying by TICKER + marketSecDes="Equity"
alone returns every matching listing across exchanges; an ambiguous ticker
will yield multiple FIGIs, and disambiguating against
universe_instruments.exchange/currency is left to a later identifiers/mapping
silver step.
"""


BASE_URL = "https://api.openfigi.com/v3/mapping"


def _build_headers() -> dict:
    """Built per-call (not at import) so the API key is read from live settings.
    The X-OPENFIGI-APIKEY header is omitted entirely when no key is set --
    sending an empty one pins us to the stricter anonymous quota with no
    benefit, and OpenFIGI treats a present-but-empty key inconsistently.
    """
    headers = {"Content-Type": "application/json"}
    if settings.openfigi_api_key:
        headers["X-OPENFIGI-APIKEY"] = settings.openfigi_api_key
    return headers


def _create_job(ticker: str) -> dict:
    ticker = ticker.upper()
    return {"idType": "TICKER", "idValue": ticker, "marketSecDes": "Equity"}


def fetch_map(symbol: str) -> dict:
    job = _create_job(symbol)
    headers = _build_headers()

    def _do_request() -> dict:
        res = requests.post(BASE_URL, headers=headers, json=[job], timeout=30)
        # 429 (rate limit) raises HTTPError -> RequestException -> retried by
        # call_with_backoff, honoring OpenFIGI's Retry-After header.
        res.raise_for_status()
        return res.json()

    data = call_with_backoff(
        _do_request, retry_on=(requests.RequestException,), description=f"OpenFIGI mapping symbol={symbol}"
    )

    # OpenFIGI returns a list with one entry per submitted job; ours has exactly one.
    result = data[0] if isinstance(data, list) and data else {}
    if "error" in result:
        raise ValueError(f"OpenFIGI mapping failed for {symbol}: {result['error']}")

    return result


def ingest_openfigi_financial_to_bronze(
    bucket: str,
    symbol: str,
):
    data = fetch_map(symbol)
    res = write_bronze_to_s3(
        bucket=bucket,
        vendor="openfigi",
        dataset="mapping",
        payload=data,
        partitions={"symbol": symbol.upper()},
        schema_version=1,
    )

    return f"s3://{bucket}/{res.key}"


if __name__ == "__main__":
    res = ingest_openfigi_financial_to_bronze(settings.bucket_id, symbol="AAPL")
    logger.info("%s", res)
