'''
	Any important information will be stored there
'''
from enum import Enum

FRED_COLUMN_SERIES = {
	"interest_rate_10y":"GS10",
	"inflation": "CPIAUCNS",
	"unemployment": "UNRATE",
  	"yield_curve": "T10Y2Y",
}

BASE_URL_FIGI="https://api.openfigi.com"


class SecEdgarRequestType(Enum):
	SUBMISSIONS="submissions",
	COMPANY_FACTS="companyfacts"


