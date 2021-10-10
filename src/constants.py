CIK_URL = ("http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner"
           "=exclude&action=getcompany")
BASE_URL = "http://www.sec.gov/cgi-bin/browse-edgar"

REGEX_PER_TARGET_SHEET = {
	"balance sheet": ["balance sheet", "financial position"],
	"income": ["income", "earning", "operation"],
	"cash": ["cash"]
}

MAP_SEC_PREFIX = {
	"10-K": "10K",
	"10-K/A": "10K_amended",
	"DEF 14A": "Proxy_Statement"
}

HTM_EXT = ".htm"
XLSX_EXT = ".xlsx"
_10K_FILING_TYPE = "10-K"
PROXY_STATEMENT_FILING_TYPE = "DEF 14A"
TICKER_CIK_CSV_FPATH = "ticker_cik.csv"
SEC_CIK_TXT_URL = "https://www.sec.gov/include/ticker.txt"
TICKERS_10K_S3_BUCKET = "tickers-10k"
TOTAL_RETRIES = 3
STATUS_FORCELIST = [403]
BACKOFF_FACTOR = 4
