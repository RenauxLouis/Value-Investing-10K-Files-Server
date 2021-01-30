CIK_URL = ("http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner"
           "=exclude&action=getcompany")
BASE_URL = "http://www.sec.gov/cgi-bin/browse-edgar"
BASE_EDGAR_URL = "https://www.sec.gov/Archives/edgar/data"
DEFAULT_FOLDER = "DEFAULT_FOLDER"

REGEX_PER_TARGET_SHEET = {
	"balance sheet": ["balance sheet"],
	"income": ["income", "earning", "operation"],
	"cash": ["cash"]
}

MAP_SEC_REGEX = {
	"10-K": ("10-k", "10k"),
	"10-K/A": ("htm", "10-ka"),
	"DEF 14A": ("", "")
}
MAP_SEC_PREFIX = {
	"10-K": "10K",
	"10-K/A": "10K_amended",
	"DEF 14A": "Proxy_Statement"
}

_10K_FILING_TYPE = "10-K"
PROXY_STATEMENT_FILING_TYPE = "DEF 14A"
TICKER_CIK_CSV_FPATH = "ticker_cik.csv"
SEC_CIK_TXT_URL = "https://www.sec.gov/include/ticker.txt"
ZIP_FILES_FOLDER = "zip_files"
