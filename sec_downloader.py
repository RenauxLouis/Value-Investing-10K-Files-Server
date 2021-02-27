import os
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup

from constants import (_10K_FILING_TYPE, BASE_EDGAR_URL, BASE_URL,
                       MAP_SEC_PREFIX, MAP_SEC_REGEX,
                       PROXY_STATEMENT_FILING_TYPE, SEC_CIK_TXT_URL,
                       TICKER_CIK_CSV_FPATH)

session = requests.Session()


class SECDownloader():

    def __init__(self):

        # self.ticker = None
        # self.years = None

        self.ticker_cik_df = None
        self.cik_per_ticker = None
        self.init_ticker_cik()

    def init_ticker_cik(self):
        self.ticker_cik_df = pd.read_csv(TICKER_CIK_CSV_FPATH)
        self.cik_per_ticker = dict(zip(self.ticker_cik_df.ticker,
                                       self.ticker_cik_df.cik))

    def get_ticker_cik(self, ticker):

        ticker_lower = ticker.lower()
        if ticker_lower not in self.cik_per_ticker:
            update_ticker_cik_df()
            self.init_ticker_cik()
            if ticker_lower not in self.cik_per_ticker:
                return None

        return str(self.cik_per_ticker[ticker_lower])


def download(ticker, cik, years, ticker_folder):

    print("Download", ticker, cik, years, ticker_folder)
    _10k_url_per_year = get_urls_per_year(
        filing_type=_10K_FILING_TYPE, years=years, cik=cik)
    excel_fpaths = []
    for year, url in _10k_url_per_year.items():
        year_folder = os.path.join(ticker_folder, year)
        os.makedirs(year_folder, exist_ok=True)

        accession_numbers = [url.split("/")[-2]]

        download_file_from_url_params(
            ticker, cik, year, accession_numbers, ".htm",
            _10K_FILING_TYPE, year_folder)
        excel_fpath = download_file_from_url_params(
            ticker, cik, year, accession_numbers, ".xlsx",
            _10K_FILING_TYPE, year_folder)
        excel_fpaths.append(excel_fpath)

    proxy_statements_url_per_year = get_urls_per_year(
        filing_type=PROXY_STATEMENT_FILING_TYPE, years=years, cik=cik)
    for year, url in proxy_statements_url_per_year.items():
        year_folder = os.path.join(ticker_folder, year)
        os.makedirs(year_folder, exist_ok=True)

        accession_numbers = [url.split("/")[-2]]

        download_file_from_url_params(
            ticker, cik, year, accession_numbers, ".htm",
            PROXY_STATEMENT_FILING_TYPE, year_folder)

    return excel_fpaths


def update_ticker_cik_df():
    r = session.get(SEC_CIK_TXT_URL)
    content = r.content.decode("utf-8")
    rows = [line.split("\t") for line in content.splitlines()]
    df = pd.DataFrame(rows, columns=["ticker", "cik"])
    df.to_csv(TICKER_CIK_CSV_FPATH)

    return df


def get_urls_per_year(filing_type, years, cik):
    last_year = years[-1]
    last_year_param = last_year + "1231"
    number_years_to_pull = len(years)

    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": last_year_param, "count": number_years_to_pull}
    r = session.get(BASE_URL, params=params)
    if r.status_code != 200:
        sys.exit("Ticker data not found when pulling filing_type: "
                 f"{filing_type}")

    data = r.text
    soup = BeautifulSoup(data, features="lxml")
    print(soup)

    urls = [link.string for link in soup.find_all("filinghref")]
    types = [link.string for link in soup.find_all("type")]
    dates_filed = [link.string for link in soup.find_all("datefiled")]
    years_filed = [date.split("-")[0] for date in dates_filed]
    assert len(urls) == len(types) == len(dates_filed)
    print("urls", urls)
    print("types", types)
    print("dates_filed", dates_filed)

    if year_missing(years_filed):
        urls_per_year = match_years_by_index()
    else:
        urls_per_year = match_years_by_value(types, years_filed,
                                             urls, filing_type)

    years_set = set(years)
    urls_per_year_set = set(urls_per_year.keys())
    assert years_set.issubset(urls_per_year_set), (
        f"{years_set} not a subset of {urls_per_year_set}"
    )
    urls_per_year = {k: v for k, v in urls_per_year.items() if k in years}

    return urls_per_year


def year_missing(years_filed):

    def _has_duplicate_items(_list):
        return len(_list) != len(set(_list))

    def _has_missing_year(years):

        years_as_int = [int(year) for year in years]
        min_year = min(years_as_int)
        max_year = max(years_as_int)
        years_range_set = set(range(min_year, max_year + 1))

        return years_range_set.issubset(set(years_as_int))

    return _has_duplicate_items(years_filed) or _has_missing_year(years_filed)


def match_years_by_value(types, years_filed, urls, filing_type):

    urls_per_year = {}
    for i, file_type in enumerate(types):
        if file_type == filing_type:
            year = years_filed[i]
            urls_per_year[year] = urls[i]

    return urls_per_year


def match_years_by_index(types, years, urls, filing_type):

    urls_per_year = {}
    for i, reversed_year in enumerate(years[::-1]):
        if types[i] == filing_type:
            urls_per_year[reversed_year] = urls[i]

    return urls_per_year


def download_file_from_url_params(ticker, cik, year, accession_numbers,
                                  ext, file_type, local_fpath):
    if ext == ".xlsx":
        regex = ("financial_report", "financial_report")
    else:
        regex = MAP_SEC_REGEX[file_type]

    prefix = MAP_SEC_PREFIX[file_type]

    full_url = combine_params_into_url(cik, accession_numbers, ext, *regex)
    r = session.get(full_url[0])
    status_code = r.status_code
    if status_code == 200:
        fpath = os.path.join(
            local_fpath, f"{ticker.upper()}_{prefix}_{year}{ext}")
        with open(fpath, "wb") as output:
            output.write(r.content)
    else:
        raise Exception(f"Wrong status code: {status_code}")

    return fpath


def combine_params_into_url(cik, accession_numbers, ext, if_1, if_2):
    return_urls = []
    for accession_number in accession_numbers:
        accession_number_url = os.path.join(
            BASE_EDGAR_URL, cik, accession_number).replace("\\", "/")
        with session.get(accession_number_url) as r:
            if r.status_code == 200:
                data = r.text
                soup = BeautifulSoup(data, features="lxml")
                links = [link.get("href") for link in soup.findAll("a")]
                corresponding_file_extension = [link for link in links if (
                        os.path.splitext(link)[-1] == ext)]
                urls = [link for link in corresponding_file_extension if (
                        if_1 in link.lower() or if_2 in link.lower())]
                urls_accession_num = [
                    url for url in urls if accession_number in url]
                # TODO
                # Find better method to pick the correct file (can't get cik V)
                if not urls_accession_num:
                    # Get first htm url with accession_number
                    urls_accession_num = [link for link in links if (
                            os.path.splitext(link)[
                                -1] == ext and accession_number in link)]
                fname = os.path.basename(urls_accession_num[0])
                url = os.path.join(accession_number_url, fname).replace(
                    "\\", "/")
                return_urls.append(url)
                # return_urls.append("check_amended")
            else:
                print("Error when request:", r.status_code)
    return return_urls
