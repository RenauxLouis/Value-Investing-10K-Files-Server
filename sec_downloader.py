import os
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup

from constants import (_10K_FILING_TYPE, BASE_URL,
                       MAP_SEC_PREFIX, PROXY_STATEMENT_FILING_TYPE,
                       SEC_CIK_TXT_URL, TICKER_CIK_CSV_FPATH, HTM_EXT,
                       XLSX_EXT)

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


def update_ticker_cik_df():

    with session.get(SEC_CIK_TXT_URL) as r:
        content = r.content.decode("utf-8")

    rows = [line.split("\t") for line in content.splitlines()]
    df = pd.DataFrame(rows, columns=["ticker", "cik"])
    df.to_csv(TICKER_CIK_CSV_FPATH)

    return df


def download(ticker, cik, years, ticker_folder):

    _10k_urls = get_folders_urls(
        filing_type=_10K_FILING_TYPE, years=years, cik=cik)
    excel_fpaths = []
    fiscal_years_10k = []
    local_years_with_xlsx = []
    print("Download 10K Forms")
    for index_url in _10k_urls:

        fiscal_year = get_fiscal_year(index_url)

        if fiscal_year in years:
            fiscal_years_10k.append(fiscal_year)
            year_folder = os.path.join(ticker_folder, fiscal_year)

            _10k_url = get_file_url(index_url, _10K_FILING_TYPE)

            prefix = MAP_SEC_PREFIX[_10K_FILING_TYPE]
            download_file_from_url(prefix, fiscal_year, HTM_EXT, ticker,
                                   _10k_url, year_folder)

            _10k_xslx_url = os.path.join(
                os.path.dirname(index_url), "Financial_Report.xlsx")
            excel_fpath = download_file_from_url(prefix, fiscal_year,
                                                 XLSX_EXT, ticker,
                                                 _10k_xslx_url, year_folder)
            if excel_fpath:
                local_years_with_xlsx.append(fiscal_year)
                excel_fpaths.append(excel_fpath)

        # Files from all requested years have been downloaded
        if set(fiscal_years_10k) == set(years):
            break

    proxy_statements_urls = get_folders_urls(
        filing_type=PROXY_STATEMENT_FILING_TYPE, years=years, cik=cik)
    fiscal_years_proxy = []
    print("Download Proxy Statements")
    for index_url in proxy_statements_urls:

        fiscal_year = get_fiscal_year(index_url)

        if fiscal_year in years:
            fiscal_years_proxy.append(fiscal_year)
            year_folder = os.path.join(ticker_folder, fiscal_year)

            proxy_url = get_file_url(index_url, PROXY_STATEMENT_FILING_TYPE)

            prefix = MAP_SEC_PREFIX[PROXY_STATEMENT_FILING_TYPE]
            download_file_from_url(prefix, fiscal_year, HTM_EXT, ticker,
                                   proxy_url, year_folder)

        # Files from all requested years have been downloaded
        if set(fiscal_years_proxy) == set(years):
            break

    return excel_fpaths, fiscal_years_10k, local_years_with_xlsx


def get_folders_urls(filing_type, years, cik):

    last_year_param = str(int(years[-1]) + 1) + "1231"

    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": last_year_param}

    with session.get(BASE_URL, params=params) as r:
        if r.status_code != 200:
            sys.exit("Ticker data not found when pulling filing_type: "
                     f"{filing_type}")

        data = r.text

    soup = BeautifulSoup(data, features="lxml")
    urls = [link.string for link in soup.find_all("filinghref")]
    types = [link.string for link in soup.find_all("type")]
    assert len(urls) == len(types)

    df_urls = pd.DataFrame(zip(urls, types), columns=["url", "type"])
    df_urls = df_urls.loc[df_urls["type"] == filing_type]
    urls = df_urls.url.values

    return urls


def get_fiscal_year(index_url):

    with session.get(index_url) as r:
        status_code = r.status_code
        if status_code == 200:
            data = r.text
        else:
            print("Error when request:", status_code)

    soup = BeautifulSoup(data, features="lxml")

    form_groupings = soup.find_all("div", {"class": "formGrouping"})
    for form_grouping in form_groupings:
        if form_grouping.find_all(text="Period of Report"):
            year_month_day = form_grouping.find(
                "div", {"class": "info"}).text
    fiscal_year = year_month_day.split("-")[0]

    return fiscal_year


def get_file_url(index_url, filing_type):

    with session.get(index_url) as r:
        status_code = r.status_code
        if status_code == 200:
            data = r.text
        else:
            print("Error when request:", status_code)

    soup = BeautifulSoup(data, features="lxml")
    tables = soup.find_all("table", {"class": "tableFile"})
    df_table = pd.read_html(str(tables[0]))[0]
    df_filetype = df_table.loc[df_table["Type"] == filing_type].copy()
    df_filetype_htm = df_filetype.loc[df_filetype[
        "Document"].str.contains(".htm")].copy()

    fnames = df_filetype_htm.Document.values
    assert len(fnames) == 1
    fname = fnames[0]
    cleaned_fname = fname.split(".htm")[0] + ".htm"

    file_url = os.path.join(os.path.dirname(index_url), cleaned_fname)

    return file_url


def download_file_from_url(prefix, year, ext, ticker, file_url, year_folder):

    with session.get(file_url) as r:
        status_code = r.status_code
        if status_code == 200:
            fpath = os.path.join(
                year_folder, f"{ticker.upper()}_{prefix}_{year}{ext}")
            with open(fpath, "wb") as output:
                output.write(r.content)
            return fpath
        else:
            print(f"Wrong status code: {status_code} when requesting {file_url}")
            return None
