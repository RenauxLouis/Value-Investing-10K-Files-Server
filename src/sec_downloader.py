import os
import sys
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from constants import (_10K_FILING_TYPE, BACKOFF_FACTOR, BASE_URL, HTM_EXT,
                       MAP_SEC_PREFIX, PROXY_STATEMENT_FILING_TYPE,
                       SEC_CIK_TXT_URL, STATUS_FORCELIST, TICKER_CIK_CSV_FPATH,
                       TOTAL_RETRIES, XLSX_EXT)

session = requests.Session()
retry = Retry(total=TOTAL_RETRIES, status_forcelist=STATUS_FORCELIST,
              backoff_factor=BACKOFF_FACTOR)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


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


def build_url(row, cik):

    url = os.path.join("https://www.sec.gov/Archives/edgar/data", 
        str(cik), 
        row.accessionNumber.replace("-", ""),
        row.primaryDocument
    )

    return url


def get_files_urls_and_year(ticker, cik, years):

    cik_leading_zeros = "0" * (10 - len(str(cik))) + str(cik)
    URL_JSON = f"https://data.sec.gov/submissions/CIK{cik_leading_zeros}.json"
    json_content = download_file_from_url(URL_JSON)

    filings = json_content["filings"]["recent"]
    df = pd.DataFrame.from_dict(filings, orient='index').transpose()
    df["year"] = df["reportDate"].apply(lambda date: date.split("-")[0])

    forms_to_keep = MAP_SEC_PREFIX.keys()
    mask_forms_years = df.form.isin(forms_to_keep) & df.year.isin(years)
    df = df.loc[mask_forms_years]
    df_10k = df.loc[df.form.isin(("10-K", "10-K/A"))].copy()
    df_10k.primaryDocument = "Financial_Report.xlsx"
    df = pd.concat((df, df_10k))

    df["url"] = df.apply(lambda row: build_url(row, cik), axis=1)

    return df


def download(ticker, cik, years, ticker_folder):

    df = get_files_urls_and_year(ticker, cik, years)
    fiscal_years_10k = list(df.year.unique())

    excel_fpaths = []
    for _, row in df.iterrows():

        year_folder = os.path.join(ticker_folder, row.year)
        os.makedirs(year_folder, exist_ok=True)

        prefix = MAP_SEC_PREFIX[row.form]
        ext = os.path.splitext(row.url)[1]
        fpath = os.path.join(
            year_folder, f"{ticker.upper()}_{prefix}_{row.year}{ext}")

        if row.primaryDocument == "Financial_Report.xlsx":
            excel_fpath = download_file_from_url(row.url, fpath)
            excel_fpaths.append(excel_fpath)
        else:
            download_file_from_url(row.url, fpath)

    return excel_fpaths, fiscal_years_10k


def http_download(url, params=None, retries=3):

    try:
        with session.get(url, params=params) as r:
            if r.status_code != 200:
                print(r.status_code)
                sys.exit(f"Wrong status code {r.status_code} when querying {url}")
            data = r.text
    except requests.exceptions.RetryError:
        if retries:
            time.sleep(2)
            http_download(url, params=params, retries=retries-1)
        else:
            sys.exit(f"Exceeded max retries when querying {url}")

    return data


def get_folders_urls(filing_type, years, cik):

    last_year_param = str(int(years[-1]) + 1) + "1231"

    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": last_year_param}

    data = http_download(BASE_URL, params)

    soup = BeautifulSoup(data, features="lxml")
    urls = [link.string for link in soup.find_all("filinghref")]
    types = [link.string for link in soup.find_all("type")]
    assert len(urls) == len(types)

    df_urls = pd.DataFrame(zip(urls, types), columns=["url", "type"])
    df_urls = df_urls.loc[df_urls["type"] == filing_type]
    urls = df_urls.url.values

    return urls


def get_fiscal_year(index_url):

    data = http_download(index_url)

    soup = BeautifulSoup(data, features="lxml")

    form_groupings = soup.find_all("div", {"class": "formGrouping"})
    for form_grouping in form_groupings:
        if form_grouping.find_all(text="Period of Report"):
            year_month_day = form_grouping.find(
                "div", {"class": "info"}).text
    fiscal_year = year_month_day.split("-")[0]

    return fiscal_year


def get_file_url(index_url, filing_type):

    data = http_download(index_url)

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


def download_file_from_url(file_url, fpath=None):

    HEADERS = {
        "User-Agent": "My User Agent 1.0",
    }

    with session.get(file_url, headers=HEADERS) as r:
        status_code = r.status_code
        if status_code == 200:
            if fpath is None:
                return r.json()
            else:
                with open(fpath, "wb") as output:
                    output.write(r.content)
                return fpath
        else:
            print(f"Wrong status code: {status_code} when requesting {file_url}")
            return None
