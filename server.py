import os
import shutil
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from constants import SEC_CIK_TXT_URL, TICKER_CIK_CSV_FPATH, BASE_URL
from download_10k_utils import (clean_excel,
                                download_years_in_ticker_folder_from_s3,
                                filter_s3_urls_to_send,
                                get_existing_merged_fpaths, get_existing_years,
                                get_fpaths_from_local_ticker,
                                merge_excel_files_across_years, parse_inputs,
                                upload_files_to_s3)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sec_downloader import SECDownloader, download, update_ticker_cik_df

session = requests.Session()
retry = Retry(total=3, status_forcelist=[403], backoff_factor=2)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

app = FastAPI()
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

sec_downloader = SECDownloader()


@app.get("/list_sec_filing_10k/")
async def get_list_sec_tickers(ticker):

    cik = sec_downloader.get_ticker_cik(ticker)

    params = {"action": "getcompany", "owner": "exclude",
              "output": "html", "CIK": cik, "type": "10-K"}

    with session.get(BASE_URL, params=params) as r:
        if r.status_code != 200:
            print(r.status_code)
            sys.exit("Ticker data not found when pulling filing_type: "
                     f"{filing_type}")
        data = r.text

    soup = BeautifulSoup(data, features="lxml")
    tables = soup.find_all("td")
    print(tables)
    is_ticker_filing_10k = True

    return {"is_ticker_filing_10k": is_ticker_filing_10k}

@app.get("/list_sec/")
async def get_list_sec_tickers():

    update_ticker_cik_df()
    df_tickers = pd.read_csv(TICKER_CIK_CSV_FPATH)

    list_tickers = list(df_tickers["ticker"].dropna().values)

    return {"tickers": list_tickers}


@app.get("/params/")
async def download_10k(ticker, years, _10k, Proxy, Balance, Income, Cash):

    with TemporaryDirectory() as dirpath:
        s3_urls_to_send_to_user, _ = get_s3_urls_to_send_to_user(
            ticker, years, _10k, Proxy, Balance, Income, Cash, dirpath)

        return {"s3_urls": s3_urls_to_send_to_user}


@app.get("/params_web/")
async def download_10k_web(ticker, years, _10k, Proxy, Balance, Income, Cash):

    with TemporaryDirectory() as dirpath:
        _, ticker_folder = get_s3_urls_to_send_to_user(
            ticker, years, _10k, Proxy, Balance, Income, Cash, dirpath)

        shutil.make_archive(ticker, "zip", ticker_folder)
        response = FileResponse(path=ticker + ".zip",
                                filename=ticker + ".zip")
        return response


def get_s3_urls_to_send_to_user(ticker, years, _10k, Proxy,
                                Balance, Income, Cash, dirpath):

    raw_files_to_send, merged_files_to_send, years = parse_inputs(
        _10k, Proxy, Balance, Income, Cash, years)
    print(raw_files_to_send, merged_files_to_send)

    cik = sec_downloader.get_ticker_cik(ticker)

    ticker_folder = os.path.join(dirpath, ticker)
    os.makedirs(ticker_folder)

    existing_s3_urls = download_years_in_ticker_folder_from_s3(
        ticker,  ticker_folder, years)
    created_fpath = create_missing_files(ticker, ticker_folder, cik, years)
    s3_urls = upload_files_to_s3(created_fpath, existing_s3_urls,
                                 ticker, ticker_folder)

    s3_urls_to_send_to_user = filter_s3_urls_to_send(
        s3_urls, raw_files_to_send, merged_files_to_send)

    return s3_urls_to_send_to_user, ticker_folder


def create_missing_files(ticker, ticker_folder, cik, years):

    existing_years = get_existing_years(ticker_folder)
    missing_years = [year for year in years if year not in existing_years]

    if missing_years:
        excel_fpaths_to_clean, created_years = download(
            ticker, cik, missing_years, ticker_folder)
        for excel_fpath in excel_fpaths_to_clean:
            clean_excel(excel_fpath)
    else:
        created_years = []

    local_years = existing_years + created_years

    existing_merged_fpaths = get_existing_merged_fpaths(
        ticker, ticker_folder, local_years)
    if len(existing_merged_fpaths) == 3:
        merged_fpaths = existing_merged_fpaths
    else:
        merged_fpaths = merge_excel_files_across_years(ticker, ticker_folder)

    raw_fpaths = get_fpaths_from_local_ticker(ticker_folder, local_years)
    created_fpath = raw_fpaths + merged_fpaths

    return created_fpath
