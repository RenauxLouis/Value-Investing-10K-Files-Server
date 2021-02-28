import os
from tempfile import TemporaryDirectory

from fastapi import FastAPI

from constants import DEFAULT_FOLDER
from download_10k_utils import (clean_excel,
                                download_ticker_folder_from_s3,
                                filter_s3_urls_to_send,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker,
                                get_existing_years,
                                merge_excel_files_across_years, parse_inputs,
                                upload_files_to_s3)
from sec_downloader import SECDownloader, download

app = FastAPI()

sec_downloader = SECDownloader()


@app.get("/params/")
async def download_10k(ticker, years, _10k, Proxy, Balance, Income, Cash):

    raw_files_to_send, merged_files_to_send, years = parse_inputs(
        _10k, Proxy, Balance, Income, Cash, years)
    print(raw_files_to_send, merged_files_to_send)

    cik = sec_downloader.get_ticker_cik(ticker)

    with TemporaryDirectory() as dirpath:

        ticker_folder = os.path.join(dirpath, DEFAULT_FOLDER, ticker)
        os.makedirs(ticker_folder)

        existing_s3_urls = download_ticker_folder_from_s3(
            ticker,  ticker_folder)
        created_fpath = create_missing_files(ticker, ticker_folder, cik, years)
        s3_urls = upload_files_to_s3(created_fpath, existing_s3_urls)

        s3_urls_to_send_to_user = filter_s3_urls_to_send(
            s3_urls, raw_files_to_send, merged_files_to_send)

        return {"s3_urls": s3_urls_to_send_to_user}


def create_missing_files(ticker, ticker_folder, cik, years):

    existing_years = get_existing_years(ticker_folder)
    missing_years = [year for year in years if year not in existing_years]

    if missing_years:
        for year in missing_years:
            year_folder = os.path.join(ticker_folder, year)
            os.makedirs(year_folder, exist_ok=True)

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
        merged_fpaths = merge_excel_files_across_years(
            ticker, ticker_folder, local_years)

    raw_fpaths = get_fpaths_from_local_ticker(ticker_folder, local_years)
    created_fpath = raw_fpaths + merged_fpaths

    return created_fpath
