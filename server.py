import os
from tempfile import TemporaryDirectory

from fastapi import FastAPI
from fastapi_profiler.profiler_middleware import PyInstrumentProfilerMiddleware

from constants import DEFAULT_FOLDER
from download_10k_utils import (clean_excel, download_from_sec,
                                download_ticker_folder_from_s3,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker,
                                get_missing_years, get_ticker_cik,
                                merge_excel_files_across_years,
                                upload_files_to_s3)

app = FastAPI()
app.add_middleware(PyInstrumentProfilerMiddleware)


@app.get("/")
def read_root():
    return {"Hello": "World"}


expected_fpaths_to_send = {
    for year in 2018-2020:
        proxy statement
        10k htm
    Balance Sheet 2018 2020
    Income 2018 2020
    Cash 2018 2020
}

@app.get("/params/")
async def download_10k(ticker, get10k, getProxyStatement, getXlsx,
                       getBalanceSheet, getIncomeStatement,
                       getCashFlowStatement, years):

    raw_files_to_send, merged_files_to_send, years = parse_inputs(
        getXlsx, get10k, getProxyStatement, getBalanceSheet,
        getIncomeStatement, getCashFlowStatement, years)

    sec_downloader = SECDownloader(ticker, years,raw_files_to_send,
                                   merged_files_to_send)

    with TemporaryDirectory() as dirpath:
        ticker_folder = os.path.join(dirpath, DEFAULT_FOLDER, ticker)
        os.makedirs(ticker_folder, exist_ok=True)

        existing_s3_urls = download_ticker_folder_from_s3(ticker,
                                                          ticker_folder)
        missing_years = get_missing_years(ticker_folder, years)
        if missing_years:
            excel_fpaths_to_clean = download_from_sec(
                ticker, cik, missing_years, ticker_folder, raw_files_to_send)
            for excel_fpath in excel_fpaths_to_clean:
                clean_excel(excel_fpath, merged_files_to_send)

        existing_merged_fpaths = get_existing_merged_fpaths(
            ticker, ticker_folder, years)
        if len(existing_merged_fpaths) == n_merged_files_to_send:
            merged_fpaths_to_send = existing_merged_fpaths
        else:
            merged_fpaths_to_send = merge_excel_files_across_years(
                ticker, ticker_folder, years)

        # TODO: Don't send the raw xlsx file
        raw_fpaths_to_send = get_fpaths_from_local_ticker(
            ticker_folder,  years)
        fpaths_to_send_to_user = raw_fpaths_to_send + merged_fpaths_to_send

        s3_urls = upload_files_to_s3(fpaths_to_send_to_user, existing_s3_urls)

        return {"s3_urls": s3_urls}
