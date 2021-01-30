import os
from shutil import rmtree

from fastapi import FastAPI

from constants import DEFAULT_FOLDER, ZIP_FILES_FOLDER
from download_10k_utils import (clean_excel, download_from_sec,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker,
                                get_missing_years, get_ticker_cik,
                                merge_excel_files_across_years)

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ticker/{ticker}/years/{years}")
async def download_10k(ticker, years):

    rmtree(ZIP_FILES_FOLDER, ignore_errors=True)
    os.makedirs(ZIP_FILES_FOLDER)

    cik = get_ticker_cik(ticker)
    if cik is None:
        return {"fpaths": "Wrong ticker"}

    ticker_folder = os.path.join(DEFAULT_FOLDER, ticker)
    os.makedirs(ticker_folder, exist_ok=True)
    start_year, end_year = years.split("-")
    years = [str(year) for year in range(int(start_year), int(end_year) + 1)]

    missing_years = get_missing_years(ticker_folder, years)
    if missing_years:
        excel_fpaths_to_clean = download_from_sec(ticker, cik, missing_years,
                                                  ticker_folder)
        for excel_fpath in excel_fpaths_to_clean:
            clean_excel(excel_fpath)

    existing_merged_fpaths = get_existing_merged_fpaths(ticker_folder, years)
    if len(existing_merged_fpaths) == 3:
        merged_fpaths = existing_merged_fpaths
    else:
        merged_fpaths = merge_excel_files_across_years(ticker_folder, years)

    # TODO: Don't send the raw xlsx file
    raw_fpaths_to_upload_to_s3 = get_fpaths_from_local_ticker(
        ticker_folder,  years)
    fpaths_to_upload_to_s3 = raw_fpaths_to_upload_to_s3 + merged_fpaths

    s3_urls = upload_files_to_s3(fpaths_to_upload_to_s3)

    return {"s3_urls": s3_urls}
