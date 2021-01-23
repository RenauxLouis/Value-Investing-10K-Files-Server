import os
from fastapi import FastAPI

from download_10k_utils import (download_from_sec, clean_excel,
                                merge_excel_files_across_years,
                                is_valid_ticker, get_missing_years,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker)
from constants import DEFAULT_FOLDER

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ticker/{ticker}/years/{years}")
def download_10k(ticker, years):

    # TODO write function
    assert is_valid_ticker(ticker)

    ticker_folder = os.path.join(DEFAULT_FOLDER, ticker)
    os.makedirs(ticker_folder, exist_ok=True)
    start_year, end_year = years.split("-")
    years = [str(year) for year in range(int(start_year), int(end_year) + 1)]

    missing_years = get_missing_years(ticker_folder, years)
    if missing_years:
        excel_fpaths_to_clean = download_from_sec(ticker, missing_years,
                                                  ticker_folder)
        for excel_fpath in excel_fpaths_to_clean:
            clean_excel(excel_fpath)

    existing_merged_fpaths = get_existing_merged_fpaths(ticker_folder, years)
    if len(existing_merged_fpaths) == 3:
        merged_fpaths = existing_merged_fpaths
    else:
        merged_fpaths = merge_excel_files_across_years(ticker_folder, years)

    raw_fpaths_to_send = get_fpaths_from_local_ticker(ticker_folder, years)
    fpaths_to_send = raw_fpaths_to_send + merged_fpaths

    return {"fpaths": fpaths_to_send}
