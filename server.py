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


@app.get("/ticker/{ticker}/years/{years}")
async def download_10k(ticker, years):

    cik = get_ticker_cik(ticker)
    if cik is None:
        return {"s3_urls": "Wrong ticker"}

    with TemporaryDirectory() as dirpath:
        ticker_folder = os.path.join(dirpath, DEFAULT_FOLDER, ticker)
        os.makedirs(ticker_folder, exist_ok=True)
        start_year, end_year = years.split("-")
        years = [str(year) for year in range(int(start_year),
                                             int(end_year) + 1)]

        existing_s3_urls = download_ticker_folder_from_s3(ticker,
                                                          ticker_folder)
        missing_years = get_missing_years(ticker_folder, years)
        if missing_years:
            excel_fpaths_to_clean = download_from_sec(ticker, cik,
                                                      missing_years,
                                                      ticker_folder)
            for excel_fpath in excel_fpaths_to_clean:
                clean_excel(excel_fpath)

        existing_merged_fpaths = get_existing_merged_fpaths(ticker,
                                                            ticker_folder,
                                                            years)
        if len(existing_merged_fpaths) == 3:
            merged_fpaths = existing_merged_fpaths
        else:
            merged_fpaths = merge_excel_files_across_years(ticker,
                                                           ticker_folder,
                                                           years)

        # TODO: Don't send the raw xlsx file
        raw_fpaths_to_send_to_user = get_fpaths_from_local_ticker(
            ticker_folder,  years)
        fpaths_to_send_to_user = raw_fpaths_to_send_to_user + merged_fpaths

        s3_urls = upload_files_to_s3(fpaths_to_send_to_user, existing_s3_urls)

        return {"s3_urls": s3_urls}
