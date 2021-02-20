import os
from tempfile import TemporaryDirectory

from fastapi import FastAPI
from fastapi_profiler.profiler_middleware import PyInstrumentProfilerMiddleware

from constants import DEFAULT_FOLDER
from download_10k_utils import (clean_excel,
                                download_ticker_folder_from_s3,
                                filter_fpaths_to_send,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker,
                                get_missing_years,
                                merge_excel_files_across_years, parse_inputs,
                                upload_files_to_s3)
from sec_downloader import SECDownloader, download

app = FastAPI()
app.add_middleware(PyInstrumentProfilerMiddleware)

sec_downloader = SECDownloader()


@app.get("/")
def read_root():
    return {"Hello": "World"}


# expected_fpaths_to_send = {
#     for year in 2018-2020:
#         proxy statement
#         10k htm
#     Balance Sheet 2018 2020
#     Income 2018 2020
#     Cash 2018 2020
# }

@app.get("/params/")
async def download_10k(ticker, years, _10k, Proxy, Balance, Income, Cash):

    raw_files_to_send, merged_files_to_send, years = parse_inputs(
        _10k, Proxy, Balance, Income, Cash, years)

    cik = sec_downloader.get_ticker_cik(ticker)

    with TemporaryDirectory() as dirpath:

        ticker_folder = os.path.join(dirpath, DEFAULT_FOLDER, ticker)
        os.makedirs(ticker_folder)

        existing_s3_urls = download_ticker_folder_from_s3(
            ticker,  ticker_folder)
        created_fpath = create_files(ticker, ticker_folder, cik, years)
        fpaths_to_send_to_user = filter_fpaths_to_send(
            created_fpath, raw_files_to_send, merged_files_to_send)
        s3_urls = upload_files_to_s3(fpaths_to_send_to_user, existing_s3_urls)

        return {"s3_urls": s3_urls}


def create_files(ticker, ticker_folder, cik, years):

    missing_years = get_missing_years(ticker_folder, years)
    if missing_years:
        excel_fpaths_to_clean = download(ticker, cik, missing_years,
                                         ticker_folder)
        for excel_fpath in excel_fpaths_to_clean:
            clean_excel(excel_fpath)

    existing_merged_fpaths = get_existing_merged_fpaths(
        ticker, ticker_folder, years)
    if len(existing_merged_fpaths) == 3:
        merged_fpaths = existing_merged_fpaths
    else:
        merged_fpaths = merge_excel_files_across_years(
            ticker, ticker_folder, years)

    raw_fpaths = get_fpaths_from_local_ticker(
        ticker_folder, years)
    created_fpath = raw_fpaths + merged_fpaths

    return created_fpath