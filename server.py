import os

from fastapi import FastAPI

from download_10k import main as download_and_parse
from read_local_files import (get_missing_years,
                              get_fpaths_from_local_ticker)

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ticker/{ticker}/years/{years}")
def download_10k(ticker, years):

    start_year, end_year = years.split("-")
    years = range(int(start_year), int(end_year))

    missing_years = get_missing_years(ticker, years)
    if missing_years:
        download_and_parse(ticker, years)

    ticker_fpaths = get_fpaths_from_local_ticker(ticker)

    return {"input": ticker_fpaths}
