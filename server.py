import os

from fastapi import FastAPI

from download_10k import main as download_and_parse
from read_local_files import (ticker_already_downloaded,
                              get_fpaths_from_local_ticker)

MAIN_FOLDER = "MAIN_FOLDER"
os.makedirs(MAIN_FOLDER, exist_ok=True)

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ticker/{ticker}")
def download_10k(ticker):

    if not ticker_already_downloaded(ticker, MAIN_FOLDER):
        download_and_parse(ticker, MAIN_FOLDER)

    ticker_fpaths = get_fpaths_from_local_ticker(ticker, MAIN_FOLDER)

    return {"input": ticker_fpaths}
