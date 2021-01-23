import os

from constants import MAIN_FOLDER

def get_missing_years(ticker, years, folder=MAIN_FOLDER):


    # downloaded_tickers = os.listdir(folder)
    # return ticker in downloaded_tickers
    return True


def get_fpaths_from_local_ticker(ticker, folder=MAIN_FOLDER):
    downloaded_tickers = os.listdir(folder)
    assert ticker in downloaded_tickers

    ticker_folder = os.path.join(folder, ticker)

    ticker_fpaths = []
    for root, _, fnames in os.walk(ticker_folder):
        ticker_fpaths.extend([os.path.join(root, fname) for fname in fnames])

    return ticker_fpaths
