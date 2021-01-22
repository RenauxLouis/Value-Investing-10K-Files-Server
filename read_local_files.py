import os


def ticker_already_downloaded(ticker, folder):
    downloaded_tickers = os.listdir(folder)
    return ticker in downloaded_tickers


def get_fpaths_from_local_ticker(ticker, folder):
    downloaded_tickers = os.listdir(folder)
    assert ticker in downloaded_tickers

    ticker_folder = os.path.join(folder, ticker)
    ticker_files = os.listdir(ticker_folder)
    ticker_fpaths = [
        os.path.join(ticker_folder, file) for file in ticker_files]

    return ticker_fpaths
