import argparse
import datetime
import os
import re
import sys
from collections import defaultdict
from functools import reduce
from shutil import rmtree

from pandas import read_excel, merge, ExcelWriter
import pandas as pd
from requests import get
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "http://www.sec.gov/cgi-bin/browse-edgar"
BASE_EDGAR_URL = "https://www.sec.gov/Archives/edgar/data"


def download_10k(ticker, cik, priorto, years, dl_folder):

    # TODO: Allow for specific year selection
    count = 5
    ext = "htm"

    fname_per_type_per_year = defaultdict(dict)
    ticker_folder = os.path.join(dl_folder, ticker)
    if os.path.exists(ticker_folder):
        rmtree(ticker_folder)

    os.makedirs(ticker_folder)

    filing_type = "10-K"
    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": priorto, "count": count}
    r = get(BASE_URL, params=params)
    if r.status_code != 200:
        sys.exit("Ticker data not found")
    else:
        data = r.text
        soup = BeautifulSoup(data, features="lxml")

        urls = [link.string for link in soup.find_all(
            "filinghref")]
        types = [link.string for link in soup.find_all(
            "type")]

        current_year = years[-1]
        urls_per_year = defaultdict(dict)
        N_10k = 0
        for type_file, url in zip(types, urls):
            if N_10k == 5:
                break
            urls_per_year[current_year][type_file] = url

            if type_file == "10-K":
                current_year -= 1
                N_10k += 1

    filing_type = "DEF 14A"
    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": priorto, "count": count}
    r = get(BASE_URL, params=params)
    if r.status_code != 200:
        sys.exit("Ticker data not found")
    else:
        data = r.text
        soup = BeautifulSoup(data, features="lxml")
        urls = [link.string for link in soup.find_all(
            "filinghref")]

        current_year = years[-1]
        for url in urls[:5]:
            urls_per_year[current_year][filing_type] = url
            current_year -= 1

    map_regex = {
        "10-K": ("10-k", "10k"),
        "10-K/A": ("htm", "10-ka"),
        "DEF 14A": ("", "")
    }
    map_prefix = {
        "10-K": "10K",
        "10-K/A": "10K_amended",
        "DEF 14A": "Proxy_Statement"
    }
    for year, urls in urls_per_year.items():
        for file_type, url in urls.items():
            file_type = file_type.replace("T", "")
            prefix = map_prefix[file_type]
            accession_numbers = [url.split("/")[-2]]
            full_url = get_files_url(cik, accession_numbers,
                                     ".htm", *map_regex[file_type])

            r = get(full_url[0])
            if r.status_code == 200:
                os.makedirs(ticker_folder, exist_ok=True)
                fpath = os.path.join(
                    ticker_folder,
                    f"{ticker.upper()}_{prefix}_{year}.{ext}")
                fname_per_type_per_year[year][file_type] = fpath
                with open(fpath, "wb") as output:
                    output.write(r.content)

            if file_type == "10-K":
                full_url = get_files_url(
                    cik, accession_numbers, ".xlsx", "financial_report",
                    "financial_report")
                r = get(full_url[0])
                if r.status_code == 200:
                    os.makedirs(ticker_folder, exist_ok=True)
                    fpath = os.path.join(
                        ticker_folder,
                        f"{ticker.upper()}_{prefix}_{year}.xlsx")
                    fname_per_type_per_year[year]["xlsx"] = fpath
                    with open(fpath, "wb") as output:
                        output.write(r.content)

    return fname_per_type_per_year


def get_files_url(cik, accession_numbers, ext, if_1, if_2):

    return_urls = []
    for accession_number in accession_numbers:
        accession_number_url = os.path.join(
            BASE_EDGAR_URL, cik, accession_number).replace("\\", "/")
        with get(accession_number_url) as r:
            if r.status_code == 200:
                data = r.text
                soup = BeautifulSoup(data, features="lxml")
                links = [link.get("href") for link in soup.findAll("a")]
                corresponding_file_extension = [link for link in links if (
                    os.path.splitext(link)[-1] == ext)]
                urls = [link for link in corresponding_file_extension if (
                    if_1 in link.lower() or if_2 in link.lower())]
                urls_accession_num = [
                    url for url in urls if accession_number in url]
                # TODO
                # Find better method to pick the correct file (can't get cik V)
                if urls_accession_num == []:
                    # Get first htm url with accession_number
                    urls_accession_num = [link for link in links if (
                        os.path.splitext(link)[
                            -1] == ext and accession_number in link)]
                fname = os.path.basename(urls_accession_num[0])
                url = os.path.join(accession_number_url, fname).replace(
                    "\\", "/")
                return_urls.append(url)
                # return_urls.append("check_amended")
            else:
                print("Error when request:", r.status_code)
    return return_urls


def get_cik(ticker):

    URL = "http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner"
    "=exclude&action=getcompany"
    CIK_RE = re.compile(r".*CIK=(\d{10}).*")

    f = get(URL.format(ticker), stream=True)
    results = CIK_RE.findall(f.text)
    if len(results):
        cik = str(results[0])
    else:
        raise Exception("Failed finding ticker")

    return cik


def parse_sheets(fname_per_type_per_year):

    sheet_per_year_target = defaultdict(dict)
    for year, fname_per_type in fname_per_type_per_year.items():

        xlsx_fpath = fname_per_type["xlsx"]
        df_10k_per_sheet = read_excel(xlsx_fpath, sheet_name=None)

        target_list = ["cash", "balance sheet"]
        target_income_statement = ["income", "earning", "operation"]
        for sheet, df in df_10k_per_sheet.items():
            title = df.columns[0].lower()
            for target in target_list:
                if target in title:
                    sheet_per_year_target[target][year] = df
                    target_list.remove(target)
                    break

            for target in target_income_statement:
                if target in title:
                    sheet_per_year_target["income"][year] = df
                    target_income_statement = []
                    break
            if target_list == target_income_statement:
                break

    return sheet_per_year_target


def merge_sheet_across_years(ticker, sheet_per_year_per_target,
                             dl_folder_fpath, years):

    first_year = years[0]
    last_year = years[-1]
    map_sheet_name = {
        "balance sheet": f"Balance Sheet {first_year}-{last_year}.xlsx",
        "cash": f"Cash Flow {first_year}-{last_year}.xlsx",
        "income": f"Income Statement {first_year}-{last_year}.xlsx",
    }
    for target, sheet_per_year in sheet_per_year_per_target.items():
        fpath = os.path.join(dl_folder_fpath, ticker,
                             map_sheet_name[target])

        with ExcelWriter(fpath, engine="xlsxwriter") as writer:
            workbook = writer.book
            # Format to $ cells
            format1 = workbook.add_format({"num_format": "$#,##0.00"})

            for year, sheet in sheet_per_year.items():
                sheet_name = str(year)
                clean_columns = [col.replace(
                    "Unnamed: ", "") for col in sheet.columns]

                sheet = sheet.rename(columns=dict(
                    zip(sheet.columns, clean_columns)))
                sheet.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]
                worksheet.set_column(1, 10, cell_format=format1)
                # Adjust columns
                for idx, col in enumerate(sheet):

                    series = sheet[col]
                    max_len = max((
                        # len of largest item
                        series.astype(str).map(len).max(),
                        len(str(series.name))  # len of column name/header
                    )) + 1  # adding a little extra space

                    # set column width
                    # print("count", series.isna().sum())
                    if (series.isna().sum() / len(series)) > 0.66:
                        default_max_length = 12
                    else:
                        default_max_length = 68
                    max_len = min(max_len, default_max_length)
                    worksheet.set_column(idx, idx, max_len)

            create_merged_df(sheet_per_year, writer, format1)


def clean_columns_df(sheet_per_year):

    # Put years in columns if in first row
    # for sheet, df in sheet_per_year.items():
    for year, df in sheet_per_year.items():
        title = df.columns[0]

        # Kill the columns of X month ended X < 12
        columns_to_keep = []
        for column in df.columns:
            clean_col = column.lower(
            )[:-1] if column.lower()[-1] == "s" else column.lower()
            if "month" in clean_col:
                months_duration = int(
                    "".join([char for char in column if char.isdigit()]))
                if months_duration == 12:
                    columns_to_keep.append(column)

        if columns_to_keep:
            df = df[[title, *columns_to_keep]]

        for year_i in [str(int(year) + 1), str(year)]:
            r = re.compile(".*" + year_i)
            year_col_list = list(
                filter(r.match, df.columns))
            if year_col_list:
                cleaned_df = df[[title, year_col_list[0]]]
                sheet_per_year[year] = cleaned_df
                break
            else:
                df_no_year_col_list = df.copy()
                df_no_year_col_list.iloc[0] = df_no_year_col_list.iloc[
                    0].fillna("")
                first_row = [str(
                    value) for value in df_no_year_col_list.iloc[0].values[1:]]
                year_first_row = list(
                    filter(r.match, first_row))
                if year_first_row:
                    new_columns = [title] + list(first_row)
                    columns_renaming = dict(zip(df.columns, new_columns))
                    cleaned_df = df_no_year_col_list.rename(
                        columns=columns_renaming)
                    cleaned_df = cleaned_df[[title, year_first_row[0]]]
                    sheet_per_year[year] = cleaned_df
                    break
    return sheet_per_year


def create_merged_df(sheet_per_year, writer, format1):

    # Clean columns of all sheets
    sheet_per_year = clean_columns_df(sheet_per_year)
    merged_df = reduce(
        lambda left, right: merge(
            left, right, left_on=left.columns[0],
            right_on=right.columns[0], how="outer"),
        list(sheet_per_year.values()))

    # Keep one column per year
    clean_cols = []
    drop_col = []
    for col in merged_df.columns:
        if col[-2:] == "_x":
            clean_cols.append(col[:-2])
        elif col[-2:] == "_y":
            clean_cols.append(col)
            drop_col.append(col)
        else:
            clean_cols.append(col)
    merged_df = merged_df.rename(columns=dict(
        zip(merged_df.columns, clean_cols)))
    merged_df = merged_df.drop(columns=drop_col)

    # Drop columns not in years range
    drop_col = []
    years = sheet_per_year.keys()
    for col in merged_df.columns[1:]:
        for year in years:
            if str(year) in col:
                break
        else:
            drop_col.append(col)
    merged_df = merged_df.drop(columns=drop_col)

    merged_df = merged_df.drop_duplicates()

    merged_sheet_name = str(
        max(sheet_per_year.keys())) + "-" + str(
            min(sheet_per_year.keys()))
    merged_df.to_excel(
        writer, sheet_name=merged_sheet_name, index=False)

    worksheet = writer.sheets[merged_sheet_name]
    worksheet.set_column(1, 10, cell_format=format1)

    # Adjust columns
    for idx, col in enumerate(merged_df):
        series = merged_df[col]
        max_len = max((
            # len of largest item
            series.astype(str).map(len).max(),
            len(str(series.name))  # len of column name/header
        )) + 1  # adding a little extra space
        # set column width
        worksheet.set_column(idx, idx, max_len)


def remove_temp_files(fname_per_type_per_year_per_ticker):
    for ticker, fname_per_type_per_year in (
            fname_per_type_per_year_per_ticker.items()):
        for year, fname_per_type in fname_per_type_per_year.items():
            os.remove(fname_per_type["xlsx"])


def download_and_parse(ticker, dl_folder_fpath):

    # diff_df = pd.read_csv("diff.csv")
    # diff_df["market_cap"] = diff_df["y_true"] * diff_df["current_volume"]
    # diff_df = diff_df.sort_values("market_cap", ascending=False)
    # diff_df = diff_df.dropna(subset=['Company'])
    # tickers = diff_df["Company"].values[:1000]

    os.makedirs(dl_folder_fpath, exist_ok=True)
    print(f"Parsing the last 5 10K documents from ticker: {ticker}")
    cik = get_cik(ticker)

    priorto = datetime.datetime.today().strftime("%Y%m%d")

    last_year = int(priorto[:4]) - 1
    years = range(last_year-4, last_year+1)

    fname_per_type_per_year = download_10k(ticker, cik, priorto, years,
                                           dl_folder_fpath)

    sheet_per_year_target = parse_sheets(fname_per_type_per_year)

    merge_sheet_across_years(ticker, sheet_per_year_target,
                             dl_folder_fpath, years)

    # remove_temp_files(fname_per_type_per_year_per_ticker)


def parse_args():
    # Parse command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str)
    parser.add_argument("--dl_folder_fpath", default="default_dl", type=str)
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    args = parse_args()
    download_and_parse(args.tickers, args.dl_folder_fpath)
