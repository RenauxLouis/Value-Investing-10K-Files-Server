import argparse
import datetime
import os
import re
import sys
from collections import defaultdict
from functools import reduce
from shutil import rmtree

from pandas import read_excel, merge, ExcelWriter
from requests import get
from bs4 import BeautifulSoup

from constants import (MAIN_FOLDER, BASE_URL, BASE_EDGAR_URL,
                       REGEX_PER_TARGET_SHEET)


def get_cik(ticker):

    url = "http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner"
    "=exclude&action=getcompany"
    cik_regex = re.compile(r".*CIK=(\d{10}).*")

    f = get(url.format(ticker), stream=True)
    results = cik_regex.findall(f.text)
    try:
        cik = str(results[0])
    except Exception:
        raise Exception("Failed finding ticker")

    return cik


def download_10k(ticker, cik, priorto, years, dl_folder):

    # TODO: Allow for specific year selection
    count = 5
    ext = "htm"

    fname_per_type_per_year = defaultdict(dict)
    ticker_folder = os.path.join(dl_folder, ticker)
    if os.path.exists(ticker_folder):
        rmtree(ticker_folder)

    # TODO: How to deal with half empty folders created
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
        year_folder = os.path.join(ticker_folder, str(year))
        os.makedirs(year_folder)
        for file_type, url in urls.items():
            file_type = file_type.replace("T", "")
            prefix = map_prefix[file_type]
            accession_numbers = [url.split("/")[-2]]
            full_url = get_files_url(cik, accession_numbers,
                                     ".htm", *map_regex[file_type])

            r = get(full_url[0])
            if r.status_code == 200:
                fpath = os.path.join(year_folder,
                                     f"{ticker.upper()}_{prefix}.{ext}")
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


def get_target_sheets_per_year(fname_per_type_per_year):

    target_sheets_per_year = defaultdict(dict)
    for year, fname_per_type in fname_per_type_per_year.items():

        df_per_sheet = read_excel(fname_per_type["xlsx"], sheet_name=None)

        sheet_name_per_title = {}
        titles = []
        for sheet_name, df in df_per_sheet.items():
            title = df.columns[0].lower()
            titles.append(title)
            sheet_name_per_title[title] = sheet_name

        for target_sheet in ["balance sheet", "income", "cash"]:
            target_regex = REGEX_PER_TARGET_SHEET[target_sheet]
            target_sheet_title = get_first_matching(titles, target_regex)
            df = df_per_sheet[sheet_name_per_title[target_sheet_title]]
            target_sheets_per_year[target_sheet][year] = df

    return target_sheets_per_year


def get_first_matching(titles, targets):

    for title in titles:
        if any(target in title for target in targets):
            return title

    return title


def merge_sheet_across_years(ticker, target_sheets_per_year,
                             dl_folder_fpath, years):

    first_year = years[0]
    last_year = years[-1]
    map_sheet_name = {
        "balance sheet": f"Balance Sheet {first_year}-{last_year}.xlsx",
        "income": f"Income Statement {first_year}-{last_year}.xlsx",
        "cash": f"Cash Flow {first_year}-{last_year}.xlsx",
    }

    for target, sheet_per_year in target_sheets_per_year.items():
        fpath = os.path.join(dl_folder_fpath, ticker,
                             map_sheet_name[target])

        with ExcelWriter(fpath, engine="xlsxwriter") as writer:
            workbook = writer.book
            dollar_format = workbook.add_format({"num_format": "$#,##0.00"})

            for year, sheet in sheet_per_year.items():
                sheet_name = str(year)
                clean_columns = [col.replace(
                    "Unnamed: ", "") for col in sheet.columns]

                sheet = sheet.rename(columns=dict(
                    zip(sheet.columns, clean_columns)))
                sheet.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]
                worksheet.set_column(1, 10, cell_format=dollar_format)
                # Adjust columns
                for idx, col in enumerate(sheet):

                    series = sheet[col]
                    max_len = max((
                        # len of largest item
                        series.astype(str).map(len).max(),
                        len(str(series.name))  # len of column name/header
                    )) + 1  # adding a little extra space

                    # set column width
                    if (series.isna().sum() / len(series)) > 0.66:
                        default_max_length = 12
                    else:
                        default_max_length = 68
                    max_len = min(max_len, default_max_length)
                    worksheet.set_column(idx, idx, max_len)

            create_merged_df(sheet_per_year, writer, dollar_format)


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


def remove_temp_files(fname_per_type_per_year):
    for year, fname_per_type in fname_per_type_per_year.items():
        os.remove(fname_per_type["xlsx"])


def main(ticker, years=None, dl_folder_fpath=MAIN_FOLDER):

    print(f"Parsing 10K documents from ticker: {ticker} from years {years}")

    os.makedirs(dl_folder_fpath, exist_ok=True)

    cik = get_cik(ticker)

    priorto = datetime.datetime.today().strftime("%Y%m%d")
    if years is None:
        print("Year not given, getting the last 5 years 10k files")
        last_year = int(priorto[:4]) - 1
        years = range(last_year-4, last_year+1)

    fname_per_type_per_year = download_10k(ticker, cik, priorto,
                                           years, dl_folder_fpath)

    target_sheets_per_year = get_target_sheets_per_year(
        fname_per_type_per_year)

    merge_sheet_across_years(ticker, target_sheets_per_year,
                             dl_folder_fpath, years)

    remove_temp_files(fname_per_type_per_year)
