import os
import re
import sys
from collections import defaultdict
from functools import reduce

import pandas as pd
from bs4 import BeautifulSoup
from pandas import ExcelWriter, merge, read_excel
from requests import get

from constants import (_10K_FILING_TYPE, BASE_EDGAR_URL, BASE_URL, CIK_URL,
                       MAP_SEC_PREFIX, MAP_SEC_REGEX, TICKER_CIK_CSV_FPATH,
                       PROXY_STATEMENT_FILING_TYPE, REGEX_PER_TARGET_SHEET,
                       SEC_CIK_TXT_URL)


def download_from_sec(ticker, cik, years, ticker_folder):

    _10k_url_per_year = get_10k_urls_per_year(
        filing_type=_10K_FILING_TYPE, years=years, cik=cik)
    proxy_statements_url_per_year = get_10k_urls_per_year(
        filing_type=PROXY_STATEMENT_FILING_TYPE, years=years, cik=cik)

    excel_fpaths = []
    for year, url in _10k_url_per_year.items():
        year_folder = os.path.join(ticker_folder, year)
        os.makedirs(year_folder, exist_ok=True)

        accession_numbers = [url.split("/")[-2]]

        download_file_from_url(ticker, cik, year, accession_numbers, ".htm",
                               _10K_FILING_TYPE, year_folder)
        excel_fpath = download_file_from_url(
            ticker, cik, year, accession_numbers, ".xlsx", _10K_FILING_TYPE,
            year_folder)
        excel_fpaths.append(excel_fpath)

    for year, url in proxy_statements_url_per_year.items():
        year_folder = os.path.join(ticker_folder, year)
        os.makedirs(year_folder, exist_ok=True)

        accession_numbers = [url.split("/")[-2]]

        download_file_from_url(ticker, cik, year, accession_numbers, ".htm",
                               PROXY_STATEMENT_FILING_TYPE, year_folder)

    return excel_fpaths


def download_file_from_url(ticker, cik, year, accession_numbers,
                           ext, file_type, local_fpath):

    if ext == ".xlsx":
        regex = ("financial_report", "financial_report")
    else:
        regex = MAP_SEC_REGEX[file_type]

    prefix = MAP_SEC_PREFIX[file_type]

    full_url = get_files_url(cik, accession_numbers, ext, *regex)
    r = get(full_url[0])
    status_code = r.status_code
    if status_code == 200:
        fpath = os.path.join(
            local_fpath, f"{ticker.upper()}_{prefix}_{year}{ext}")
        with open(fpath, "wb") as output:
            output.write(r.content)
    else:
        raise Exception(f"Wrong status code: {status_code}")

    return fpath


def get_10k_urls_per_year(filing_type, years, cik):

    current_year = years[-1]
    current_year_param = current_year + "1231"
    number_years_to_pull = len(years)

    params = {"action": "getcompany", "owner": "exclude",
              "output": "xml", "CIK": cik, "type": filing_type,
              "dateb": current_year_param, "count": number_years_to_pull}
    r = get(BASE_URL, params=params)
    if r.status_code != 200:
        sys.exit("Ticker data not found when pulling filing_type: "
                 f"{filing_type}")

    data = r.text
    soup = BeautifulSoup(data, features="lxml")

    urls = [link.string for link in soup.find_all("filinghref")]
    types = [link.string for link in soup.find_all("type")]
    dates_filed = [link.string for link in soup.find_all("datefiled")]
    assert len(urls) == len(types) == len(dates_filed)

    urls_per_year = {}
    for i, file_type in enumerate(types):
        if file_type == filing_type:
            year = dates_filed[i].split("-")[0]
            urls_per_year[year] = urls[i]

    assert set(years).issubset(set(urls_per_year.keys()))
    urls_per_year = {k: v for k, v in urls_per_year.items() if k in years}

    return urls_per_year


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


def merge_excel_files_across_years(ticker_folder, years):

    merged_fnames_map = get_merged_fnames_map(years)
    excel_fpath_per_year = get_local_excel_fpath_per_year(ticker_folder, years)
    sheet_per_year_per_target = get_sheets_per_year_per_target(
        excel_fpath_per_year)

    merged_fpaths = []
    for target, sheet_per_year in sheet_per_year_per_target.items():

        merged_fpath = os.path.join(ticker_folder, merged_fnames_map[target])
        merged_fpaths.append(merged_fpath)
        with ExcelWriter(merged_fpath, engine="xlsxwriter") as writer:
            workbook = writer.book
            dollar_format = workbook.add_format({"num_format": "$#,##0.00"})

            for year, sheet in sheet_per_year.items():
                sheet_name = year
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

    return merged_fpaths


def get_sheets_per_year_per_target(excel_fpath_per_year):

    sheet_per_year_per_target = defaultdict(dict)
    for year, excel_fpath in excel_fpath_per_year.items():
        df_per_target = pd.read_excel(excel_fpath, sheet_name=None)
        for target, df in df_per_target.items():
            sheet_per_year_per_target[target][year] = df

    return sheet_per_year_per_target


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

        for year_i in [str(int(year) + 1), year]:
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
            if year in col:
                break
        else:
            drop_col.append(col)
    merged_df = merged_df.drop(columns=drop_col)

    merged_df = merged_df.drop_duplicates()

    years_as_int = [int(year) for year in sheet_per_year.keys()]
    last_year = max(years_as_int)
    first_year = min(years_as_int)
    merged_sheet_name = str(last_year) + "-" + str(first_year)
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


def get_ticker_cik(ticker):

    ticker_lower = ticker.lower()
    ticker_cik_df = pd.read_csv(TICKER_CIK_CSV_FPATH)
    if not any(ticker_cik_df.ticker.str.contains(ticker_lower)):
        ticker_cik_df = update_ticker_cik_df()
        if not any(ticker_cik_df.ticker.str.contains(ticker_lower)):
            return None

    ciks = ticker_cik_df.loc[
        ticker_cik_df["ticker"] == ticker_lower]["cik"].values
    assert len(ciks) == 1, ciks
    cik = str(ciks[0])

    return cik


def update_ticker_cik_df():

    r = get(SEC_CIK_TXT_URL)
    content = r.content.decode("utf-8")
    rows = [line.split("\t") for line in content.splitlines()]
    df = pd.DataFrame(rows, columns=["ticker", "cik"])
    df.to_csv(TICKER_CIK_CSV_FPATH)

    return df


def get_missing_years(ticker_folder, years):

    if not os.path.exists(ticker_folder):
        return years

    years_folders = os.listdir(ticker_folder)
    missing_years = [year for year in years if year not in years_folders]

    return missing_years


def get_fpaths_from_local_ticker(ticker_folder, years):

    raw_fpaths = []
    ticker_subfolders = os.listdir(ticker_folder)

    for year in years:
        if year in ticker_subfolders:
            year_folder = os.path.join(ticker_folder, year)
            year_fnames = os.listdir(year_folder)
            year_fpaths = [os.path.join(year_folder, fname)
                           for fname in year_fnames]
            raw_fpaths.extend(year_fpaths)

    return raw_fpaths


def get_existing_merged_fpaths(ticker_folder, years):

    merged_fnames_map = get_merged_fnames_map(years)
    merged_fpaths = [os.path.join(ticker_folder, fname)
                     for fname in merged_fnames_map.values()]
    existing_merged_fpaths = [fpath for fpath in merged_fpaths
                              if os.path.exists(fpath)]

    return existing_merged_fpaths


def get_merged_fnames_map(years):

    first_year = years[0]
    last_year = years[-1]
    merged_fnames_map = {
        "balance sheet": f"Balance Sheet {first_year}-{last_year}.xlsx",
        "income": f"Income Statement {first_year}-{last_year}.xlsx",
        "cash": f"Cash Flow {first_year}-{last_year}.xlsx",
    }
    return merged_fnames_map


def get_local_excel_fpath_per_year(ticker_folder, years):

    local_excel_fpath_per_year = {}
    for year in years:
        year_folder = os.path.join(ticker_folder, year)
        excel_fnames = [fname for fname in os.listdir(year_folder)
                        if os.path.splitext(fname)[1] == ".xlsx"]
        assert len(excel_fnames) == 1
        excel_fpath = os.path.join(year_folder, excel_fnames[0])
        local_excel_fpath_per_year[year] = excel_fpath

    return local_excel_fpath_per_year


def clean_excel(excel_fpath):

    df_per_sheet = read_excel(excel_fpath, sheet_name=None)

    sheet_name_per_title = {}
    titles = []
    for sheet_name, df in df_per_sheet.items():
        title = df.columns[0].lower()
        titles.append(title)
        sheet_name_per_title[title] = sheet_name

    with pd.ExcelWriter(excel_fpath) as writer:
        for target_sheet_name in ["balance sheet", "income", "cash"]:
            target_regex = REGEX_PER_TARGET_SHEET[target_sheet_name]
            target_sheet_title = get_first_matching(titles,
                                                    target_regex)
            df = df_per_sheet[sheet_name_per_title[target_sheet_title]]
            df.to_excel(writer, sheet_name=target_sheet_name,
                        index=False)

    return


def get_first_matching(titles, targets):

    for title in titles:
        if any(target in title for target in targets):
            return title

    return title
