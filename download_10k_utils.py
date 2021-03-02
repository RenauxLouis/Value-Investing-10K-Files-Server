import os
import re
from collections import defaultdict
from functools import reduce

import boto3
import pandas as pd
import requests
from pandas import ExcelWriter, merge, read_excel
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from constants import (DEFAULT_FOLDER,  REGEX_PER_TARGET_SHEET,
                       TICKERS_10K_S3_BUCKET)

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


def merge_excel_files_across_years(ticker, ticker_folder):

    years = get_xslx_years(ticker_folder)
    merged_fnames_map = get_merged_fnames_map(ticker, years)
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


def get_xslx_years(ticker_folder):
    content = os.listdir(ticker_folder)
    xslx_years = [item for item in content
                  if (os.path.isdir(os.path.join(ticker_folder, item))
                      and ".xslx" in os.listdir(item))]
    return xslx_years


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
    for idx, _ in enumerate(merged_df):
        series = merged_df.iloc[:, idx]
        max_len = max((
            # len of largest item
            series.astype(str).map(len).max(),
            len(str(series.name))  # len of column name/header
        )) + 1  # adding a little extra space
        # set column width
        worksheet.set_column(idx, idx, max_len)


def get_existing_years(ticker_folder):

    if os.path.exists(ticker_folder):
        ticker_folder_content = os.listdir(ticker_folder)
        years_folders = [item for item in ticker_folder_content
                         if os.path.isdir(os.path.join(ticker_folder, item))]
        return years_folders
    return []


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


def get_existing_merged_fpaths(ticker, ticker_folder, years):

    merged_fnames_map = get_merged_fnames_map(ticker, years)
    merged_fpaths = [os.path.join(ticker_folder, fname)
                     for fname in merged_fnames_map.values()]
    existing_merged_fpaths = [fpath for fpath in merged_fpaths
                              if os.path.exists(fpath)]

    return existing_merged_fpaths


def get_merged_fnames_map(ticker, years):

    first_year = years[0]
    last_year = years[-1]
    merged_fnames_map = {
        "balance sheet": f"{ticker} Balance Sheet {first_year}-{last_year}"
                         ".xlsx",
        "income": f"{ticker} Income Statement {first_year}-{last_year}.xlsx",
        "cash": f"{ticker} Cash Flow {first_year}-{last_year}.xlsx",
    }
    return merged_fnames_map


def get_local_excel_fpath_per_year(ticker_folder, years):

    local_excel_fpath_per_year = {}
    for year in years:
        year_folder = os.path.join(ticker_folder, year)
        excel_fnames = [fname for fname in os.listdir(year_folder)
                        if os.path.splitext(fname)[1] == ".xlsx"]
        assert len(excel_fnames) == 1, (
            f"Not a unique excel_fname for year {year} but {excel_fnames}")
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
        for target_sheet_name in REGEX_PER_TARGET_SHEET:
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


def upload_files_to_s3(created_fpaths, existing_s3_urls):

    s3_client = boto3.client("s3")

    s3_urls = []
    for fpath in created_fpaths:
        s3_prefix = fpath.split(DEFAULT_FOLDER + "/")[1]
        s3_url = os.path.join("s3://", TICKERS_10K_S3_BUCKET, s3_prefix)
        if s3_url not in existing_s3_urls:
            s3_client.upload_file(fpath, TICKERS_10K_S3_BUCKET, s3_prefix)

        s3_urls.append(s3_url)

    return s3_urls


def parse_inputs(get10k, getProxyStatement, getBalanceSheet,
                 getIncomeStatement, getCashFlowStatement, years):

    raw_files_to_send = {
        "10k": get10k == "true",
        "proxy": getProxyStatement == "true"
    }
    merged_files_to_send = {
        "balance": getBalanceSheet == "true",
        "income": getIncomeStatement == "true",
        "cash": getCashFlowStatement == "true",
    }
    start_year, end_year = years.split("-")
    years = [str(year) for year in range(int(start_year),
                                         int(end_year) + 1)]

    return raw_files_to_send, merged_files_to_send, years


def download_years_in_ticker_folder_from_s3(ticker, ticker_folder, years):

    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(TICKERS_10K_S3_BUCKET)
    s3_objects = bucket.objects.filter(Prefix=ticker)

    existing_s3_urls = []
    for s3_obj in s3_objects:
        s3_folder = s3_obj.key.split("/")[1]
        if s3_folder not in years:
            continue

        s3_url = os.path.join("s3://", s3_obj.bucket_name, s3_obj.key)
        existing_s3_urls.append(s3_url)

        target = os.path.join(ticker_folder, os.path.relpath(s3_obj.key,
                                                             ticker))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if s3_obj.key[-1] == "/":
            continue
        bucket.download_file(s3_obj.key, target)

    return existing_s3_urls


def filter_s3_urls_to_send(s3_urls_to_send_to_user, raw_files_to_send,
                           merged_files_to_send):

    raw_files_to_remove = [file_type for file_type, select in
                           raw_files_to_send.items() if not select]
    merged_files_to_remove = [file_type for file_type, select in
                              merged_files_to_send.items() if not select]

    for file_regex in raw_files_to_remove:
        s3_urls_to_send_to_user = [file for file in s3_urls_to_send_to_user
                                   if file_regex not in file.lower()]
    for file_regex in merged_files_to_remove:
        s3_urls_to_send_to_user = [file for file in s3_urls_to_send_to_user
                                   if file_regex not in file.lower()]

    return s3_urls_to_send_to_user
