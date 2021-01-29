import os
from io import BytesIO
from fastapi import FastAPI, Response
import zipfile

from constants import DEFAULT_FOLDER
from download_10k_utils import (clean_excel, download_from_sec,
                                get_existing_merged_fpaths,
                                get_fpaths_from_local_ticker,
                                get_missing_years, get_ticker_cik,
                                merge_excel_files_across_years)
import shutil

app = FastAPI()


def zipfiles(fpaths):

    zip_subdir = "archive"
    zip_filename = "%s.zip" % zip_subdir

    # Open StringIO to grab in-memory ZIP contents
    s = BytesIO()
    # The zip compressor
    zf = zipfile.ZipFile(s, "w")

    for fpath in fpaths:
        # Calculate path for file in zip
        fdir, fname = os.path.split(fpath)
        zip_path = os.path.join(zip_subdir, fname)

        # Add file, at correct path
        zf.write(fpath, zip_path)

    # Must close zip for all contents to be written
    zf.close()

    # Grab ZIP file from in-memory, make response with correct MIME-type
    resp = Response(s.getvalue(), mimetype="application/x-zip-compressed")
    # ..and correct content-disposition
    resp["Content-Disposition"] = "attachment; filename=%s" % zip_filename

    return resp


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ticker/{ticker}/years/{years}")
def download_10k(ticker, years):

    cik = get_ticker_cik(ticker)
    if cik is None:
        return {"fpaths": "Wrong ticker"}

    ticker_folder = os.path.join(DEFAULT_FOLDER, ticker)
    os.makedirs(ticker_folder, exist_ok=True)
    start_year, end_year = years.split("-")
    years = [str(year) for year in range(int(start_year), int(end_year) + 1)]

    missing_years = get_missing_years(ticker_folder, years)
    if missing_years:
        excel_fpaths_to_clean = download_from_sec(ticker, cik, missing_years,
                                                  ticker_folder)
        for excel_fpath in excel_fpaths_to_clean:
            clean_excel(excel_fpath)

    existing_merged_fpaths = get_existing_merged_fpaths(ticker_folder, years)
    if len(existing_merged_fpaths) == 3:
        merged_fpaths = existing_merged_fpaths
    else:
        merged_fpaths = merge_excel_files_across_years(ticker_folder, years)

    # TODO: Don't send the raw xlsx file
    raw_fpaths_to_send = get_fpaths_from_local_ticker(ticker_folder, years)
    fpaths_to_send = raw_fpaths_to_send + merged_fpaths

    # return zipfiles(fpaths_to_send)

    shutil.make_archive(ticker_folder, "zip", ticker_folder)
    # headers = {
    #     "Content-Disposition": f'attachment; filename="{ticker_folder}.zip"'
    # }
    # output = "coucou"
    #   return StreamingResponse(output, headers=headers)

    resp = Response("", mimetype="application/x-zip-compressed")
    # ..and correct content-disposition
    resp["Content-Disposition"] = f'attachment; filename="{ticker_folder}.zip"'
    return resp

