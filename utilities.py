#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
import glob
import datetime
#from datetime import date
from csv import writer
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP

def get_test_file(date):
    path = os.path.abspath('.')
    test_file = path + "/data/" + date + ".json"
    return test_file

# Create a list of data files in the /data directory:
def get_file_list():
    path = os.path.abspath('.')
    file_list = glob.glob(path + '/data/*')
    file_list.sort()
    return file_list
 
# Convert a JSON breadcrumb data file to a pandas dataframe:
def json_to_df(datafile, columns):
    with open(datafile, "r") as j: 
        data = json.load(j)
    df = pd.DataFrame(data)
    # Why does this not get saved and how can I make it so?: TODO
    df.columns = columns #JSON_COLUMNS
    return df

# Use this form to convert a date:
# date = datetime.date(2021, 1, 17)
def map_dates():
    #"OPD_DATE": "05-SEP-20",
    dates = []
    path = os.path.abspath('.')
    file_list = glob.glob(path + '/data/*')
    for f in file_list:
        f = f[25:35]
        dates.append(f)
    df = pd.DataFrame(dates)
    df = df.sort_values(by=0)
    df = df.reset_index(drop=True)
    df[0] = pd.to_datetime(df[0], format="%Y_%m_%d")
    df[1] = (df[0] + pd.to_timedelta(-115, unit="d"))
    df["download_date"] = df[0].dt.date
    df["service_date"] = df[1].dt.date
    datemap = dict(zip(df.download_date, df.service_date))
    return datemap


