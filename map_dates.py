#!/usr/local/bin/
import pandas as pd
import os
import glob
import datetime
    
# Use this form to convert a date:
#date = datetime.date(2021, 1, 17)
    
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


if __name__ == '__main__':
    map_dates()

