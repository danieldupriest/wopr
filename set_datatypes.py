#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
from data_stats import get_file_list, json_to_df, JSON_COLUMNS, HEADERS, TO_DROP

# Clean up the dataframe by dropping unneeded columns and dropping rows with empty fields:
def drop_cols_and_rows(df, TO_DROP):
    #df.replace('', np.NaN, inplace=True) #TODO This doesn't matter if we dropna() later.
    df.drop(TO_DROP, axis=1, inplace=True)
    df.dropna(axis=0, inplace=True) #subset=["normalized-losses"],

# This seems to working except the OPD_DATE remains an object:
def set_dtypes(df):
    df = df.astype({
            "EVENT_NO_TRIP": 'int64',
            "OPD_DATE": 'string', # Remains object type
            "VEHICLE_ID": 'int64',
            "ACT_TIME": 'int64',
            "VELOCITY": 'int64',
            "DIRECTION": 'int64',
            "GPS_LONGITUDE": 'float64',
            "GPS_LATITUDE": 'float64'
            }, errors='ignore')
    return df

# convert_dtypes turns them all to strings, then set_dtypes sets correct type:
def convert_and_set_dtypes(df):
    df = df.convert_dtypes()
    df = set_dtypes(df)
    return df

##### The rest is about getting the datetime field and finally tacking it onto the main df #####

# Copy date and time to separate dataframes and convert to datetime dtype:
def date_time_to_dfs(df):
    datedf = df['OPD_DATE']
    timedf = df['ACT_TIME']
    datedf = pd.to_datetime(datedf, dayfirst=True, infer_datetime_format=True)
    timedf = pd.to_datetime(timedf, unit='s')
    timedf = pd.Series(timedf.dt.time) # Is this still necessary? TODO Yes, for some reason yes.
    return [datedf, timedf]

# Convert from series to df and rename date / time columns: 
def build_date_and_time(datedf, timedf):
    d = pd.DataFrame(data=datedf)
    d = d.rename(columns={"OPD_DATE": "date"})
    t = pd.DataFrame(data=timedf)
    t = t.rename(columns={"ACT_TIME": "time"})
    return [d,t]

# Combine, aggregate, and separate the final datetime column.
def concat_date_time(d, t):
    c = pd.concat([d,t], axis=1)
    c["tstamp"] = d["date"].astype(str) + " " + t["time"].astype(str)
    dt = c['tstamp']
    return dt

# Drop the columns used to aggregate datetime, concat, and covert dt to datetime dtype :
def convert_to_datetime(df, dt):
    df.drop(['OPD_DATE', 'ACT_TIME'], axis=1, inplace=True)
    df = pd.concat([df, dt], axis=1)
    df['tstamp'] = pd.to_datetime(df['tstamp'] )
    return df

def main():
    # Replace this when we have system for batch processing bc in consumer. TODO
    # Select the file(s) and put into dataframe:
    path = os.path.abspath('.')
    datafile = path + "/test/2021_02_06.json"
    df = json_to_df(datafile)

    drop_cols_and_rows(df, TO_DROP)
    df = convert_and_set_dtypes(df)
    #print(df.dtypes)

    ##### Process datetime #####
    dfs = date_time_to_dfs(df)
    datedf = dfs[0] 
    timedf = dfs[1] 

    dt_list = build_date_and_time(datedf, timedf)
    d = dt_list[0]
    t = dt_list[1]

    dt = concat_date_time(d, t)
    df = convert_to_datetime(df, dt)
    ##### End Process datetime #####

    # What happened to the top row? Suddenly missing a couple fields. TODO
    #I'm dropping it so it doesn't cause trouble later on:
    df = df.drop(df.index[0])
    print(df.head())
    print(df.dtypes)

if __name__ == '__main__':
    main()

