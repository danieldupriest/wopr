#!/usr/local/bin/
import pandas as pd
import numpy as np
import datetime

# Copy date and time to separate dataframes and convert to datetime dtype:
def date_time_to_dfs(df):
    datedf = df['date']
    timedf = df['arrive_time']
    datedf = pd.to_datetime(datedf, dayfirst=True, infer_datetime_format=True)
    timedf = pd.to_datetime(timedf, unit='s')
    # Is this still necessary? TODO Yes, for some reason yes.
    timedf = pd.Series(timedf.dt.time)
    return [datedf, timedf]

# Convert from series to dataframe and rename date / time columns: 
def build_date_and_time(datedf, timedf):
    d = pd.DataFrame(data=datedf)
    t = pd.DataFrame(data=timedf)
    return [d,t]

def concat_date_time(d, t):
    # Combine, aggregate, and separate the final datetime column.
    c = pd.concat([d,t], axis=1)
    # Now create a new dataframe column out of the date and time columns:
    c["tstamp"] = d["date"].astype(str) + " " + t["arrive_time"].astype(str)
    # Finally, we have a single column dataframe with datetime to work with:
    dt = c['tstamp']
    return dt

def convert_to_datetime(df, dt):
    # Drop the columns used to aggregate datetime:
    df.drop(['date','arrive_time'], axis=1, inplace=True)
    # Concatenate the processed date and time dataframes:
    df = pd.concat([df, dt], axis=1)
    # Finally convert dt dataframe to datetime dtype:
    df['tstamp'] = pd.to_datetime(df['tstamp'] )
    return df

