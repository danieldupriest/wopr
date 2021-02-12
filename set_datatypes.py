#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP
from utilities import get_file_list, json_to_df
from transform import convert_to_mph 

# Clean the dataframe by dropping unneeded columns and rows with empty fields:
def drop_cols_and_rows(df, TO_DROP):
    # This is important bc dropna() does not drop rows with 1+ empty string.
    df = df.replace('', np.NaN)
    # Drop the unneeded columns (axis=1 specifies columns):
    df = df.drop(TO_DROP, axis=1)
    # Drop rows where any field has an empty string (axis=0 specifies rows):
    df = df.dropna(axis=0)
    return df

# Set the datatypes so we can work with the data more effectively:
def set_dtypes(df):
    df = df.astype({
            "EVENT_NO_TRIP": 'int64',
            "OPD_DATE": 'string', # This doesn't seem to work: remains object type
            "VEHICLE_ID": 'int64', # 'string',
            "ACT_TIME": 'int64',
            "VELOCITY": 'int64',
            "DIRECTION": 'int64',
            "GPS_LONGITUDE": 'float64',
            "GPS_LATITUDE": 'float64'
            })
    return df

# Seems like convert_dtypes is unnecessary middleman, but made it work:
def convert_and_set_dtypes(df):
    # First, convert all types from pandas object to string:
    df = df.convert_dtypes()
    # Then convert to the correct datatypes: 
    df = set_dtypes(df)
    return df

##### PROCESS DATETIME ########################################################
# Copy date and time to separate dataframes and convert to datetime dtype:
def date_time_to_dfs(df):
    datedf = df['OPD_DATE']
    timedf = df['ACT_TIME']
    datedf = pd.to_datetime(datedf, dayfirst=True, infer_datetime_format=True)
    timedf = pd.to_datetime(timedf, unit='s')
    # Is this still necessary? TODO Yes, for some reason yes.
    timedf = pd.Series(timedf.dt.time)
    return [datedf, timedf]

# Convert from series to dataframe and rename date / time columns: 
def build_date_and_time(datedf, timedf):
    d = pd.DataFrame(data=datedf)
    d = d.rename(columns={"OPD_DATE": "date"})
    t = pd.DataFrame(data=timedf)
    t = t.rename(columns={"ACT_TIME": "time"})
    return [d,t]

def concat_date_time(d, t):
    # Combine, aggregate, and separate the final datetime column.
    c = pd.concat([d,t], axis=1)
    # Now create a new dataframe column out of the date and time columns:
    c["tstamp"] = d["date"].astype(str) + " " + t["time"].astype(str)
    # Finally, we have a single column dataframe with datetime to work with:
    dt = c['tstamp']
    return dt

def convert_to_datetime(df, dt):
    # Drop the columns used to aggregate datetime:
    df.drop(['OPD_DATE', 'ACT_TIME'], axis=1, inplace=True)
    # Concatenate the processed date and time dataframes:
    df = pd.concat([df, dt], axis=1)
    # Finally convert dt dataframe to datetime dtype:
    df['tstamp'] = pd.to_datetime(df['tstamp'] )
    return df

##### END PROCESS DATETIME ####################################################

def main():
    # TODO
    # Replace this when we have system for batch processing bc in consumer.
    # Select the file(s) and put into dataframe:
    path = os.path.abspath('.')
    datafile = path + "/test/2021_02_06.json"
    df = json_to_df(datafile, JSON_COLUMNS)


    # Drop unneeded columns and rows with empty string; set datatypes:
    df = drop_cols_and_rows(df, TO_DROP)

    '''
    print(df.describe())
    print(df.dtypes)
    '''

    df = convert_and_set_dtypes(df)

    '''
    print(df.describe())
    print(df.dtypes)
    '''

    '''
    ##### Process datetime #####
    # Process the 2 columns that have date/time data:
    dfs = date_time_to_dfs(df)
    datedf = dfs[0] 
    timedf = dfs[1] 
    dt_list = build_date_and_time(datedf, timedf)
    d = dt_list[0]
    t = dt_list[1]

    # Combine the date and time fields and convert to datetime datatype:
    dt = concat_date_time(d, t)
    df = convert_to_datetime(df, dt)
    ##### End Process datetime #####

    # What happened to the top row? Suddenly missing a couple fields. TODO
    #I'm dropping it so it doesn't cause trouble later on:
    df = df.drop(df.index[0])

    print(df.head())
    print(df.dtypes)
    '''

if __name__ == '__main__':
    main()

