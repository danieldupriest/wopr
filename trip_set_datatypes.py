#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP
from utilities import get_file_list, json_to_df
from convert_datetime import date_time_to_dfs, build_date_and_time, concat_date_time, convert_to_datetime 
from transform import convert_to_mph 
from valdata import validate_no_null

# Clean the dataframe by dropping unneeded columns and rows with empty fields:
def drop_cols_and_rows(df, TO_DROP):
    # This is important bc dropna() does not drop rows with 1+ empty string.
    df = df.replace('', np.NaN)
    # Drop the unneeded columns (axis=1 specifies columns):
    df = df.drop(TO_DROP, axis=1)
    # Drop rows where any field has an empty string (axis=0 specifies rows):
    # TODO Maybe save the dropped bc to a list? Not sure how with dropna().
    df = df.dropna(axis=0)
    return df

# Set the datatypes so we can work with the data more effectively:
def trip_set_dtypes(df):
    df = df.astype({
        'vehicle_number': 'int64' ,
        'route_number': 'int64',
        'direction': 'string',
        'service_key': 'string',
        'trip_id': 'int64'
        'date' 'string':
        })
        '''
        "EVENT_NO_TRIP": 'int64',
        "OPD_DATE": 'string', # This doesn't seem to work: remains object type
        "VEHICLE_ID": 'int64', # 'string',
        "ACT_TIME": 'int64',
        "VELOCITY": 'int64',
        "DIRECTION": 'int64',
        "GPS_LONGITUDE": 'float64',
        "GPS_LATITUDE": 'float64'
        })
        '''
    return df

# Seems like convert_dtypes is unnecessary middleman, but made it work:
def convert_and_set_dtypes(df):
    # First, convert all types from pandas object to string:
    df = df.convert_dtypes()
    # Then convert to the correct datatypes: 
    df = set_dtypes(df)
    return df

# This should be renamed to maybe manage_bc() 
def set_datatypes(df):
    # TODO This is done in proc_html:
    # Drop unneeded columns and rows with empty string; set datatypes:
    #df = drop_cols_and_rows(df, TO_DROP)

    #TODO Initial data validation for existence here:
    errors = validate_no_null(df, SELECTED_HDRS[0:8]) 
    for error in errors:
        print(error)

    df = convert_and_set_dtypes(df)
    
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

    return df


def main():
    filelist = get_file_list()
    f = filelist[0] # Use the first JSON file for now
    f = "/home/jemerson/wopr/test/2021_02_06.json"
    df = json_to_df(f, JSON_COLUMNS) 

    set_datatypes(df)


if __name__ == '__main__':
    main()

