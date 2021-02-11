#!/usr/local/bin/
import pandas as pd
import numpy as np
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
import json
import os
import glob
from data_stats import get_file_list, json_to_df, JSON_COLUMNS, HEADERS, TO_DROP, get_test_file
from set_datatypes import drop_cols_and_rows, convert_and_set_dtypes, set_dtypes

####################################################
# Or maybe this is the one that breaks the computer:
def get_all_bc():
    dfs = []
    file_list = get_file_list()
    for f in file_list:
        df = json_to_df(f)
        dfs.append(df)
    return dfs

# This seems to break the computer:
def combine_dfs(df_list):
    all_data = pd.concat(df_list)
    all_data.columns = JSON_COLUMNS
    return all_data 
######################################

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
    #df = df.rename(columns={0: "download_date", 1: "service_date"})
    #datemap = df.to_dict()
    #df["download_date"] = df["download_date"].dt.date
    #datemap = dict(zip(df.download_date, df.service_date))
    datemap = dict(zip(df.download_date, df.service_date))
    for item in datemap:
        #print("Key : {}, Value : {}".format(item, datemap[item]))
        #print(item, ":", datemap[item])
        print(type(item))
    #print(datemap[2021-01-17])
    
    #print(df)
    #print(datemap)

#def get_stats(df, headers, selected, stat_file):
def get_stats(df, stat_file):
    '''
    df.columns = headers
    df = df[selected] 
    #print( df.describe())
    '''
    f = open(stat_file, "a")
    #print(df['OPD_DATE'][0], file=f)
    print(df.describe(), file=f)
    f.close()

#def get_all_stats(file_list, all_headers, selected_headers, stat_file):
def get_all_stats(file_list, json_columns, to_drop, stat_file):
    for f in file_list:
        '''
        df = json_to_df(f, JSON_COLUMNS)
        get_stats(df, all_headers, selected_headers, stat_file)
        get_max(df, all_headers, selected_headers, stat_file)
        '''
        df = json_to_df(f, json_columns)
        df = drop_cols_and_rows(df, to_drop)
        df = convert_and_set_dtypes(df)
        get_stats(df, stat_file)
        get_max(df, stat_file)
    

#def get_max(df, headers, columns, stat_file):
def get_max(df, stat_file):
    '''
    df.columns = headers
    df = df[columns]
    '''
    f = open(stat_file, "a")
    print(df.max(), file=f)
    print("---------------------------------------------------", file=f)
    f.close()

    '''
    print(df.max())
    print(df.dtypes)
    '''

def get_max_max(file_list, headers, column):
    for d in file_list:
        pass

#map_dates()
datafile = get_test_file("2021_02_10") 
datafile = "/home/jemerson/wopr/test/2021_02_06.json"
file_list = [datafile]
'''
df = json_to_df(datafile, JSON_COLUMNS) 
df = drop_cols_and_rows(df, TO_DROP)
df = convert_and_set_dtypes(df)
'''
stat_file = "stat_test.txt"
file_list = get_file_list()
#get_all_stats(file_list, JSON_COLUMNS, HEADERS[0:8], stat_file)
get_all_stats(file_list, JSON_COLUMNS, TO_DROP, stat_file)
#print(file_list)


#get_max(df, JSON_COLUMNS, HEADERS[0:8], "test.txt")
#get_stats(df, JSON_COLUMNS, HEADERS[0:8], stat_file)
#get_max(df, JSON_COLUMNS, HEADERS[0:8])

