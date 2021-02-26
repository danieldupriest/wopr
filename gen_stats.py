#!/usr/local/bin/
import pandas as pd
import numpy as np
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
import json
import os
import csv
import re
import subprocess
import glob
from utilities import get_test_file, get_file_list, json_to_df
from set_datatypes import drop_cols_and_rows, convert_and_set_dtypes, set_dtypes, set_datatypes
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP, VEHICLE_IDS

def get_stats(df, stat_file):
    f = open(stat_file, "a")
    print(df.describe(), file=f)
    f.close()

def get_all_stats(file_list, json_columns, to_drop, stat_file):
    for f in file_list:
        df = json_to_df(f, json_columns)
        df = drop_cols_and_rows(df, to_drop)
        # The object data stats are more useful than stats after converting to numeric datatypes:
        get_stats(df, stat_file)
        df = convert_and_set_dtypes(df)
        get_min_max(df, stat_file)

def get_min_max(df, stat_file):
    f = open(stat_file, "a")
    print("MIN-----\n", df.max(), file=f)
    print("MAX-----\n", df.min(), file=f)
    print("---------------------------------------------------", file=f)
    f.close()

def get_value_counts(datafile, all_cols, to_drop, column, stat_file):
    df = json_to_df(datafile, all_cols)
    df = drop_cols_and_rows(df, to_drop)
    get_stats(df, stat_file)
    f = open(stat_file, "a")
    print(df[column].value_counts(), file=f)
    f.close()
    return df

def write_min_max(columns):
    stat_file = "/home/jemerson/wopr/stats/data_stats.txt"
    temp = "'/home/jemerson/wopr/stats/min_max_values.txt'"
    #os.system("rm " +  temp)
    for c in columns:
        stmt = "grep ^" + c + " " + stat_file + " >> " + temp
        print(stmt)
        os.system(stmt)

def write_veh_ID_list(columns):
    ID_file = "/home/jemerson/wopr/stats/vehicle_IDs.txt"
    vehicle_IDs = []
    filelist = get_file_list()
    for f in filelist:
        df = json_to_df(f, columns) 
        df = set_datatypes(df)
        IDs = df['VEHICLE_ID']
        for ID in IDs:
            if ID not in vehicle_IDs:
                vehicle_IDs.append(ID)
    
    f = open(ID_file, "w")
    print(vehicle_IDs, file=f)
    f.close()

def list_to_csv(outfile, data):
    data.sort()
    temp = []
    for d in data:
        d = str(d)
        item = d.split(', ')
        temp.append(item)

    with open(outfile, 'w') as f: 
        write = csv.writer(f) 
        for t in temp:
            write.writerow(t) 

def min_max_lists(infile, columns):
    # Using readlines()
    f = open(infile, 'r')
    lines = f.readlines()
    minmax = {}
     
    for col in columns:
        minmax[col] = []
        #print(col)
        for line in lines:
            #print(line)
            tofind = "^" + col + " (-?\d*(?:\.\d*)?)" 
            x = re.findall(tofind, line)
            #print(x)
            if len(x) != 0:
                minmax[col].append(float(x[0]))
        #print(minmax[col])
        minmax[col].sort()
    return minmax

def main():
    # Already got the full list. Now append with ...
    '''
    stat_file = "/home/jemerson/wopr/stats/data_stats.txt"
    file_list = get_file_list()
    get_all_stats(file_list, JSON_COLUMNS, TO_DROP, stat_file)
    '''
    #write_min_max([SELECTED_HDRS[0]])
    #write_min_max(SELECTED_HDRS[3:8])
    #write_veh_ID_list(JSON_COLUMNS)
    #list_to_csv("/home/jemerson/wopr/stats/vehicle_IDs.csv", VEHICLE_IDS)
    IDs = "/home/jemerson/wopr/stats/min_max_values.txt"

    minmax = min_max_lists(IDs, SELECTED_HDRS[3:8])
    for m in minmax:
        print(minmax[m])
    

if __name__ == '__main__':
    main()

