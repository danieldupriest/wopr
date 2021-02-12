#!/usr/local/bin/
import pandas as pd
import numpy as np
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
import json
import os
import glob
from utilities import get_test_file, get_file_list, json_to_df
from set_datatypes import drop_cols_and_rows, convert_and_set_dtypes, set_dtypes
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP 

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

def get_max_max(file_list, headers, column):
    for d in file_list:
        pass

def get_value_counts(datafile, all_cols, to_drop, column, stat_file):
    df = json_to_df(datafile, all_cols)
    df = drop_cols_and_rows(df, to_drop)
    get_stats(df, stat_file)
    f = open(stat_file, "a")
    print(df[column].value_counts(), file=f)
    f.close()
    return df


def main():
    stat_file = "/home/jemerson/wopr/stats/data_stats.txt"
    file_list = get_file_list()
    get_all_stats(file_list, JSON_COLUMNS, TO_DROP, stat_file)


if __name__ == '__main__':
    main()

