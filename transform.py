#!/usr/local/bin/
import pandas as pd
import numpy as np
import os
from data_stats import get_file_list, json_to_df, JSON_COLUMNS,HEADERS,TO_DROP

def convert_to_mph(df):
    df['VELOCITY'] = df['VELOCITY'] * 2.236936
    return df

def main():
    path = os.path.abspath('.')
    datafile = path + "/test/2021_02_06.json"
    df = json_to_df(datafile, JSON_COLUMNS)

    # This doesn't work bc datatypes have not been set:
    df = convert_to_mph(df)
    print(df.head())



if __name__ == '__main__':
    main()

