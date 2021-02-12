#!/usr/local/bin/
import pandas as pd
import numpy as np
import os
from utilities import get_file_list, json_to_df, map_dates
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP
import datetime

def convert_to_mph(df):
    df['VELOCITY'] = df['VELOCITY'] * 2.236936
    return df

def main():
    path = os.path.abspath('.')
    datafile = path + "/test/2021_02_06.json"
    df = json_to_df(datafile, JSON_COLUMNS)

    # This doesn't work bc datatypes have not been set:
    #df = convert_to_mph(df)
    #print(df.head())

    # Use this form to convert a date:
    # date = datetime.date(2021, 1, 17)
    datemap = map_dates()
    print(datemap[datetime.date(2021,1,17)])



if __name__ == '__main__':
    main()

