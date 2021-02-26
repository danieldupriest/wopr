#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
import glob
from datetime import date
from csv import writer
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP
from utilities import get_test_file, get_file_list, json_to_df
       
def get_new_file():
    path = os.path.abspath('.') 
    file_list = glob.glob(path + '/data/*')
    latest_file = max(file_list, key=os.path.getctime)

    today = date.today()
    the_date = today.strftime("%Y_%m_%d")
    the_file = str(latest_file)[25:35]
    if the_date == the_file:
        return str(path) + "/data/" + the_date + ".json"
    else:
        print("Attempted to append today's row to empty_str_stats.csv, but was unsuccessful for some reason (maybe today's JSON file did not download successfully).")

##### PROCESS DATA ON EMPTY FIELDS #####
# Loop over data directory and process the daily JSON breadcrumb data files:
def create_empty_fields_df(file_list, all_columns, selected_headers):
    num_rows = []
    dfs = []
    json_dates = []
    for datafile in file_list:
        df = json_to_df(datafile, all_columns)
        df_list = calc_empty_fields(df, num_rows)
        dfs.append(df_list[0])
        num_rows = df_list[1] 
        json_date = str(datafile)
        json_dates.append(json_date[25:35])
    df = pd.concat(dfs)
    df.columns = all_columns # JSON_COLUMNS
    df = add_col(df, "num_rows", num_rows) 
    df = add_col(df, "json_date", json_dates)
    df = filter_columns(df, selected_headers) # SELECTED_HDRS
    sorted_df = df.sort_values(by='json_date')
    return sorted_df

# Pass a dataframe to parse for empty fields: 
def calc_empty_fields(df, num_rows):
    num_rows.append(len(df))
    values = []

    # Calculate the number of rows where the value = empty string for each column:
    for i in range (0, len(df.columns)):
        values.append(len(df[df[df.columns[i]] == '']))

    # Convert the values to a dataframe
    df = pd.DataFrame(values)

    # The axes are wrong; columns and rows need to be swapped:
    df = df.transpose()
    return [df, num_rows]

def add_col(df, row_name, data_list):
    df[row_name] = data_list
    return df

def filter_columns(df, headers):
    df = df[headers] 
    return df

def calc_percentages(df):
    columns = ['VELOCITY','DIRECTION','GPS_LONGITUDE','GPS_LATITUDE']
    for c in columns:
        df[c] = round(100 * df[c]/df['num_rows'], 2)
    return df

def create_empty_str_stats_csv(df):
    empty_fields_csv = df.to_csv('empty_str_stats.csv')
    return empty_fields_csv

def display_empty_str_stats(csv):
    pass
    # To view from CLI:
    #column -s, -t < empty_str_stats.csv | less -#2 -N -S
    
def process_all_json():
    # Process the whole list:
    file_list = get_file_list()
    df = create_empty_fields_df(file_list, JSON_COLUMNS, SELECTED_HDRS)
    create_empty_str_stats_csv(df)

def add_daily_data():
    file_name = get_new_file()
    df = create_empty_fields_df([file_name], JSON_COLUMNS, SELECTED_HDRS)
    df = calc_percentages(df)
    df.to_csv('empty_str_stats.csv', header=None, mode='a')

def main():
    # Already processed the old ones.
    # Append the latest row:  
    add_daily_data()


if __name__ == '__main__':
    main()

