#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
import glob
from datetime import date
from csv import writer

JSON_COLUMNS = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'RADIO_QUALITY', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION']
HEADERS = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'num_rows', 'json_date']
TO_DROP = ['EVENT_NO_STOP','METERS', 'RADIO_QUALITY', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION']

def get_test_file(date):
    path = os.path.abspath('.')
    test_file = path + "/data/" + date + ".json"
    return test_file

# Create a list of data files in the /data directory:
def get_file_list():
    path = os.path.abspath('.')
    file_list = glob.glob(path + '/data/*')
    test_file = path + "/data/2021_02_05.json"
    file_list.sort()
    #return [test_file]
    return file_list
        
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
        print("Attempted to append today's row to bc_stats.csv, but was unsuccessful for some reason (maybe today's JSON file did not download successfully).")

# Convert a JSON breadcrumb data file to a pandas dataframe:
def json_to_df(datafile, columns):
    with open(datafile, "r") as j: 
        data = json.load(j)
    df = pd.DataFrame(data)
    # Why does this not get saved and how can I make it so?: TODO
    df.columns = columns #JSON_COLUMNS
    return df

##### PROCESS DATA ON EMPTY FIELDS #####
# Loop over data directory and process the daily JSON breadcrumb data files:
def create_empty_fields_df(file_list, all_columns):
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
    df = filter_columns(df, HEADERS)
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

def create_bc_stats_csv(df):
    empty_fields_csv = df.to_csv('bc_stats.csv')
    
def main():
    # Process the whole list:
    '''
    file_list = get_file_list()
    df = create_empty_fields_df(file_list)
    create_bc_stats_csv(df)
    '''
    # Already processed the old ones.
    # Append the latest row:  
    file_name = get_new_file()
    df = create_empty_fields_df([file_name])
    df = calc_percentages(df)
    df.to_csv('bc_stats.csv', header=None, mode='a')
    
if __name__ == '__main__':
    main()

