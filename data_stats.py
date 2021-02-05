#!/usr/local/bin/
import pandas as pd
#import numpy as np
#from pandas_schema import Column, Schema
#from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
import json
import os
import glob

all_columns = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'RADIO_QUALITY', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION']
hdrs = ['EVENT_NO_TRIP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'num_rows']

path = os.path.abspath('.')
file_list = glob.glob(path + '/data/*')
#file_list = [path + "/data/2021_02_03.json"]
empty_count = pd.DataFrame()
num_rows = []

# Loop over data directory and process the daily JSON files:
for datafile in file_list:
    with open(datafile, "r") as j: 
        data = json.load(j)
    df = pd.DataFrame(data)
    num_rows.append(len(df))
    values = []
    # Calculate the number of rows where the value = empty string for each column:
    for i in range (0, len(df.columns)):
        values.append(len(df[df[df.columns[i]] == '']))
    # Convert the values to a dataframe
    val = pd.DataFrame(values)
    # The orientation is wrong; columns and rows need to be switched:
    val = val.transpose()
    empty_count = empty_count.append(val)

empty_count.columns = all_columns
empty_count['num_rows'] = num_rows
empty_count = empty_count[hdrs]
bc_stats = empty_count.to_csv('bc_stats.csv')
print(empty_count)
