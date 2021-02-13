#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
import datetime
from csv import writer
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP, FINAL_COLS, VEHICLE_IDS
from utilities import get_file_list, json_to_df, map_dates
from transform import convert_to_mph 
#from set_datatypes import set_datatypes

def validate_no_null(df, columns):
    null_validation = CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')
    
    exist_cols = []
    for col in columns:
        exist_cols.append(Column(col, [null_validation]))
    exist = Schema(exist_cols)
    errors = exist.validate(df.loc[:,[c for c in columns]])
    return errors


def validate_limits(df):
    limit = Schema([
                Column('ACT_TIME', [null_validation, InRangeValidation(14000, 92000)]), # min/max observed -/+ ~1000
                Column('VELOCITY', [null_validation, InRangeValidation(0,40)]), # min/max observed -0/+2
                Column('DIRECTION', [null_validation, InRangeValidation(0,360)]), # known range 
                Column('GPS_LONGITUDE', [null_validation, InRangeValidation(45.49436, 45.89615)]), # min/max observed -/+ ?
                Column('GPS_LATITUDE', [null_validation, InRangeValidation(-122.70826, -122.299455)]), # min/max observed -/+ ?
                ])
    errors= limit.validate(df.loc[:,['ACT_TIME', 'VELOCITY', 'DIRECTION', 'GPS_LONGITUDE', 'GPS_LATITUDE']])
    for error in errors:
        print(error)

def validate_intra(df):
    '''
    intra = Schema([
                Column(...)
                ])
    '''
    for frame in df:
        if frame['GPS_LONGITUDE'] == 0  or frame['GPS_LATITUDE'] == 0:
            print("uhoh")
            return False
'''
def validate_inter(df):
    inter = Schema([
                Column(...)
                ])


def validate_summary(df):
    summary = Schema([
                Column(...)
                ])
                
def validate_distribution(df):
    distribution = Schema([
                Column(hmmm...)
                ])
'''

def validate_integrity(df):
    integrity = Schema([
                Column('VEHICLE_ID', [InListValidation(VEHICLE_IDS)])
                ])

def main():
    pass


if __name__ == '__main__':
    main()

