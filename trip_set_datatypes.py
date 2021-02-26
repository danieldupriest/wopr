#!/usr/local/bin/
import pandas as pd
#import numpy as np
from lists import STOP_HDRS_TO_DROP
#from valdata import validate_no_null

# Clean the dataframe by dropping unneeded columns and rows with empty fields:
def drop_cols_and_rows(df, COLUMNS_TO_DROP):
    #TODO Not sure if this is needed or not:
    # This is important bc dropna() does not drop rows with 1+ empty string.
    #df = df.replace('', np.NaN)
    # Drop the unneeded columns (axis=1 specifies columns):
    df = df.drop(COLUMNS_TO_DROP, axis=1)
    return df

# Seems like convert_dtypes is unnecessary middleman, but made it work:
def convert_and_set_dtypes(df):
    # First, convert all types from pandas object to string:
    df = df.convert_dtypes()
    # Then convert to the correct datatypes: 
    df = set_dtypes(df)
    return df

# Set the datatypes so we can work with the data more effectively:
def set_dtypes(df):
    df = df.astype({
        'vehicle_number': 'int64' ,
        'route_number': 'int64',
        'direction': 'string',
        'service_key': 'string',
        'trip_id': 'int64'
        'date' 'string':
        })
    return df

# This should be renamed to maybe manage_stop_data() 
def manage_stop_data(df):
    # Drop unneeded columns and rows with empty string; set datatypes:
    df = drop_cols_and_rows(df, STOP_HDRS_TO_DROP)

    '''
    #TODO Initial data validation for existence here:
    errors = validate_no_null(df, SELECTED_HDRS[0:8]) 
    for error in errors:
        print(error)
    '''

    # Convert datatypes:
    df = convert_and_set_dtypes(df)
    return df
   

def main():
    pass
    
if __name__ == '__main__':
    main()

