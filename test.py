#!/bin/python3
#from consumer2 import process_batch
#import json
from proc_html import get_stop_data
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
from valdata import validate_no_null
from lists import STOP_HDRS_TO_KEEP, VEHICLE_IDS
from trip_set_datatypes import manage_stop_data

'''
json_batch = []
for i in range(0, 10):
    # Grab a single json record:
    json_list = json.loads('/home/jemerson/wopr/stop_data/test.json')
    #aList = json.dumps(json_str)
    record = json_list[i]
    data = json.loads(record)
    json_batch.append(data)

df = process_batch(json_batch)
print(df)
'''

html = "/home/jemerson/wopr/stop_data_html/test.html"
html = "/home/jemerson/wopr/stop_data_html/2021_02_24.html"
data = get_stop_data(html)
data = manage_stop_data(data) 
errors = validate_no_null(data, STOP_HDRS_TO_KEEP)
print(errors)
# No null values in test df. We should be able to drop rows with null values?

'''
Column('vehicle_number', [null_validation, InRangeValidation(0,)]), # What is range or set?
Column('route_number', [null_validation, InRangeValidation(0,)]), # What is range or set?

Column('trip_id', [null_validation, InRangeValidation(,)]), #
Column('date', [null_validation, InRangeValidation(,)]), #
'''

# ['vehicle_number','route_number','direction','service_key','trip_id','date']
def validate_stop_data(df):
    toval = Schema([
                Column('vehicle_number', [null_validation, InListValidation(VEHICLE_IDS)]), # 
                Column('direction', [null_validation, InListValidation([0,1])]), # Should verify all for trip_id have same direction 
                Column('service_key', [null_validation, InListValidation(['W', 'S', '?'])]), #
                ])
    errors = toval.validate(df.loc[:, STOP_HDRS_TO_KEEP])
    for error in errors:
        print(error)

# Query DB to verify date exists in breadcrumb data
# Query DB to verify trip_id exists in breadcrumb data


