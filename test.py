#!/bin/python3
#from consumer2 import process_batch
#import json
from proc_html import get_stop_data
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation, InRangeValidation, IsDistinctValidation 
from valdata import validate_no_null
from lists import STOP_HDRS_TO_KEEP
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



