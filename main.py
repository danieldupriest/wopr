#!/usr/local/bin/
import pandas as pd
import numpy as np
from lists import JSON_COLUMNS, SELECTED_HDRS, TO_DROP, FINAL_COLS
from utilities import get_file_list, json_to_df, map_dates
from set_datatypes import set_datatypes
from valdata import validate_limits, validate_no_null, validate_intra

filelist = get_file_list()
f = filelist[0] # Use the first JSON file for now
f = "/home/jemerson/wopr/test/2021_02_06.json"
df = json_to_df(f, JSON_COLUMNS) 
df = set_datatypes(df)

#validate_limits(df)
error = []
if not validate_intra(df):
    error.append(d)
    print("no")    





