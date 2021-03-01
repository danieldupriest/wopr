#!/bin/python3
# Bulk load stop data from json file.

from datetime import datetime
import glob
import os
import json
import random
import sys
import time
import db_helper
import pandas as pd
from lists import STOP_HDRS_TO_KEEP
from trip_set_datatypes import manage_stop_data
import consumer

# Opens the specified data file and returns the json interpretation.
def load_data_file(data_file):
    print("Loading data file", data_file, "for bulk insert.")
    file = open(data_file)
    data = json.load(file)
    return data

def main():
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        breadcrumb_event_data = load_data_file(filename)
        print("Processing {} rows.".format(len(breadcrumb_event_data)))
        consumer.process_batch(breadcrumb_event_data)


if __name__ == '__main__':
    main()

