#!/bin/python3
# Load data from file and produce kafka messages slowly over the day.
# Check regularly for newer files to load.

from confluent_kafka import Producer, KafkaError
from datetime import datetime
import glob
import os
#import json
import random
import time
from proc_html import get_stop_data

WORKING_PATH = "/home/jemerson/wopr"
CONFIG_FILE = "/home/jemerson/.confluent/librdkafka.config"

# TODO Need to adjust these as needed:
ROWS_PER_INTERVAL = 8 # n rows sent every interval
SLEEP_INTERVAL = 5 # seconds
FILE_CHECK_RATE = 100 # check for new file once every n rows

# Grabs the path of the latest file in the data/ directory.
def get_latest_data_file():
    file_list = glob.glob(WORKING_PATH + '/stop_data/*') # Changed from /data/*
    latest_file = max(file_list, key=os.path.getctime)
    return latest_file

# Opens the specified data file and returns the json interpretation.
def replace_data_file(data_file):
    date = str(datetime.now().strftime("%Y_%m_%d"))
    time = str(datetime.now().strftime("%H:%M"))
    log_file_path = WORKING_PATH + "/log/" + "produce2_log.txt" # Changed from "produce_log.txt"
    with open(log_file_path, "a+") as f:
        f.write(date + " " + time + " - " + "Loading data from file " + data_file + " for production.\n")
    file = open(data_file)

    #TODO Need to add a function here that processes the HTML:
    data = json.load(file)
    data.sort(key=lambda i: int(i["ACT_TIME"]))

    return data

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
