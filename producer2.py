#!/bin/python3
# Load data from file and produce kafka messages slowly over the day.
# Check regularly for newer files to load.

from confluent_kafka import Producer, KafkaError
from datetime import datetime
import glob
import os
import json
import random
import time
#from proc_html import get_stop_data
#import csv

delivered_records = 0
WORKING_PATH = "/home/jemerson/wopr"
CONFIG_FILE = "/home/jemerson/.confluent/librdkafka.config"

# TODO Adjust these as needed:
ROWS_PER_INTERVAL = 10 # n rows sent every interval
SLEEP_INTERVAL = 5 # seconds
FILE_CHECK_RATE = 100 # check for new file once every n rows

# Grabs the path of the latest file in the data/ directory.
def get_latest_data_file():
    file_list = glob.glob(WORKING_PATH + '/stop_data/*')
    latest_file = max(file_list, key=os.path.getctime)
    return latest_file

# Opens the specified data file and returns the json interpretation.
def replace_data_file(data_file):
    date = str(datetime.now().strftime("%Y_%m_%d"))
    time = str(datetime.now().strftime("%H:%M"))
    log_file_path = WORKING_PATH + "/log/" + "produce2_log.txt"
    with open(log_file_path, "a+") as f:
        f.write(date + " " + time + " - " + "Loading data from file " + data_file + " for production.\n")
    file = open(data_file)

    #Sort
    data = json.load(file)
    data.sort(key=lambda i: int(i["arrive_time"]))
    return data

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        # print("Produced record to topic {} partition [{}] @ offset {}"
        #      .format(msg.topic(), msg.partition(), msg.offset()))

def main():
    # Read configuration and initialize
    topic = "stop_data"
    conf = json.load(open(CONFIG_FILE))
    current_data_file = get_latest_data_file()

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    stop_event_data = replace_data_file(current_data_file)

    while True:
        i = 0
        while i in range(len(stop_event_data)):
            if i % ROWS_PER_INTERVAL == 0:
                producer.flush()
                time.sleep(SLEEP_INTERVAL)
            if i % FILE_CHECK_RATE == 0:
                latest_file = get_latest_data_file()
                if latest_file != current_data_file:
                    stop_event_data = replace_data_file(latest_file)
                    current_data_file = latest_file
                    i = 0

            data_line = stop_event_data[i]
            record_key = "wopr_key2"
            record_value = json.dumps(data_line)

            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            producer.poll(0)
            i += 1
    producer.flush()

if __name__ == '__main__':
    main()

