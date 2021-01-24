#!/bin/python3

from confluent_kafka import Producer, KafkaError
import glob
import os
import json
import random
import time

# Grab latest datafile from ./data/ and sort by act_time
def get_latest_json_from_data_file():
    file_list = glob.glob('/home/jemerson/wopr/data/*')
    latest_file = max(file_list, key=os.path.getctime)
    print("Loading json from", latest_file)
    file = open(latest_file)
    data = json.load(file)
    data.sort(key=lambda i: int(i["ACT_TIME"]))
    return data

if __name__ == '__main__':

    # Read configuration and initialize
    config_file = "/home/jemerson/.confluent/librdkafka.config"
    topic = "breadcrumbs"
    conf = json.load(open(config_file))

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed. Problem with ccloud_lib
    # ccloud_lib.create_topic(conf, topic)

    delivered_records=0

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

    while True:
        data = get_latest_json_from_data_file()
        for i in range(len(data)):
            if i%5 == 0:
                producer.flush()
                time.sleep(5)
            data_line = data[i]
            record_key = "wopr_key"
            record_value = json.dumps(data_line)
            # print("Producing record: {}\t{}".format(record_key, record_value))
            producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
            producer.poll(0)

    producer.flush()

