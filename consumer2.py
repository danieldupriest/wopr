#!/bin/python3
import json
import db_helper
import pandas as pd
from confluent_kafka import Consumer
from lists import STOP_HDRS_TO_KEEP
from trip_set_datatypes import manage_stop_data

DB_INSERT_BATCH_SIZE = 10

# Carry out data validation and conversion
def process_batch(batch):
    df = pd.DataFrame(batch)
    #df.columns = STOP_HDRS_TO_KEEP

    #TODO Drop columns, convert datatypes:
    df = manage_stop_data(df)

    #TODO Validation here maybe?

    #TODO
    #add_data_to_database(df) 

# Insert breadcrumb rows into database, generating trip ids as needed
def add_data_to_database(df):

    # Get a new database connection
    conn = db_helper.connect()
    cur = conn.cursor()

    # Process the rows
    for i, row in df.iterrows():
        
        # First check if trip exists
        trip_id = row['EVENT_NO_TRIP']
        if trip_exists(trip_id, cur):
            insert_breadcrumb(row, cur)
        else:
            insert_trip(row, cur)
            insert_breadcrumb(row, cur)

    # Finalize changes and close connection
    conn.commit()
    cur.close()
    conn.close()
    
    print("Wrote {} breadcrumbs to the database.".format(DB_INSERT_BATCH_SIZE))

# Check if a trip exists already in the database
def trip_exists(trip_id, cur):
    cur.execute("""
        SELECT COUNT(trip_id)
        FROM trip
        WHERE trip_id = %s
        """,
        (trip_id,)
    )
    result = cur.fetchone()[0]
    if result == 1:
        return True
    return False

# Insert trip into database (required as a foreign key for breadcrumbs)
def insert_trip(row, cur):
    trip_id = row['EVENT_NO_TRIP']
    route_id = 0
    vehicle_id = row['VEHICLE_ID']
    service_key = 'Weekday'
    direction = 'Out'
    cur.execute("""
        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (trip_id, route_id, vehicle_id, service_key, direction)
    )

# Consume from kafka topic, validate, transform and insert into database
def main():
    # Read arguments and configurations and initialize
    config_file = "/home/jemerson/.confluent/librdkafka.config"
    topic = "stop_data"
    conf = json.load(open(config_file))

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Collect messages
    json_batch = []
    try:
        while True:
            # Wait until message batch it full to process
            if len(json_batch) >= DB_INSERT_BATCH_SIZE:
                process_batch(json_batch)
                json_batch = []
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                json_batch.append(data)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

if __name__ == '__main__':
    main()

