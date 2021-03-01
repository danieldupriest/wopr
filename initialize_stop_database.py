#!/bin/python3

import json
import psycopg2
import db_helper

def initialize(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
        drop table if exists Stop;

        create table Stop (
            vehicle_number integer,
            route_number integer,
            trip_id integer,
            arrive_time timestamp,
            FOREIGN KEY (trip_id) REFERENCES Trip
        );
        """)

def main():
    conn = db_helper.connect()
    initialize(conn)

if __name__ == "__main__":
    main()
