#!/bin/python3

import json
import psycopg2

DATABASE_CONFIG = "database.conf"

def connect():
    conf = json.load(open(DATABASE_CONFIG))
    connection = psycopg2.connect(
            host = conf['host'],
            database = conf['database'],
            user = conf['user'],
            password = conf['password']
            )
    connection.autocommit = True
    return connection

def initialize(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
        drop table if exists BreadCrumb;
        drop table if exists Trip;
        drop type if exists service_type;
        drop type if exists tripdir_type;

        create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
        create type tripdir_type as enum ('Out', 'Back');

        create table Trip (
            trip_id integer,
            route_id integer,
            vehicle_id integer,
            service_key service_type,
            direction tripdir_type,
            PRIMARY KEY (trip_id)
        );

        create table BreadCrumb (
            tstamp timestamp,
            latitude float,
            longitude float,
            direction integer,
            speed float,
            trip_id integer,
            FOREIGN KEY (trip_id) REFERENCES Trip
        );
        """)

def main():
    conn = connect()
    initialize(conn)

if __name__ == "__main__":
    main()
