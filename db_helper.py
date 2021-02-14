#!/bin/python3
# Helper for connecting to database.

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
