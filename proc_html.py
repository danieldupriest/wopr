#!/bin/python3
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
import sys
from lists import STOP_HDRS_TO_DROP, STOP_HDRS_TO_KEEP
from datetime import datetime
import download_html
from transform import transform_stop_data

def print_all(df):
    with pd.option_context('display.max_rows', None,'display.max_columns', None):
        print(df)

def get_stop_data(html):
    f = open(html, "r")
    soup = BeautifulSoup(f, 'lxml')

    d = soup.find('h1') 
    d = d.get_text(strip=True)
    d = re.search("\d{4}-\d\d?-\d\d?", d)
    doc_date = d[0]
    
    headers = []
    hdrs = soup.find_all('th')
    for h in hdrs:
        str_cells = str(h)
        cleantext = BeautifulSoup(str_cells, "lxml").get_text()
        headers.append(cleantext)
    headers = list(dict.fromkeys(headers))

    # Create list of trip IDs on website:
    trip_list = []
    trips = soup.find_all('h3')
    for t in trips:
        t = t.get_text(strip=True)
        t = re.search("\d+", t)
        trip_list.append(t[0])
    
    # Create list of tables, each of which corresponds to a trip ID:
    table_list = []
    tables = soup.find_all('table')
    for t in tables:
        table_list.append(t)

     # Create list with header row and one row with trip data: 
    table_rows = []
    for t in table_list:
        rows = t.find_all('tr', limit=2)
        table_rows.append(rows)

    # Extract data from each table and save into a separate dataframe, then save dfs to list:
    dfs = []
    for i in range(0, len(table_rows)):
        # Create list of data for each row of data (skip headers):
        list_rows = []
        for row in table_rows[i]:
            row_td = row.find_all('td')
            str_cells = str(row_td)
            cleantext = BeautifulSoup(str_cells, "lxml").get_text()
            list_rows.append(cleantext.strip())

        # Create dataframe using pandas:
        df = pd.DataFrame(list_rows)

        # Split into columns and remove brackets:
        df[0] = df[0].str.strip('[]')
        df = df[0].str.split(',', expand=True)

        # Drop first garbage row and add headers:
        df = df.drop(index=0)
        df.columns = headers

        # Create df for trip_id column:
        df['trip_id'] = trip_list[i]  
        dfs.append(df)
        
        # Create df for date column:
        df['date'] = doc_date
        dfs.append(df)

    # Combine all dataframes:
    all_data = dfs[0]
    for i in range(1, len(dfs)):
        all_data = all_data.append(dfs[i])

    # Strip leading whitespace from data we need:
    for col in STOP_HDRS_TO_KEEP:
        all_data[col] = all_data[col].str.strip()

    # Map the service_key to Weekday/Saturday/Sunday and direction to Out/Back:
    all_data = transform_stop_data(all_data)

    return all_data

# Test / Process Backlog
def process_other_html(filename):
    html = "/home/jemerson/wopr/stop_data_html/" + filename + ".html"
    print("Processing file", html)
    all_data = get_stop_data(html)
    json_file = "/home/jemerson/wopr/stop_data/" + filename + ".json"
    all_data.to_json(json_file, orient='records')
    print("Wrote to json file ", json_file)
    '''
    get_stop_data(html)
    '''

# Process the HTML from today and convert to JSON:
def process_latest_html():
    date = str(datetime.now().strftime("%Y_%m_%d"))
    html = "/home/jemerson/wopr/stop_data_html/" + date + ".html"
    all_data = get_stop_data(html)
    json_file = '/home/jemerson/wopr/stop_data/' + date + '.json'
    all_data.to_json(json_file, orient='records')

def proc_html():
    #process_other_html("test")
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        process_other_html(filename)
    else:
        process_latest_html()
     

if __name__ == '__main__':
    proc_html()
