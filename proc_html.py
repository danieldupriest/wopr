#!/bin/python3
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
from lists import STOP_HDRS_TO_DROP
from datetime import datetime
import download_html

def print_all(df):
    with pd.option_context('display.max_rows', None,'display.max_columns', None):
        print(df)

def get_stop_data(html):
    '''
    # Open URL and create a BeautifulSoup object to work with:
    url = "http://rbi.ddns.net/getStopEvents"
    html = urlopen(url)
    '''
 
    html = "/home/jemerson/wopr/stop_data_html/test.html"
    f = open(html, "r")
    soup = BeautifulSoup(f, 'lxml')

    d = soup.find('h1') 
    d = d.get_text(strip=True)
    d = re.search("\d{4}-\d\d?-\d\d?", d)
    print(d[0])
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

    # Create list of rows for each table: 
    table_rows = []
    for t in table_list:
        rows = t.find_all('tr')
        table_rows.append(rows)
       
    # Extract data from each table and save into a separate dataframe, then save dfs to list:
    dfs = []
    for i in range(0, len(table_rows)):
        # Create list of data for each row:
        list_rows = []
        for row in table_rows[i]:
            row_td = row.find_all('td')
            str_cells = str(row_td)
            cleantext = BeautifulSoup(str_cells, "lxml").get_text()
            list_rows.append(cleantext)

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

    all_data = dfs[0]
    for i in range(1, len(dfs)):
        all_data = all_data.append(dfs[i])

    return all_data


def main():
    '''
    # Grab html file to process;
    try:
        html = download_html() 
    except Exception:
        print("HTML download failed for unknown reason.")
    '''
    date = str(datetime.now().strftime("%Y_%m_%d"))
    html = "/home/jemerson/wopr/stop_data_html/" + date + ".html"
    all_data = get_stop_data(html)
    #print(all_data.head())
    json_file = '/home/jemerson/wopr/stop_data/' + date + '.json'
    json_data = all_data.to_json(orient='records')

    #print(json_data[:1000])
    #TODO Don't actually need to return data, maybe a return code instead: 
    return json_data
   

if __name__ == '__main__':
    main()
