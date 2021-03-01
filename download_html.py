#!/usr/bin/python3
import urllib.request
from datetime import datetime
import os

def download_html():
    url = "http://34.83.136.192:8000/getStopEvents/"
    path = "/home/jemerson/wopr"
    #date = str(datetime.now().strftime("%Y_%m_%d_%H:%M")) # < For testing
    date = str(datetime.now().strftime("%Y_%m_%d")) 
    datafile = path + "/stop_data_html/" + date + ".html"
    try:
        urllib.request.urlretrieve(url, datafile)
    except Exception:
        print("HTML download failed for unknown reason.")


if __name__ == "__main__":
    download_html()

