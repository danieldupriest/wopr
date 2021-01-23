#!/usr/bin/python3
import urllib.request
from datetime import datetime
import os

def download_json():
    url = "http://rbi.ddns.net/getBreadCrumbData"
    path = os.path.abspath('.')  
    #date = str(datetime.now().strftime("%Y_%m_%d_%H:%M")) # < For testing
    date = str(datetime.now().strftime("%Y_%m_%d")) 
    datafile = path + "/data/" + date + ".json"
    urllib.request.urlretrieve(url, datafile)

if __name__ == "__main__":
    download_json()

