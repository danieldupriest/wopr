#!/usr/bin/python3
import urllib.request
from datetime import datetime
import os

def download_json():
    url = "http://rbi.ddns.net/getBreadCrumbData"
    #path = os.path.abspath('.')  
    path = "/home/jemerson/wopr"
    #date = str(datetime.now().strftime("%Y_%m_%d_%H:%M")) # < For testing
    date = str(datetime.now().strftime("%Y_%m_%d")) 
    datafile = path + "/data/" + date + ".json"
    try:
        urllib.request.urlretrieve(url, datafile)
    except Exception:
        print("Download attempt failed for unknown reason.")
    

if __name__ == "__main__":
    download_json()

