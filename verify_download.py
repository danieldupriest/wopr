#!/usr/bin/python3
from datetime import datetime
import os
import download_json

path = os.path.abspath('.')  
date = str(datetime.now().strftime("%Y_%m_%d")) 
time = str(datetime.now().strftime("%H:%M")) 
datafile = path + "/data/" + date + ".json"
logfile = path + "/log/" + "download_log.txt"
success = False

try:
    f = open(datafile)
    success = True
except IOError:
    download_json.download_json()
finally:
    f.close()

f = open(logfile,"a+")
if success is True:
    f.write(date + " " + time + " Download appears to have been successful.\n")
elif success is False:
    f.write(date + " " + time + " Download appears to have failed, so we'll re-download.\n")
f.close()


