#!/usr/bin/python3
from datetime import datetime
import os
import download_json

#path = os.path.abspath('.')  
path = "/home/jemerson/wopr"
date = str(datetime.now().strftime("%Y_%m_%d")) 
time = str(datetime.now().strftime("%H:%M")) 
datafile = path + "/data/" + date + ".json"
logfile = path + "/log/" + "download_log.txt"
success = False

try:
    f = open(datafile)
    success = True
    f.close()
except IOError:
    download_json.download_json()

#finally:
#    f.close()

try:
    log = open(logfile,"a+")
    if success is True:
        log.write(date + " " + time + " Download appears to have been successful.\n")
    elif success is False:
        log.write(date + " " + time + " Download appears to have failed, so we'll re-download.\n")
except IOError:
    pass
finally:
    log.close()


