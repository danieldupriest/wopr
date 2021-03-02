#!/usr/bin/python3
from datetime import datetime
import os
import download_html
from proc_html import proc_html

path = "/home/jemerson/wopr"
date = str(datetime.now().strftime("%Y_%m_%d")) 
time = str(datetime.now().strftime("%H:%M")) 
datafile = path + "/stop_data/" + date + ".json"
logfile = path + "/log/" + "html_to_json_log.txt"
success = False

try:
    f = open(datafile)
    success = True
    f.close()
except IOError:
    try:
        proc_html()
    except:
        log = open(logfile,"a+")
        log.write(date + " " + time + "Could not process HTML at this time.\n"
        log.close()
        break

try:
    log = open(logfile,"a+")
    if success is True:
        log.write(date + " " + time + " Convert HTML to JSON appears to have been successful.\n")
    elif success is False:
        log.write(date + " " + time + " Convert HTML to JSON appears to have failed, so we'll re-download.\n")
except IOError:
    pass
finally:
    log.close()


