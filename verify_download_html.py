#!/usr/bin/python3
from datetime import datetime
import os
import download_html

path = "/home/jemerson/wopr"
date = str(datetime.now().strftime("%Y_%m_%d")) 
time = str(datetime.now().strftime("%H:%M")) 
datafile = path + "/stop_data_html/" + date + ".html"
logfile = path + "/log/" + "html_download_log.txt"
success = False

try:
    f = open(datafile)
    success = True
    f.close()
except IOError:
    download_html()

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


