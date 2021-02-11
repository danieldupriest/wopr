#!/usr/local/bin/
import pandas as pd
import numpy as np
import json
import os
import glob
from data_stats import get_file_list, json_to_df 

def hdop_values(df, date):
    HDOP_VAL = [-np.inf,0,1,2,5,10,20,np.inf]

    hdop = df['GPS_HDOP'] 
    hdop.replace('', np.NaN, inplace=True)
    hdop.rename(date, inplace=True)
    hdop = hdop.astype(float) #, errors='ignore')

    hdop = hdop.value_counts(normalize=True, bins=HDOP_VAL, sort=False, dropna=False)
    hdop = hdop.to_frame()
    hdop = hdop.transpose()
    hdop.columns = [str(HDOP_VAL[i-1]) + "-" + str(HDOP_VAL[i]) for i in range(1, len(HDOP_VAL))]
    #print(hdop)
    return hdop

def process_hdop_values(file_list):
    hdops = []
    for f in file_list:
        df = json_to_df(f)
        hdop = hdop_values(df, str(f[25:35]))
        hdops.append(hdop)
        
    all_hdop = pd.concat(hdops)
    #print(all_hdop)
    all_hdop.sort_index(inplace=True)

    return all_hdop

def hdop_csv(df):
    hdop_csv = df.to_csv('hdop_data.csv')

def main():
    file_list = get_file_list()
    all_hdop_data = process_hdop_values(file_list)
    #hdop_csv(all_hdop_data) 
    
    print(all_hdop_data)


if __name__ == '__main__':
    main()

