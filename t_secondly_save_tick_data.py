# -*- coding: utf-8 -*-
import finlib
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt

from sqlalchemy import create_engine
import tushare as ts
from pandas import read_csv
from pandas import datetime
from pandas import DataFrame
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
import multiprocessing

import sys
import time
import gc

todayS = datetime.today().strftime('%Y-%m-%d')

#实时行情.   一次性获取当前交易所有股票的行情数据
#Get all code here
print(("Update Data of " + todayS))

dump_all_code = "/home/ryan/DATA/" + todayS + "_all.pickle"

if not os.path.isfile(dump_all_code):
    today_all = ts.get_today_all()
    today_all.to_pickle(dump_all_code)
    print(("Today all Pickle saved to " + dump_all_code))
else:
    today_all = pd.read_pickle(dump_all_code)

record_code = today_all['code']

#record_code = ['600000'] #debug
pick_dir = "/home/ryan/DATA/pickle/tick"

code_len = record_code.__len__()
split_cnt = 500
in_trading = True
df = pd.DataFrame()

i = 0
while True:
    todayS = datetime.today().strftime('%Y-%m-%d')
    dump = pick_dir + "/" + todayS + "_tick.pickle"

    for root, dirs, files in os.walk(pick_dir):
        pass

    file_num = files.__len__() + 1  # start suffix of the pickle

    #check trading time
    hour = float(datetime.today().strftime('%H'))
    min = float(datetime.today().strftime('%M'))
    sec = float(datetime.today().strftime('%S'))

    now_float = hour + min / 100  #

    if (in_trading == True):
        print("system time " + str(now_float))

    if now_float < 9.30 or now_float > 15.30:  #9.29,  15.31
        if (in_trading == True):  #print once only
            print("not in trading time, " + str(now_float))
            in_trading = False
        time.sleep(1)
        continue

    if now_float > 12.00 and now_float < 13.00:  #12.01 to 12.59
        if (in_trading == True):
            print("not in trading time, " + str(now_float))
            in_trading = False
        time.sleep(1)
        continue

    ### START
    in_trading = True
    i += 1

    for j in range(int(code_len / split_cnt) + 1):
        start = j * split_cnt
        end = start + split_cnt - 1
        if end > code_len - 1:
            end = code_len - 1
        print(str(i) + ", " + str(start) + "," + str(end))
        df_tmp = ts.get_realtime_quotes(record_code[start:end])
        df = df.append(df_tmp)

    print(str(i) + ", " + df[-1:]['date'].values[0] + " " + df[-1:]['time'].values[0] + ", df len:" + str(df.__len__()))

    if i % 10 == 0:  #save to disk every 10*5 seconds
        df.to_pickle(dump + "." + str(file_num))
        file_num += 1
        print("saved pickle " + dump + "." + str(file_num))
        del df  #free memory every run 10 times
        df = pd.DataFrame()
        gc.collect()

    time.sleep(3)

print("Script completed")
os._exit(0)
