# coding: utf-8

import finlib
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import re
from scipy import stats
from multiprocessing import Pool
import multiprocessing
import sys
import shutil
import mysql.connector

debug = False
debug = True

#This script run daily after the marketing closed,
# Identify the price and volume


def backtest(array):
    inputF = array['inputF']

    print("Work on file " + inputF)

    #df = pd.read_csv(inputF, skiprows=1, header=None, names=['date','code','op','op_rsn','op_strength', 'c'])
    df = pd.read_csv(inputF)

    #df=df[df.op.notnull()]

    if df.__len__() <= 0:
        print("no operation in the csv, skip")
        return

    ### Do back test

    showplt = False
    acountWealth_init = 10000
    acountWealth = acountWealth_init
    acountWealth_last = 0
    stockCount = 0

    # init transaction day, buy count, sell count.
    day_cnt = 0
    b_sig_cnt = 0
    s_sig_cnt = 0
    b_cnt = 0
    s_cnt = 0
    max_win = 0
    max_draw = 0

    win_cnt = 0
    lose_cnt = 0

    actW = []
    #tmp = df
    for i in range(df.__len__()):
        # print "acount " + str(acountWealth)

        code = df.iloc[i, df.columns.get_loc('code')]
        op = df.iloc[i, df.columns.get_loc('op')]
        op_rsn = df.iloc[i, df.columns.get_loc('op_rsn')]
        op_strength = df.iloc[i, df.columns.get_loc('op_strength')]
        c = df.iloc[i, df.columns.get_loc('c')]

        date = df.iloc[i, df.columns.get_loc('date')]
        day_cnt += 1

        c_2d = c_5d = c_10d = c_20d = c_60d = c_120d = c
        dta_2d = dta5d = dta_10d = dta20d = dta_60d = dta_120d = 0

        if (i + 2 < df.__len__()):
            c_2d = df.iloc[i + 2, df.columns.get_loc('c')]
            dta_2d = (c_2d - c) / c
        if (i + 5 < df.__len__()):
            c_5d = df.iloc[i + 5, df.columns.get_loc('c')]
            dta_5d = (c_5d - c) / c
        if (i + 10 < df.__len__()):
            c_10d = df.iloc[i + 10, df.columns.get_loc('c')]
            dta_10d = (c_10d - c) / c
        if (i + 20 < df.__len__()):
            c_20d = df.iloc[i + 20, df.columns.get_loc('c')]
            dta_20d = (c_20d - c) / c
        if (i + 60 < df.__len__()):
            c_60d = df.iloc[i + 60, df.columns.get_loc('c')]
            dta_60d = (c_60d - c) / c
        if (i + 120 < df.__len__()):
            c_120d = df.iloc[i + 120, df.columns.get_loc('c')]
            dta_120d = (c_120d - c) / c

        import math
        if type(op) == float and math.isnan(op):
            continue
        else:
            #showplt = True
            #if re.match("B",op):
            if re.match("S", op):  #debug
                #plt.annotate('b signal' + " " + p + date[i], (i, close[i]))
                #print "B" + " " + p[3:] + " " + date[i] + " " + str(open[i]) + " " + str(high[i]) + " " + str(
                #    low[i]) + " " + str(close[i])
                b_sig_cnt += 1

                if (stockCount <= 0):
                    # Buy(acountWealth)
                    stockCount = acountWealth / c
                    acountWealth -= (stockCount * c) * 0.0003  # handle fee of buy
                    actW.append(acountWealth)
                    acountWealth_last = acountWealth
                    print("brought code " +code+","+ str(stockCount) + ", amount " + str(acountWealth) + " ," \
                          + str((acountWealth - acountWealth_init) * 100 / acountWealth_init) \
                          + ", " + str(date) + ", " + str(c) \
                          +". op "+op+", op_rsn "+op_rsn+", op_strength"+str(op_strength))
                    b_cnt += 1
                else:
                    print("cannot buy as already have this stock")

            #if re.match("S",op):
            if re.match("B", op):  #debug

                #plt.annotate('s signal' + " " + p + date[i], (i, close[i]))
                #print "S" + " " + p[3:] + " " + date[i] + " " + str(open[i]) + " " + str(high[i]) + " " + str(
                #    low[i]) + " " + str(close[i])
                s_sig_cnt += 1

                if (stockCount > 0):
                    # sell(acountWealth)
                    acountWealth = stockCount * c
                    acountWealth -= (stockCount * c) * 0.0013
                    print("sell code " +code+", "+ str(stockCount) + ", amount " + str(acountWealth) + " ," \
                          + str((acountWealth - acountWealth_init) * 100 / acountWealth_init) \
                          + ", " + str(date) + ", " + str(c) \
                          +". op: "+op+", op_rsn: "+op_rsn+", op_strength: "+str(op_strength))
                    s_cnt += 1
                    stockCount = 0
                    actW.append(acountWealth)

                    if (acountWealth - acountWealth_init > 0) and (((acountWealth - acountWealth_init) * 100 / acountWealth_init) > max_win):
                        max_win = ((acountWealth - acountWealth_init) * 100 / acountWealth_init)
                        print("max_win:" + str(max_win))

                    if (acountWealth - acountWealth_init < 0) and (((acountWealth - acountWealth_init) * 100 / acountWealth_init) < max_draw):
                        max_draw = ((acountWealth - acountWealth_init) * 100 / acountWealth_init)
                        print("max_draw:" + str(max_draw))

                    if acountWealth > acountWealth_last:
                        win_cnt += 1
                    else:
                        lose_cnt += 1

                else:
                    print("cannot sell as stockCount<=0")

                    # if False:
                    # if showplt:
                    # plt.plot(close)
                    # plt.show()
                    # plt.plot(close)
                    # plt.xlabel(p)
                    # plt.plot(actW)
    code = df.iloc[0, df.columns.get_loc('code')]
    growth = (acountWealth - acountWealth_init) / acountWealth_init * 100
    print("code " + code + ", pattern " + "op_rsn_TODO" + ", from " + str(acountWealth_init) + " to " + str(
        acountWealth) + ", increase " + str(growth) \
          + ", max_win:" + str(max_win) + ", max_draw:" + str(max_draw) \
          + ", win_cnt:" + str(win_cnt) + ", lose_cnt:" + str(lose_cnt) \
          + ", trading day:" + str(day_cnt) + ", buy_signal:" + str(b_sig_cnt) + ", sell_signal:" + str(
        s_sig_cnt))
    c_begin = float(df['c'][0:1])
    c_end = float(df['c'][-1:])
    no_operation_increase = ((c_end - c_begin) * 100) / c_begin
    no_operation_increase = no_operation_increase * 0.98  # say 2% buy and sell cost

    # update to DB

    cnx = mysql.connector.connect(user='root', password='admin888.@_@', host='127.0.0.1', database='ryan_stock_db')

    cnx.set_converter_class(finlib.NumpyMySQLConverter)

    cursor = cnx.cursor()

    add_s_p_perf = ("INSERT INTO stock_pattern_overview " "(ID, stockID, pattern, date_s, date_e, trading_days, buy_signal_cnt, sell_signal_cnt, \
                    buy_cnt, sell_cnt, win_cnt, \
                    lose_cnt, max_win, max_draw, increase_percent, no_op_in) " "VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

    data_s_p_perf = ('', code, "PATTEN_TODO", df['date'][0:1].values[0], df['date'][-1:].values[0], \
                     day_cnt, b_sig_cnt, s_sig_cnt, \
                     b_cnt, s_cnt, \
                     win_cnt, lose_cnt, max_win, max_draw, growth, no_operation_increase)

    cursor.execute(add_s_p_perf, data_s_p_perf)
    cnx.commit()
    cursor.close()
    cnx.close()

    print(1)


### Main Start ###
if debug:
    backtest({
        'inputF': "/home/ryan/DATA/tmp/pv/pv_SH00001.csv",
    })
else:
    array = []
    #multicore implmentation:
    for root, dirs, files in os.walk("/home/ryan/DATA/tmp/pv"):
        pass

    #each code generate a csv
    for file in files:
        array.append({
            'inputF': "/home/ryan/DATA/tmp/pv/" + file,
        })  #array of dict
    cpu_count = multiprocessing.cpu_count()
    pool = Pool(cpu_count - 1)  #leave one core free

    pool.map(backtest, array)
    pool.close()
    pool.join()

print("Script completed")
