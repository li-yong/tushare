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
import shutil
import mysql.connector
import math
from optparse import OptionParser
from scipy import stats
import sys

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#This script read the csv files came from 't_daily_pattern_Hit_price_Volume.py (debug=false, max_exam_day = 220000)
#     Then calculate the performance of each straegy, include price change % after 2days, 5, 10, 20, 60, 120
#      and insert to the MYSQL db.
#This script doesn't need to run everyday.
#step1: update DATA/DAY
#Step3: python t_daily_pattern_Hit_Price_Volume.py  --max_exam_day 300000  #not debug-->so parse all the stocks.
#Step4: Mysql truncate table stock_pattern_pereformance
#Step5: python t_monthly_strage_perf_gathering.py
#Step6: Once today data triggered a patter
# , <--- This is checked by  t_daily_pattern_Hit_Price_Volume.py -m 22, and t_daily_summary.py
#       check the pattern-Stock perf pair of 2,5,10,20,60,120D to decide the operation.
#       - Come to a holding period, expected win percent.

parser = OptionParser()

parser.add_option("-s", "--single_process", action="store_true", dest="single_process", default=False, help="using single process, otherwise using multiple process")

parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="debug enabled, use in development purpose")

parser.add_option("-t", "--truncate_tbl", action="store_true", dest="truncate_tbl", default=False, help="trucate db table pattern_perf_(debug/forex) before insert.")

parser.add_option("-n", "--skip_backup_before_execute", action="store_true", dest="skip_backup_before_execute", default=False, help="Skip backup table before execute")

#parser.add_option("-l", "--to_table", dest="to_table", default='zzz_pattern_perf_debug',
#                  help="save to DB table.")

parser.add_option("-b", "--db_tbl", dest="db_tbl", default='zzz_pattern_perf_debug', type="str", help="insert to which table in DB, default is zzz_pattern_perf_debug.\n \
                   pattern_perf_forex, pattern_perf ")

(options, args) = parser.parse_args()

debug = options.debug
single_process = options.single_process
truncate_tbl = options.truncate_tbl
skip_backup_before_execute = options.skip_backup_before_execute
db_tbl = options.db_tbl

user = "root"
password = "admin888.@_@"
host = "localhost"
database = "ryan_stock_db"
filestamp = time.strftime('%Y-%m-%d-%I:%M')

if not skip_backup_before_execute:
    print("backing up db table: " + db_tbl)
    os.popen("mysqldump -u %s -p%s -h %s  -e --opt -c %s %s| gzip -c > %s.gz" % (user, password, host, database, db_tbl, database + "_" + db_tbl + "_" + filestamp))
    print("table %s backup to %s " % (db_tbl, database + "_" + db_tbl + "_" + filestamp + ".gz"))
else:
    print("skip databasae table backup, " + db_tbl)

print("debug is " + str(debug))
print("single_process is " + str(single_process))
print("truncate_tbl is " + str(truncate_tbl))
print("db_tbl is " + str(db_tbl))

dict = {}
progress_run = 0
toal_run = 0

# Identify the price and volume


def updatePtnPerf(i, c, op, df, ptn, code):
    global dict

    code_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(1)  # EUR_USD_S_talib_xxx
    b_or_s_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(2)  # EUR_USD_S_talib_xxx
    ptn_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(3)  # EUR_USD_S_talib_xxx

    code_in_ptn = re.sub("_", '', code_in_ptn)

    ptn = code_in_ptn + b_or_s_in_ptn + ptn_in_ptn  # it is not EURUSD_S_talib_xxx

    tm_list = ['1', '2', '3', '5', '7', '10', '15', '20', '30', '60', '120', '240']

    for tm in tm_list:  #time window, of the csv input file.
        tm = str(tm)
        #eval("c_"+tm+" = c") #c_2 = c
        #eval("dta_"+tm+" = 0") #dta_2 = 0

        c_x = c
        dta_x = 0

        if (i + int(tm) < df.__len__()):
            #c_2d = df.iloc[i + 2, df.columns.get_loc('c')]
            c_x = df.iloc[i + int(tm), df.columns.get_loc('c')]

            if c == 0:
                dta_x = 0
            else:
                dta_x = round((c_x - c) / c, 8)

            dict[ptn + "_" + code]["c_" + tm + "_arr"].append(dta_x)

            #debug
            #if dict[ptn+"_"+code]["c_"+tm+"_arr"].__len__() > 2:
            #    pass

            if dta_x > 0:
                dict[ptn + "_" + code][tm + "_upcnt"] += 1
            else:
                dict[ptn + "_" + code][tm + "_dncnt"] += 1

    if re.match("B", op, re.IGNORECASE):
        dict[ptn + "_" + code]["buy_signal_cnt"] += 1

    if re.match("S", op, re.IGNORECASE):
        dict[ptn + "_" + code]["sell_signal_cnt"] += 1

    tm_list = ['1', '2', '3', '5', '7', '10', '15', '20', '30', '60', '120', '240']

    for tm in tm_list:  #time window, of the csv input file.
        tm = str(tm)
        a = dict[ptn + "_" + code]["c_" + tm + "_arr"]
        #print a.__len__()

        if a.__len__() <= 0:
            dict[ptn + "_" + code][tm + "_nobs"] = 0
            dict[ptn + "_" + code][tm + "_min"] = 0
            dict[ptn + "_" + code][tm + "_max"] = 0
            dict[ptn + "_" + code][tm + "_mean"] = 0
            dict[ptn + "_" + code][tm + "_variance"] = 0
            dict[ptn + "_" + code][tm + "_skewness"] = 0
            dict[ptn + "_" + code][tm + "_kurtosis"] = 0
            dict[ptn + "_" + code][tm + "_median"] = 0
        else:
            b = stats.describe(a)
            dict[ptn + "_" + code][tm + "_nobs"] = round(b.nobs, 8)  #2_nobs -> 2nob.  var name --> db column name
            dict[ptn + "_" + code][tm + "_min"] = round(b.minmax[0], 8)  #2_min ->2min
            dict[ptn + "_" + code][tm + "_max"] = round(b.minmax[1], 8)  #2_max ->2max
            dict[ptn + "_" + code][tm + "_mean"] = round(b.mean, 8)  #2_mean ->2mea
            dict[ptn + "_" + code][tm + "_variance"] = round(b.variance, 8)  #2_variance --> 2var
            dict[ptn + "_" + code][tm + "_skewness"] = round(b.skewness, 8)  #2_skewness --> 2ske
            dict[ptn + "_" + code][tm + "_kurtosis"] = round(b.kurtosis, 8)  #2_kurtosis --> 2kur
            dict[ptn + "_" + code][tm + "_median"] = round(np.median(np.array(a)), 8)  #2_median --> 2_med


def backtest(array):
    inputF = array['inputF']

    global dict
    global progress_run
    progress_run += 1

    print("Work on file " + inputF + " " + str(progress_run) + "/" + str(toal_run))

    #df = pd.read_csv(inputF, skiprows=1, header=None, names=['date','code','op','op_rsn','op_strength', 'c'])
    df = pd.read_csv(inputF)

    #df=df[df.op.notnull()]

    if df.__len__() <= 0:
        print("no operation in the csv, skip")
        return

    ### Do back test
    # init transaction day, buy count, sell count.
    day_cnt = 0
    b_sig_cnt = 0
    s_sig_cnt = 0

    min_win = 0
    min_lost = 0
    mid_win = 0
    mid_lost = 0
    avg_win = 0
    avg_lost = 0

    code = df.iloc[0, df.columns.get_loc('code')]
    code = re.sub("_", '', code)  #change EUR_USD to EURUSD. TODO, need change it from the oanda get source.

    # init all the pattern dict
    for i in range(df.__len__()):
        op = df.iloc[i, df.columns.get_loc('op')]
        #print op

        if op == "na":
            continue
        elif re.match('.*float.*', str(type(op))) and math.isnan(op):
            continue
        else:
            ptn_arr = df.iloc[i, df.columns.get_loc('op_rsn')].split(';')
            for ptn in ptn_arr:
                if ptn == '':  #ptn: 'EURUSD_B_talib_CDLBELTHOLD'
                    continue
                elif ptn == 'pv_ignore':
                    continue

                code_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(1)  #'EURUSD'
                b_or_s_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(2)  #'_B_'
                ptn_in_ptn = re.match('(.*)(_[s|b]_)(.*)', ptn, re.IGNORECASE).group(3)  #talib_CDLBELTHOLD

                code_in_ptn = re.sub("_", '', code_in_ptn)

                ptn = code_in_ptn + b_or_s_in_ptn + ptn_in_ptn  # EURUSD_S_talib_xxx.  e.g, 'EURUSD_B_talib_CDLBELTHOLD'

                dict[ptn + "_" + code] = {}  #dict['EURUSD_B_talib_CDLBELTHOLD_EURUSD']

                tm_list = ['1', '2', '3', '5', '7', '10', '15', '20', '30', '60', '120', '240']
                for tm in tm_list:
                    tm = str(tm)
                    dict[ptn + "_" + code]["c_" + tm + "_arr"] = []  #c_2_arr
                    dict[ptn + "_" + code][tm + "_upcnt"] = 0
                    dict[ptn + "_" + code][tm + "_dncnt"] = 0

                dict[ptn + "_" + code]["buy_signal_cnt"] = 0
                dict[ptn + "_" + code]["sell_signal_cnt"] = 0

    for i in range(df.__len__()):
        #print "i "+str(i)
        #if i==18:
        #    pass #debug
        #code = df.iloc[i, df.columns.get_loc('code')]
        op = df.iloc[i, df.columns.get_loc('op')]
        c = df.iloc[i, df.columns.get_loc('c')]
        date = df.iloc[i, df.columns.get_loc('date')]
        day_cnt += 1

        #print(op)
        if op == "na":
            continue
        if re.match('.*float.*', str(type(op))) and math.isnan(op):
            continue
        else:
            #op = df.iloc[i, df.columns.get_loc('op')]
            op_arr = op.split(";")
            op_arr.remove("na")

            op_rsn = df.iloc[i, df.columns.get_loc('op_rsn')]
            ptn_arr = op_rsn.split(';')
            ptn_arr.remove("pv_ignore")

            #op_strength = df.iloc[i, df.columns.get_loc('op_strength')]
            #op_strength_arr=op_strength.split(';')

            for j in range(ptn_arr.__len__()):
                #print "j "+str(j)
                updatePtnPerf(i, c, op_arr[j], df, ptn_arr[j], code)

    # update to DB
    code = df.iloc[0, df.columns.get_loc('code')]
    code = re.sub("_", '', code)

    cnx = mysql.connector.connect(user='root', password='admin888.@_@', host='127.0.0.1', database='ryan_stock_db')

    cnx.set_converter_class(finlib.NumpyMySQLConverter)

    cursor = cnx.cursor()

    #update/create new records in dict into db.
    finlib.Finlib().create_or_update_ptn_perf_db_record(df, dict, code, day_cnt, cursor, cnx, db_tbl)

    cursor.close()
    cnx.close()


### Main Start ###
if __name__ == '__main__':
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))
    array = []

    if truncate_tbl:
        sql = "TRUNCATE TABLE " + db_tbl
        print(sql)

        cnx = mysql.connector.connect(user='root', password='admin888.@_@', host='127.0.0.1', database='ryan_stock_db')

        cnx.set_converter_class(finlib.NumpyMySQLConverter)

        cursor = cnx.cursor()
        cursor.execute(sql)
        cnx.commit()
        cursor.close()
        cnx.close()

    source_dir = "/home/ryan/DATA/tmp/pv/AG/"

    if debug:
        source_dir = "/home/ryan/DATA/tmp/pv.dev"

    for root, dirs, files in os.walk(source_dir):
        pass

    #each code generate a csv
    for file in files:
        array.append({
            'inputF': source_dir + "" + file,
        })

    if single_process or debug:  #debug always use single_process
        #singl process implementation
        cnt = 1
        for a in array:
            print("progress " + str(cnt) + " of " + str(array.__len__()))
            backtest(a)
            cnt += 1

        print("script (single process) completed, Done!")
        os._exit(0)
    else:
        #multicore implmentation:
        #array of dict
        cpu_count = multiprocessing.cpu_count()
        print("saw cpu_count " + str(cpu_count))
        toal_run = int(files.__len__() / (cpu_count - 2)) + 2

        if cpu_count == 1:
            pool = Pool(processes=cpu_count)  #leave one core free
        else:
            pool = Pool(processes=cpu_count - 1)  #leave one core free

        pool.map(backtest, array)
        pool.close()
        pool.terminate()
        pool.join()
        print("script (multiprocess) completed, Done!")
        os._exit(0)
