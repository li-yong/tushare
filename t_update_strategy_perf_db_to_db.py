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

#THIS Script read the remote source host db records, then merged into local host.

parser = OptionParser()

parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="debug enabled, use in development purpose")

parser.add_option("--source_host", dest="source_host", default='10.32.191.66', type="str", help="source host,  default '10.32.191.66'")

parser.add_option("--source_table", dest="source_table", default='zzz_pattern_perf_debug', type="str", help="source db table, default zzz_pattern_perf_debug, other options: pattern_perf_forex, pattern_perf ")

parser.add_option("--dest_host", dest="dest_host", default='127.0.0.1', type="str", help="destination host, default 127.0.0.1")

parser.add_option("--dest_table", dest="dest_table", default='zzz_pattern_perf_debug', type="str", help="destination db table,  default zzz_pattern_perf_debug, other options: pattern_perf_forex, pattern_perf ")

parser.add_option("--skip_backup_before_execute", action="store_true", dest="skip_backup_before_execute", default=False, help="Skip backup table before execute, default=False")

#parser.add_option("-b", "--db_tbl", dest="db_tbl",default='zzz_pattern_perf_debug', type="str",
#                  help="insert to which table in DB, default is zzz_pattern_perf_debug.\n \
#                   pattern_perf_forex, pattern_perf ")

(options, args) = parser.parse_args()

debug = options.debug
source_host = options.source_host
source_table = options.source_table
dest_host = options.dest_host
dest_table = options.dest_table
skip_backup_before_execute = options.skip_backup_before_execute

#debug:
#source_host="127.0.0.1"
#source_table='pattern_perf'

#dest_host='127.0.0.1'
#dest_table = 'zzz_pattern_perf_debug'

print("debug is " + str(debug))
print("source_host is " + str(source_host))
print("source_table is " + str(source_table))
print("dest_host is " + str(dest_host))
print("dest_table is " + str(dest_table))

user = "root"
password = "admin888.@_@"
host = "localhost"
database = "ryan_stock_db"
filestamp = time.strftime('%Y-%m-%d-%I:%M')
sqldump = database + "_" + dest_table + "_" + filestamp

if not skip_backup_before_execute:
    print("backing up table: " + dest_table + "@" + dest_host)
    os.popen("mysqldump -u %s -p%s -h %s  -e --opt -c %s %s| gzip -c > %s.gz" % (user, password, dest_host, database, dest_table, sqldump))
    print("table %s at %s backuped to %s " % (dest_table, dest_host, sqldump + ".gz"))
else:
    print("skip databasae table backup, " + dest_table + "@" + dest_host)

cnx_src = mysql.connector.connect(user='root', password='admin888.@_@', host=source_host, database='ryan_stock_db')

cnx_src.set_converter_class(finlib.NumpyMySQLConverter)

cursor_src = cnx_src.cursor()

cnx_dst = mysql.connector.connect(user='root', password='admin888.@_@', host=dest_host, database='ryan_stock_db')

cnx_dst.set_converter_class(finlib.NumpyMySQLConverter)

cursor_dst = cnx_dst.cursor()

#===== Read source
select_ptn_perf = ("SELECT * FROM `" + source_table + "` WHERE 1")
cursor_src.execute(select_ptn_perf)
record = cursor_src.fetchall()

if (record.__len__() == 0):
    print("there is not record in source table, exit")
    exit(0)

for i in range(record.__len__()):
    df = pd.DataFrame([''] * 2, columns=['date'])

    start_date = record[i][3]
    end_date = record[i][4]
    df.iloc[0, df.columns.get_loc('date')] = start_date
    df.iloc[1, df.columns.get_loc('date')] = end_date
    #print(record[i])
    #print(df)

    if True:
        id = record[i][0]
        code = record[i][1]
        ptn = record[i][2]
        date_s = record[i][3]
        date_e = record[i][4]
        trading_days = record[i][5]
        buy_signal_cnt = record[i][6]
        sell_signal_cnt = record[i][7]

        mea1 = record[i][8]
        med1 = record[i][9]
        min1 = record[i][10]
        max1 = record[i][11]
        var1 = record[i][12]
        skw1 = record[i][13]
        kur1 = record[i][14]
        uc1 = record[i][15]
        dc1 = record[i][16]

        mea2 = record[i][17]
        med2 = record[i][18]
        min2 = record[i][19]
        max2 = record[i][20]
        var2 = record[i][21]
        skw2 = record[i][22]
        kur2 = record[i][23]
        uc2 = record[i][24]
        dc2 = record[i][25]

        mea3 = record[i][26]
        med3 = record[i][27]
        min3 = record[i][28]
        max3 = record[i][29]
        var3 = record[i][30]
        skw3 = record[i][31]
        kur3 = record[i][32]
        uc3 = record[i][33]
        dc3 = record[i][34]

        mea5 = record[i][35]
        med5 = record[i][36]
        min5 = record[i][37]
        max5 = record[i][38]
        var5 = record[i][39]
        skw5 = record[i][40]
        kur5 = record[i][41]
        uc5 = record[i][42]
        dc5 = record[i][43]

        mea7 = record[i][44]
        med7 = record[i][45]
        min7 = record[i][46]
        max7 = record[i][47]
        var7 = record[i][48]
        skw7 = record[i][49]
        kur7 = record[i][50]
        uc7 = record[i][51]
        dc7 = record[i][52]

        mea10 = record[i][53]
        med10 = record[i][54]
        min10 = record[i][55]
        max10 = record[i][56]
        var10 = record[i][57]
        skw10 = record[i][58]
        kur10 = record[i][59]
        uc10 = record[i][60]
        dc10 = record[i][61]

        mea15 = record[i][62]
        med15 = record[i][63]
        min15 = record[i][64]
        max15 = record[i][65]
        var15 = record[i][66]
        skw15 = record[i][67]
        kur15 = record[i][68]
        uc15 = record[i][69]
        dc15 = record[i][70]

        mea20 = record[i][71]
        med20 = record[i][72]
        min20 = record[i][73]
        max20 = record[i][74]
        var20 = record[i][75]
        skw20 = record[i][76]
        kur20 = record[i][77]
        uc20 = record[i][78]
        dc20 = record[i][79]

        mea30 = record[i][80]
        med30 = record[i][81]
        min30 = record[i][82]
        max30 = record[i][83]
        var30 = record[i][84]
        skw30 = record[i][85]
        kur30 = record[i][86]
        uc30 = record[i][87]
        dc30 = record[i][88]

        mea60 = record[i][89]
        med60 = record[i][90]
        min60 = record[i][91]
        max60 = record[i][92]
        var60 = record[i][93]
        skw60 = record[i][94]
        kur60 = record[i][95]
        uc60 = record[i][96]
        dc60 = record[i][97]

        mea120 = record[i][98]
        med120 = record[i][99]
        min120 = record[i][100]
        max120 = record[i][101]
        var120 = record[i][102]
        skw120 = record[i][103]
        kur120 = record[i][104]
        uc120 = record[i][105]
        dc120 = record[i][106]

        mea240 = record[i][107]
        med240 = record[i][108]
        min240 = record[i][109]
        max240 = record[i][110]
        var240 = record[i][111]
        skw240 = record[i][112]
        kur240 = record[i][113]
        uc240 = record[i][114]
        dc240 = record[i][115]

    code = record[i][1]
    pattern = record[i][2]
    day_cnt = record[i][5]

    key = pattern + "_" + code

    if True:
        dict = {
            key: {
                '10_dncnt': dc10,
                '10_kurtosis': kur10,
                '10_max': max10,
                '10_mean': mea10,
                '10_median': med10,
                '10_min': min10,
                '10_nobs': '',
                '10_skewness': skw10,
                '10_upcnt': uc10,
                '10_variance': var10,
                '120_dncnt': dc120,
                '120_kurtosis': kur120,
                '120_max': max120,
                '120_mean': mea120,
                '120_median': med120,
                '120_min': min120,
                '120_nobs': '',
                '120_skewness': skw120,
                '120_upcnt': uc120,
                '120_variance': var120,
                '15_dncnt': dc15,
                '15_kurtosis': kur15,
                '15_max': max15,
                '15_mean': mea15,
                '15_median': med15,
                '15_min': min15,
                '15_nobs': '',
                '15_skewness': skw15,
                '15_upcnt': uc15,
                '15_variance': var15,
                '1_dncnt': dc1,
                '1_kurtosis': kur1,
                '1_max': max1,
                '1_mean': mea1,
                '1_median': med1,
                '1_min': min1,
                '1_nobs': '',
                '1_skewness': skw1,
                '1_upcnt': uc1,
                '1_variance': var1,
                '20_dncnt': dc20,
                '20_kurtosis': kur20,
                '20_max': max20,
                '20_mean': mea20,
                '20_median': med20,
                '20_min': min20,
                '20_nobs': '',
                '20_skewness': skw20,
                '20_upcnt': uc20,
                '20_variance': var20,
                '240_dncnt': dc240,
                '240_kurtosis': kur240,
                '240_max': max240,
                '240_mean': mea240,
                '240_median': med240,
                '240_min': min240,
                '240_nobs': '',
                '240_skewness': skw240,
                '240_upcnt': uc240,
                '240_variance': var240,
                '2_dncnt': dc2,
                '2_kurtosis': kur2,
                '2_max': max2,
                '2_mean': mea2,
                '2_median': med2,
                '2_min': min2,
                '2_nobs': '',
                '2_skewness': skw2,
                '2_upcnt': uc2,
                '2_variance': var2,
                '30_dncnt': dc30,
                '30_kurtosis': kur30,
                '30_max': max30,
                '30_mean': mea30,
                '30_median': med30,
                '30_min': min30,
                '30_nobs': '',
                '30_skewness': skw30,
                '30_upcnt': uc30,
                '30_variance': var30,
                '3_dncnt': dc3,
                '3_kurtosis': kur3,
                '3_max': max3,
                '3_mean': mea3,
                '3_median': med3,
                '3_min': min3,
                '3_nobs': '',
                '3_skewness': skw3,
                '3_upcnt': uc3,
                '3_variance': var3,
                '5_dncnt': dc5,
                '5_kurtosis': kur5,
                '5_max': max5,
                '5_mean': mea5,
                '5_median': med5,
                '5_min': min5,
                '5_nobs': '',
                '5_skewness': skw5,
                '5_upcnt': uc5,
                '5_variance': var5,
                '60_dncnt': dc60,
                '60_kurtosis': kur60,
                '60_max': max60,
                '60_mean': mea60,
                '60_median': med60,
                '60_min': min60,
                '60_nobs': '',
                '60_skewness': skw60,
                '60_upcnt': uc60,
                '60_variance': var60,
                '7_dncnt': dc7,
                '7_kurtosis': kur7,
                '7_max': max7,
                '7_mean': mea7,
                '7_median': med7,
                '7_min': min7,
                '7_nobs': '',
                '7_skewness': skw7,
                '7_upcnt': uc7,
                '7_variance': var7,
                'buy_signal_cnt': buy_signal_cnt,
                'sell_signal_cnt': sell_signal_cnt
            }
        }

    #update/create new records in dict into db.
    finlib.Finlib().create_or_update_ptn_perf_db_record(df, dict, code, day_cnt, cursor_dst, cnx_dst, dest_table)

print("Completed.")
os._exit(0)
