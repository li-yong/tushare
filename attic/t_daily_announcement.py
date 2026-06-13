# coding: utf-8

import finlib
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import re
import sys
import os.path
import os
import numpy as np
from optparse import OptionParser

#This script run daily after the marketing closed,
#     Please run after the csv file updated today's data.
#It show the stocks which meet patter (buy or sell) point.
#The result can be a reference for next day's trading.

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

logging.info(__file__+" "+"\n")
logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

parser = OptionParser()

parser.add_option("-e", "--exam_date", dest="exam_date", help="exam_date, YYYY-MM-DD, no default value, missing will calc the nearest trading day, most time is today")

(options, args) = parser.parse_args()

exam_date = options.exam_date

dir_base = '/home/ryan/DATA/announcement'

if exam_date is None:
    #print("exam_date: " + exam_date)
    for look_ahead in range(7):
        todaySl = datetime.strptime(finlib.Finlib().get_last_trading_day(), '%Y%m%d').strftime('%Y-%m-%d')
        exam_date = datetime.strptime(todaySl, '%Y-%m-%d') - timedelta(look_ahead)  # suppose run the AG on next day morning.
        exam_date = exam_date.strftime('%Y-%m-%d')
        print("searching latest reg or sse file in local, checking " + exam_date)

        file_reg = dir_base + '/reg/list/' + str(datetime.strptime(exam_date, '%Y-%m-%d').year) + '/' + exam_date + '.csv'
        file_sse = dir_base + '/sse/list/' + str(datetime.strptime(exam_date, '%Y-%m-%d').year) + '/' + exam_date + '.csv'

        if os.path.isfile(file_reg) and os.path.isfile(file_sse):
            print(("based on local file search, exam_date set to: " + exam_date))
            break
else:
    file_reg = dir_base + '/reg/list/' + str(datetime.strptime(exam_date, '%Y-%m-%d').year) + '/' + exam_date + '.csv'
    file_sse = dir_base + '/sse/list/' + str(datetime.strptime(exam_date, '%Y-%m-%d').year) + '/' + exam_date + '.csv'

#exam_date="2017-11-17"
base_dir = '/home/ryan/DATA/result'
dump = "announcement_" + exam_date + ".pickle"
update_latest_list = True  # The upper driver use this to control if update it's output to latest_list.
use_lastest_list_as_input = True  # The lower driver use this to control if it use latest_list as input.
#use_lastest_list_as_input = False # The lower driver use this to control if it use latest_list as input.

#start main
engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')

#display result setting
#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1024)

positive_kw = {
    'p_zen_chi': '增持',
    'P_fen_hong': '分红',
    'P_ksjs': '亏损大幅减少',
    'P_yjtg': '业绩大幅提高',
    'P_yjzz': '业绩大幅增长',
    'P_yjts': '业绩大幅提升',
    'P_gjbd': '股价大幅波动',
}

negative_kw = {
    'n_jian_chi': '减持',
    'n_jie_jin': '解禁',
    'n_ci_zhi': '辞职',
}

# Gong gao, Announcement
#cd ~/repo/trading/lib/China_stock_announcement_ryan/python_scraw
#python cninfo_main.py reg 20171222  --> /home/ryan/DATA/announcement/reg/list/2017/2017-12-22.csv
#python cninfo_main.py sse 20171222  --> /home/ryan/DATA/announcement/sse/list/2017/2017-12-22.csv
#today = 1 #cannot remember what's this, remove it if no use. 20171129

df = pd.DataFrame()

if os.path.isfile(file_sse):
    print("loading " + file_sse)
    df_sse=pd.read_csv(file_sse, dtype=str, names=['anncid', 'symbol', 'abbv', 'title', 'anday', 'antime', \
                            'file_type', 'url', 'valid','gettime'])
    df = df_sse
else:
    print("no such file " + file_sse)

if os.path.isfile(file_reg):
    print("loading " + file_reg)
    df_reg=pd.read_csv(file_reg, dtype=str, names=['anncid', 'symbol', 'abbv', 'title', 'anday', 'antime', \
                            'file_type', 'url', 'valid','gettime'])
    df = df.append(df_reg, ignore_index=True)

else:
    print("no such file " + file_reg)

#df['symbol'] = df['symbol'].astype(str)

if df.__len__() <= 0:
    print("Not see any announcemen on day " + exam_date)
    print("Empty file " + file_reg)
    print("Empty file " + file_sse)
    exit(1)
else:
    s = df['symbol'].dropna()

df_result = pd.DataFrame(columns=['code', 'name', 'annc_cnt', 'P_SUM', 'N_SUM', 'op_rsn', 'op_strength', 'date', 'close_p', 'hit_ptn_cnt'])  #

for k in list(positive_kw.keys()):
    df_result[k] = pd.Series()

for k in list(negative_kw.keys()):
    df_result[k] = pd.Series()

df_result.set_index('code')

for i in range(df.__len__()):
    symbol = df.iloc[i, df.columns.get_loc('symbol')]  #000685
    #if not re.match('^(\d){6}$', symbol):
    #    s.drop(i[0],inplace=True)
    abbv = df.iloc[i, df.columns.get_loc('abbv')]  #中山公用
    anday = df.iloc[i, df.columns.get_loc('anday')]  #2018-03-23
    gettime = df.iloc[i, df.columns.get_loc('gettime')]  #2018-03-23 15:06:07
    url = df.iloc[i, df.columns.get_loc('url')]
    anncid = df.iloc[i, df.columns.get_loc('anncid')]
    valid = df.iloc[i, df.columns.get_loc('valid')]
    title = df.iloc[i, df.columns.get_loc('title')]

    df_result.at[symbol, 'name'] = abbv  #compliance with t_summary.py
    df_result.at[symbol, 'code'] = symbol
    df_result.at[symbol, 'date'] = anday

    if pd.isnull(abbv) or pd.isnull(symbol):
        continue

    symbol_market = finlib.Finlib().add_market_to_code_single(symbol)
    price_file = "/home/ryan/DATA/DAY_Global/AG/" + symbol_market + ".csv"

    if os.path.isfile(price_file):
        tmp_df = pd.read_csv(price_file, skiprows=1, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
        #last_day = tmp_df.loc[tmp_df['date'] == anday]  #or tmp_df[-2:-1]
        last_day_df = tmp_df[-1:]
        if tmp_df.__len__() <= 0:  # tmp_df has no header
            print("empty file " + price_file)
            continue

        close_p = last_day_df['c'].values[0]
        df_result.at[symbol, 'close_p'] = close_p

    if pd.isnull(df_result.at[symbol, 'annc_cnt']):
        df_result.at[symbol, 'annc_cnt'] = 0

    for k in list(positive_kw.keys()) + list(negative_kw.keys()) + ['P_SUM', 'N_SUM']:
        #if symbol == '600438' and k=='P_SUM':
        #    print("600438: "+ str(df_result.get_value(symbol, k)))

        if pd.isnull(df_result.at[symbol, k]):
            df_result.at[symbol, k] = 0

    for k in list(positive_kw.keys()):
        kw = positive_kw[k]
        #print(symbol )
        #print( kw)
        #print( title)

        if title.find(kw) >= 0:

            print(symbol + " POSITIVE " + abbv + " match " + kw + ", " + title + ", " + anday)
            df_result.at[symbol, k] = df_result.at[symbol, k] + 1

            df_result.at[symbol, 'P_SUM'] = df_result.at[symbol, 'P_SUM'] + 1

            df_result.at[symbol, 'op_rsn'] = 'Positive Announce cnt ' + str(df_result.at[symbol, 'P_SUM'])

            df_result.at[symbol, 'op_strength'] = df_result.at[symbol, 'P_SUM']

            df_result.at[symbol, 'hit_ptn_cnt'] = df_result.at[symbol, 'P_SUM']

            #if symbol == '002390':
            #    df_debug = df_result.loc[df_result['code'] == '002390']
            #    print(df_debug)

    for k in list(negative_kw.keys()):
        kw = negative_kw[k]

        if title.find(kw) >= 0:
            print(symbol + " NEGATIVE " + abbv + " match " + kw + ", " + title + ", " + anday)
            df_result.at[symbol, k] = df_result.at[symbol, k] + 1
            df_result.at[symbol, 'N_SUM'] = df_result.at[symbol, 'N_SUM'] + 1

    df_result.at[symbol, 'annc_cnt'] = df_result.at[symbol, 'annc_cnt'] + 1

for i in range(df_result.__len__()):
    code = df_result.iloc[i, df_result.columns.get_loc('code')]

    if pd.isnull(code):
        print("null on code")
        continue

    #print(code)
    if re.match('^6', code):
        code = "SH" + code
    elif re.match('^[0|3]', code):
        code = "SZ" + code
    else:
        print(("Warn: ignore UNKNOWN CODE " + code))
        #exit(1)

    df_result.iloc[i, df_result.columns.get_loc('code')] = code

df_positive = df_result.loc[df_result['P_SUM'] > 0]
print("Positive DataFrame")
print(df_positive)

csvf = "/home/ryan/DATA/result/today/announcement.csv"
#df_result.to_csv(csvf, index=False)
df_positive.to_csv(csvf, index=False)  #only change the positive records

print(("Today Announcement result saved to " + csvf))
#exit(0)
os._exit(0)
