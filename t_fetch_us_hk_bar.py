# coding: utf-8

import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas
import math
import re
from scipy import stats
import finlib
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs
import finlib
from optparse import OptionParser

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

logging.info(__file__+" "+"\n")
logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

parser = OptionParser()

parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")
parser.add_option("--force_fetch", action="store_true", dest="force_fetch", default=False, help="force fetch data")

(options, args) = parser.parse_args()

selected = options.selected
stock_global = options.stock_global
force_fetch = options.force_fetch

if (stock_global not in ['US','US_HOLD','HK', 'HK_HOLD']):
    logging.fatal("stock_global only can be in ['US','US_HOLD','HK', 'HK_HOLD']")
    exit()

# This script to get US,HK stock daily bar.
# delete /home/ryan/DATA/pickle/instrument.csv to fetch again.


def get_hk_us(df, cons, start_date, appendix, todayS, force_fetch):
    default_date_d = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    fast_fetch = True  #for full fetch
    fast_fetch = False  #for daily update

    #df = df[df['code']=='AAPL']  #ryan debug

    for i in range(df.__len__()):
        a_wait_time = 0  #looks no wait works even for a frequent fetch. Just re-request the conns

        if i % 500 == 0:  #renew connection every N times
            ts.close_apis(cons)
            cons = ts.get_apis()

        start_date_req = start_date  #start_date_req: date sent to the api

        code = df.iloc[i]['code']
        name = df.iloc[i]['name']
        #cons = ts.get_apis()  #<<<<< renew connection every time

        logging.info(__file__+" "+str(i + 1) + "/" + str(df.__len__()) + " get " + str(code) + " " + str(name) + ". " + appendix + ". ")  #print without newline
        #a_csv = '/home/ryan/DATA/DAY_Global/' + str(code) + '.' + appendix  # WUBA.CH
        #a_csv = '/home/ryan/DATA/DAY_Global/' + appendix + '/' + str(code) + '.' + appendix  # DATA/DAY_Global/CH/WUBA.CH
        a_csv = '/home/ryan/DATA/DAY_Global/' + appendix + '/' + str(code) + '.csv'  # DATA/DAY_Global/CH/WUBA.CH

        #fetch all from begin. As saved csv corrupt format sometimes.
        if os.path.isfile(a_csv):
            os.remove(a_csv)

        #if appendix == 'KH':
        #    a_csv = '/home/ryan/DATA/DAY_Global/HK/' + str(code) + '.csv'

        #if finlib.Finlib().is_cached(a_csv, day=1):
        #    logging.info(__file__+" "+"ignore because file was updated within 24 hours, " + a_csv)
        #    continue

        df_tmp = pandas.DataFrame()  #exists csv
        a_stock_df = pandas.DataFrame()  #data fetched this time.

        #resume the last update. Enable this when run mutliple times in same day. To get a fully update quickly.
        if fast_fetch:
            a_wait_time = 0
            if os.path.isfile(a_csv):
                continue

        if finlib.Finlib().is_cached(a_csv) and (not force_fetch):
            logging.info(__file__+" "+"file has been updated in a day, not fetch again. " + a_csv)
            continue

        if os.path.isfile(a_csv):

            if os.stat(a_csv).st_size == 0:
                logging.info(__file__+" "+"empty file, skip. " + a_csv)
                continue

            df_tmp = pd.read_csv(a_csv, converters={'code': str, 'datetime': str})
            last_row = df_tmp[-1:]
            last_date = last_row['datetime'].values[0]

            if last_date == '':
                continue

            next_date = datetime.datetime.strptime(str(last_date), '%Y-%m-%d') + datetime.timedelta(1)
            a_week_before_date = datetime.datetime.strptime(todayS, '%Y-%m-%d') - datetime.timedelta(60)

            #if next_date > datetime.datetime.today():
            if next_date.strftime('%Y-%m-%d') > todayS:
                logging.info(__file__+" "+"file already updated, not fetching again. " + a_csv + ". updated to " + last_date)
                continue

            #last date in csv is 7 days ago, most likely the source is not update, so skip this csv.
            #logging.info(__file__+" "+"Next "+next_date.strftime('%Y-%m-%d'))
            #logging.info(__file__+" "+"a week before "+ a_week_before_date.strftime('%Y-%m-%d'))
            if next_date.strftime('%Y-%m-%d') < a_week_before_date.strftime('%Y-%m-%d'):
                logging.info(__file__+" "+"file too old to updated, not fetching. " + a_csv + ". updated to " + last_date)
                #continue

            if next_date > default_date_d:  #csv already have data
                start_date_req = next_date.strftime('%Y-%m-%d')
                logging.info(__file__+" "+"append exist csv from " + start_date_req + ". ")
            else:
                logging.info(__file__+" "+"will do a full update, since " + start_date_req + ". ")

        try:
            exc_info = sys.exc_info()
            print("code " + str(code))
            a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
            if str(a_stock_df) != 'None':
                logging.info(__file__+" "+"fetched df len " + str(a_stock_df.__len__()))

            #time.sleep(2)
            if str(a_stock_df) == 'None':
                logging.info(__file__+" "+"ts.bar return None. retry " + str(code) + ". 1st. ")
                ts.close_apis(cons)
                time.sleep(a_wait_time)
                cons = ts.get_apis()
                a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
        except:
            logging.info(__file__+" "+"\tcaught exception when getting data, try to renew cons")
            ts.close_apis(cons)
            time.sleep(a_wait_time)

            cons = ts.get_apis()
            a_stock_df = ts.bar(code, conn=cons, asset='X', adj='qfq', start_date=start_date_req, end_date='')
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info

        if str(a_stock_df) == 'None':
            logging.info(__file__+" "+"ts.bar return None for code. Retry exhausted. " + str(code))
            continue

    #At this stage, a_stock_df should be valid.

        try:
            exc_info = sys.exc_info()
            a_stock_df = a_stock_df.reindex(index=a_stock_df.index[::-1])  #revert

            a_stock_df = a_stock_df.reset_index()

            logging.info(__file__+" "+" len " + str(a_stock_df.__len__()) + ". ")

            df_name = pd.DataFrame([name] * a_stock_df.__len__(), columns=['name'])
            df_short_name = pd.DataFrame([appendix] * a_stock_df.__len__(), columns=['short_name'])
            a_stock_df = a_stock_df.join(df_name)  #append column
            a_stock_df = a_stock_df.join(df_short_name)  #append column

            #logging.info(a_stock_df.head(2)) #debug.

            cols = ['code', 'datetime', 'open', 'high', 'low', 'close', 'vol', 'name']
            a_stock_df = a_stock_df[cols]  #re-arrange adjust columns

            a_stock_df['open'] = a_stock_df['open'].round(2)
            a_stock_df['high'] = a_stock_df['high'].round(2)
            a_stock_df['low'] = a_stock_df['low'].round(2)
            a_stock_df['close'] = a_stock_df['close'].round(2)
            a_stock_df['vol'] = a_stock_df['vol'].round(2)
            a_stock_df['datetime'] = a_stock_df['datetime'].astype('str')
            a_stock_df.rename(columns={"datetime": "date"}, inplace=True)

            a_stock_df = df_tmp.append(a_stock_df, ignore_index=True)
            finlib.Finlib().pprint(a_stock_df.iloc[-1:])
            a_stock_df.to_csv(a_csv, encoding='UTF-8', index=False)
            logging.info(__file__+" "+"saved " + a_csv)
        except:
            logging.info(__file__+" "+"\tcaught exception when processing to csv")
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info

        pass


#instrument.csv
#/home/ryan/DATA/pickle/market.csv
#market,category,name,short_name
#31,2,香港主板,KH
#40,11,中国概念股,CH
#41,11,美股知名公司,MG
#48,2,香港创业板,KG
#74,13,美国股票,US  <<<<<  13,74,APLE,Apple,APLE
# 71,2,港股通,GH <<< 00001.KH move to this market

cons = ts.get_apis()
start_date = '2010-01-01'

rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
out_dir = rst['out_dir']
csv_dir = rst['csv_dir']
stock_list = rst['stock_list']

todayS = datetime.datetime.today().strftime('%Y-%m-%d')
get_hk_us(stock_list, cons, start_date, stock_global, todayS=todayS, force_fetch=force_fetch)

logging.info('script completed')
os._exit(0)
