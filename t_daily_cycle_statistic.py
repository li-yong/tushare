# coding: utf-8

import finlib
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np

import pandas
import math
import re
from scipy import stats
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs
import finlib

from sklearn.cluster import KMeans
from calendar import monthrange

import stockstats
import tabulate
import os
import logging
import calendar
from optparse import OptionParser

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)



def analyze_period_increase(target_stock,start_period='20160101'):
    if target_stock == 'SP500':
        file = "/home/ryan/DATA/DAY_Global/stooq/US_INDEX/SP500.csv"
    elif target_stock == 'NASDAQ100':
        file = "/home/ryan/DATA/DAY_Global/stooq/US_INDEX/NASDAQ100.csv"
    elif target_stock == 'AGINDEX':
        file = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
    elif target_stock == 'SH600519':
        file = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    else:
        file = "/home/ryan/DATA/DAY_Global/AG/"+target_stock+".csv"
        # logging.error("Unsupported target stock code "+str(target_stock))
        # exit(0)

    df_daily = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=file)
    df_daily = df_daily[df_daily['date']>=start_period]

    code  = df_daily.iloc[0]['code']
    _t = finlib.Finlib().daily_to_monthly_bar(df_daily=df_daily)
    df_weekly= _t['df_weekly']
    df_monthly = _t['df_monthly']



    #df_daily['date_dt']= df_daily['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y%m%d'))
    #df_daily['date_weekday_int']= df_daily['date_dt'].apply(lambda _d: _d.weekday()) #0: Monday, 6:Sunday
    df_daily['date_weekday_str']= df_daily['date'].apply(lambda _d: calendar.day_name[_d.weekday()])  #By name
    df_daily['increase'] = round((df_daily['close'] - df_daily['open'])*100/df_daily['open'],1)
    desc_increase_day_of_week= df_daily.groupby(by='date_weekday_str').describe()['increase'].sort_values(
        by=['mean'], ascending=False, inplace=False)
    logging.info("\n\nincrease_day_of_week, code "+str(code)+" since "+start_period)
    finlib.Finlib().pprint(desc_increase_day_of_week)

    df_weekly['date_week_of_year']= df_weekly['date'].apply(lambda _d: _d.isocalendar()[1]+1)
    df_weekly['date_week_of_month']= df_weekly['date'].apply(lambda _d: finlib.Finlib().week_number_of_month(_d))
    df_weekly['increase'] = round((df_weekly['close'] - df_weekly['open'])*100/df_weekly['open'],1)
    desc_increase_week_of_year = df_weekly.groupby(by='date_week_of_year').describe()['increase'].sort_values(
        by=['mean'], ascending=False, inplace=False)
    logging.info("\n\nincrease_week_of_year, code "+str(code)+" since "+start_period)
    finlib.Finlib().pprint(desc_increase_week_of_year)

    desc_increase_week_of_month = df_weekly.groupby(by='date_week_of_month').describe()['increase'].sort_values(
        by=['mean'], ascending=False, inplace=False)
    logging.info("\n\nincrease_week_of_month, code "+str(code)+" since "+start_period)
    finlib.Finlib().pprint(desc_increase_week_of_month)

    df_monthly['date_month']= df_monthly['date'].apply(lambda _d: _d.month_name())
    df_monthly['increase']=  round((df_monthly['close'] - df_monthly['open'])*100/df_monthly['open'],1)

    desc_month_of_year = df_monthly.groupby(by='date_month').describe()['increase'].sort_values(by=['mean'], ascending=False, inplace=False)
    logging.info("\n\nincrease_month_of_year, code "+str(code)+" since "+start_period)
    finlib.Finlib().pprint(desc_month_of_year)



    exit(0)


    pass


def main():
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-t","--target_stock", type="str", dest="target_stock_f", default=None, help="indicator, one of [NASDAQ100|SP500|AGINDEX|AGCODE]")

    parser.add_option("-p", "--start_period", type="str", dest="start_period_f", default=None, help="since when, in format yyyymmdd")

    (options, args) = parser.parse_args()

    target_stock = options.target_stock_f
    start_period = options.start_period_f

    if target_stock == None:
        logging.info("missing target_stock [NASDAQ100|SP500|AGIDEX|AGID]")
        exit(0)


    analyze_period_increase(target_stock=target_stock,start_period=start_period)


### MAIN ####
if __name__ == '__main__':
    main()
