# coding: utf-8

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
import finlib
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
import finlib_indicator

from optparse import OptionParser
import constant

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)


def _entry(csv_f, period, dc_length=20):

    #dc_length : Donchian Channels length, 20 days by default.

    rtn = {
        "reason": [''],
        "strength": [''],
        "action": [''],
        "code": [''],
        "date": [''],
        "close": [''],
        "dif1": [''],
        "dea1": [''],
        "macd1": [''],
        "dif2": [''],
        "dea2": [''],
        "macd2": [''],
        "sma60_1": [''],
        "ema60_1": [''],

        "distance_to_5ma_perc": 0,
        "distance_to_sma12_perc": 0,
        "distance_to_sma21_perc": 0,
        "distance_to_sma55_perc": 0,
        "distance_to_sma60_perc": 0,
    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)


    df = df[-1000:]  #last 4 years.

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    #adding max/min

    df_period['pre_date'] = df_period['date'].shift()

    #pre_20D_max: the max value if 20 days before today
    df_period['pre_dc_max'] =  df_period['high'].rolling(dc_length).max().shift()

    # pre_20D_min: the in value if 20 days before today
    df_period['pre_dc_min'] =  df_period['low'].rolling(dc_length).min().shift()

    df_period = finlib_indicator.Finlib_indicator().add_tr_atr(df_period, short=5, middle=21, long=55)

    df_period = df_period[-250:]
    df_period.loc[df_period['high'] > df_period['pre_dc_max'], ['action','max','reason']] = ['B',True,"Break Turtle Dochian Channel "+str(dc_length)+" days high"]
    df_period.loc[df_period['low'] < df_period['pre_dc_min'], ['action','min','reason']] = ['S',True,"Break Turtle Dochian Channel "+str(dc_length)+" days low"]

    df_turtle = df_period

    a_unit_amt = 100 # the number of stocks in a trading unit
    start_cash = 1000000 #cash in account
    net_cash = start_cash
    hold_unit=0
    profit = 0

    # open_price = 0
    # hold_unit_threshold = 0.02
    max_allowed_spent_cash = start_cash * 0.02
    has_spent_cash = 0

    for index, row in df_turtle.iterrows():
        N = row['atr_middle_21']
        stock_value = hold_unit * a_unit_amt * row['close']

        if (row['high'] > row['pre_dc_max']) and has_spent_cash < max_allowed_spent_cash:
        # if (row['close'] > 0.5 * (row['pre_dc_max']+row['pre_dc_min'])) and has_spent_cash < max_allowed_spent_cash:
            #open 1st persition

            A_Unit_Change = N*a_unit_amt  # $ changes in a Day
            #A_Unit_Change = N*row['close']*100 # $ changes in a Day
            # unit_to_open = round(0.01*net_cash/A_Unit_Change/100,8)
            unit_to_open = round(0.01*net_cash/A_Unit_Change/10,0)

            # open_price = row['close']

            hold_unit += unit_to_open
            spent_cash = round(unit_to_open * a_unit_amt * row['close'],2)
            has_spent_cash += spent_cash
            net_cash -= spent_cash
            logging.info(row['date'].strftime('%Y%m%d')+', open unit '+str(unit_to_open)+", price "
                         +str(row['close'])
                         +" spent "+str(spent_cash)
                         +", cash left "+str(net_cash)
                         )
            stock_value = hold_unit * a_unit_amt * row['close']

        if (row['low'] < row['pre_dc_min']) and hold_unit > 0:
        # if (row['close'] < 0.5 * (row['pre_dc_max']+row['pre_dc_min'])) and hold_unit > 0:
            #close persition
            net_cash += stock_value
            profit = round(net_cash - start_cash, 2)
            profit_perc = round(profit*100.0/start_cash, 0)

            logging.info(row['date'].strftime('%Y%m%d')+', close all unit '+str(hold_unit)+", price "+str(row['close'])
                         +" profit "+str(profit)+" , profit_perc "+str(profit_perc)+" cash left "+str(net_cash)+"\n\n")

            hold_unit = 0
            has_spent_cash = 0

        p = row['close'] - row['close_-1_s']
        if hold_unit > 0 and p < 0:
            #check stop loss or win exit

            if np.abs(p) > 2 * N:
                net_cash += hold_unit * a_unit_amt * row['close']
                profit = round(net_cash - start_cash, 2)
                profit_perc = round(profit * 100.0 / start_cash, 0)

                logging.info(row['date'].strftime('%Y%m%d')+', stop loss close'
                             +", price "+str(row['close'])
                             +", unit "+str(hold_unit)
                             +" profit "+str(profit)
                             +" profit_perc "+str(profit_perc)
                             +" cash left "+str(net_cash)
                             +"\n\n"
                             )
                hold_unit = 0
                has_spent_cash = 0


    current_value = stock_value + net_cash
    logging.info("a code verify completed. current value "+ str(current_value))

    return(rtn)


def entry(period, debug=False):
    output_csv = "/home/ryan/DATA/result/turtle_selection_" + period + ".csv"  #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)
    stock_list = finlib.Finlib().remove_garbage(df=stock_list)
    stock_list = finlib.Finlib().add_stock_name_to_df(df=stock_list)

    #stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
    if debug:
        stock_list = stock_list.head(3) #debug

    cnt = stock_list.__len__()
    i = 0
    for index, row in stock_list.iterrows():
        i += 1
        c = row['code']
        name = row['name']

        csv_f = '/home/ryan/DATA/DAY_Global/AG_qfq/' + c + ".csv"
        logging.info(str(i) + " of " + str(cnt) + " " + c)

        r = _entry(csv_f=csv_f, period=period)
        logging.info("end of " + c+" "+name)


def main():
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-p","--period", type="str", dest="period_f", default=None, help="period, one of [M|W|D]")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug")

    (options, args) = parser.parse_args()

    period = options.period_f
    debug = options.debug_f


    entry(period=period, debug=debug)


### MAIN ####
if __name__ == '__main__':
    main()
