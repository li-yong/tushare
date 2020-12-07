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


def _entry(csv_f, period):
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

    distance_to_sma5_perc = 0
    distance_to_sma12_perc = 0
    distance_to_sma21_perc = 0
    distance_to_sma55_perc = 0
    distance_to_sma60_perc = 0

    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug
    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SZ000008.csv'


    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)
    #df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv") ##ryan debug for ag index

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)

    this_code = df.iloc[0]['code']  #'SH603999'
    this_date = ''  #monthly period, end date of the month.
    this_reason = ''
    this_strength = 0
    this_action = ''

    # print(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #debug
    # print(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-1000:]  #last 4 years.

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    #df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=5, middle=21, long=55)
    #df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=12, middle=26, long=60)
    df_period = finlib_indicator.Finlib_indicator().add_tr_atr(df_period, short=5, middle=21, long=55)

    df_period = df_period[-250:]

    #the day when close higher than previous N periods
    # df_max=df_period[df_period['close'].rolling(window=20).max() == df_period['close']]
    df_max=df_period[df_period['high'].rolling(window=21).max() == df_period['high']]
    df_max = df_max[['date','close', 'high']]
    df_max = df_max[['date']]
    df_max['max'] = True

    #the day when close lower than previous N periods
    # df_min=df_period[df_period['close'].rolling(window=10).max() == df_period['close']]
    df_min=df_period[df_period['low'].rolling(window=10).min() == df_period['low']]
    df_min = df_min[['date','close', 'low']]
    df_min = df_min[['date']]
    df_min['min'] = True

    df_period = pd.merge(df_period, df_max, on='date', how='outer')
    df_period = pd.merge(df_period, df_min, on='date', how='outer')

    #df_period[(df_period['max']==True) | (df_period['min']==True)]

    df_turtle = df_period

    a_unit_amt = 100 # the number of stocks in a trading unit
    start_cash = 1000000 #cash in account
    net_cash = start_cash
    hold_unit=0
    profit = 0

    open_price = 0



    for index, row in df_turtle.iterrows():
        # print(row)


        # if row['max']==True and hold_unit == 0:
        if row['max']==True:
            #open 1st persition
            N = row['atr_middle_21']
            A_Unit_Change = N*a_unit_amt  # $ changes in a Day
            #A_Unit_Change = N*row['close']*100 # $ changes in a Day
            unit_to_open = round(0.01*net_cash/A_Unit_Change,0)

            open_price = row['close']

            hold_unit += unit_to_open
            net_cash -=  unit_to_open * a_unit_amt * open_price
            logging.info(row['date'].strftime('%Y%m%d')+', open unit '+str(unit_to_open)+", price "+str(row['close']))



        if row['min']==True and hold_unit > 0:
            #close persition

            net_cash += hold_unit * a_unit_amt * row['close']

            profit = round(net_cash - start_cash, 2)
            profit_perc = round(profit*100.0/start_cash, 0)

            logging.info(row['date'].strftime('%Y%m%d')+', close all unit '+str(hold_unit)+", price "+str(row['close']))
            logging.info("profit "+str(profit)+" , profit_perc "+str(profit_perc)+"\n\n")

            hold_unit = 0

        p = row['close'] - open_price
        if hold_unit > 0 and p < 0:
            #check stop loss or win exit

            if abs(p) > 2 * N:
                logging.info(row['date'].strftime('%Y%m%d')+', stop loss close'+", price "+str(row['close']))
                hold_unit = 0




        #print('')
    logging.info('script completed')
    exit(0)



    df_period['max_of_n_days']=df_period[df_period['close'].rolling(window=2).max() == df_period['close']]





    #print(tabulate.tabulate(df_period[-10:], headers='keys', tablefmt='psql'))
    '''
    MACD: (12-day EMA - 26-day EMA)
    Signal Line: 9-day EMA of MACD
    MACD Histogram: 2*(MACD - Signal Line)
    
    df = finlib_indicator.Finlib_indicator().add_ma_ema(df[['close']], short=12, middle=26, long=60)
    df['ema_12_minus_26'] = df['ema_short_12'] - df['ema_middle_26']  # named DIF in tradeview/eastmoney/MooMoo
    df['signal'] = df['ema_12_minus_26'].ewm(span=9, min_periods=0, adjust=False, ignore_na=False).mean() # named DEA in tradeview/eastmoney/MooMoo
    df['Histogram'] = 2 * (df['ema_12_minus_26'] - df['signal'])  # named MACD in tradeview/eastmoney/MooMoo
    '''

    df_macd = stockstats.StockDataFrame.retype(df_period).reset_index()
    # close price less than 10.0 in 5 days count
    df_macd = df_macd['close_10.0_le_5_c']


    #df_macd = stock[['macd', 'macds', 'macdh','close','sma_long_60','ema_long_60','date','code']]  #macds: # MACD signal line, macdh: # MACD histogram
    df_macd[['macd', 'macds', 'macdh', 'date']]  #macds: # MACD signal line, macdh: # MACD histogram
    df_macd.rename(columns={
        "macd": "DIF_main",    # DIF_Main called DIF in tradeview/eastmoney/Moomoo
        "macds": "DEA_signal", # DEA_signal called DEA in tradeview/eastmoney/Moomoo
        "macdh": "MACD_histogram", # MACD_histogram called 'MACD' in tradeview/eastmoney/MooMoo
    }, inplace=True)
    df_macd = df_macd.round({'DIF_main': 2, 'DEA_signal': 2, 'MACD_histogram': 2})
  
 
    #print(tabulate.tabulate(df_macd[-2:], headers='keys', tablefmt='psql'))

    if df_macd.__len__() < 2:  #at least two records.
        return (rtn)

    d2 = df_macd.iloc[-2]
    d1 = df_macd.iloc[-1]

    this_date = d1.date.strftime("%Y-%m-%d")  #'2019-03-31'
    dif1 = d1.DIF_main  # DIF.  close_ema_12 - close_ema_26. Difference btwn fast/slow price ema.
                        # zero value means price not change in a period (26 days)
                        # Bigger positive value means faster up movement.
                        # Bigger nagative value means faster down movement.

    dea1 = d1.DEA_signal  #DEA  ema_9_of_DIF. sum(dif1..dif9)/9
                          # 0 means sum(dif1..diff9) == 0. Last 9 days diff sum are zero --> restored to the 9 days before.
                          # positive means last 9 days diff are positive, --> up movement.
                          # nagative means last 9 days diff are nagative, --> down movement.

    macd1 = d1.MACD_histogram  #MACD. 2 * (df['ema_12_minus_26'] - df['signal'])
    c1 = d1.close
    sma60_1 = d1.sma_long_60
    ema60_1 = d1.ema_long_60

    dif2 = d2.DIF_main
    dea2 = d2.DEA_signal
    macd2 = d2.MACD_histogram
    c2 = d2.close
    sma60_2 = d2.sma_long_60
    ema60_2 = d2.ema_long_60

    if c1 < sma60_1:
        this_reason += constant.UNDER_SMA60 + "; "
        this_action += constant.SELL_CHECK + "; "
        logging.info(this_reason)

#distance to MA
    if c1 > 0 and c2 > 0:
        distance_to_sma5_perc = round((c1 - d1.close_5_sma) * 100 /c1, 1)
        distance_to_sma12_perc = round((c1 - d1.close_12_sma) * 100 /c1, 1)
        distance_to_sma21_perc = round((c1 - d1.close_21_sma) * 100 /c1, 1)
        distance_to_sma55_perc = round((c1 - d1.close_55_sma) * 100 /c1, 1)
        distance_to_sma60_perc = round((c1 - d1.close_60_sma) * 100 /c1, 1)

        distance_to_sma5_perc_2 = round((c2 - d2.close_5_sma) * 100 /c2, 1)
        distance_to_sma12_perc_2 = round((c2 - d2.close_12_sma) * 100 /c2, 1)
        distance_to_sma21_perc_2 = round((c2 - d2.close_21_sma) * 100 /c2, 1)
        distance_to_sma55_perc_2 = round((c2 - d2.close_55_sma) * 100 /c2, 1)
        distance_to_sma60_perc_2 = round((c2 - d2.close_60_sma) * 100 /c2, 1)

        if distance_to_sma5_perc_2 < 0 and distance_to_sma5_perc > 0:
            this_reason += constant.CROSS_OVER_SMA5+'; '
            this_strength += round(distance_to_sma5_perc - distance_to_sma5_perc_2,1)
            this_action += constant.BUY_EARLY + "; "
            logging.info(this_reason)
        elif distance_to_sma5_perc_2 > 0 and distance_to_sma5_perc < 0:
            this_reason +=  constant.CROSS_DOWN_SMA5+'; '
            this_strength += round((distance_to_sma5_perc - distance_to_sma5_perc_2)*-1)
            this_action += constant.SELL_EARLY + "; "
            logging.info(this_reason)


        if distance_to_sma21_perc_2 < 0 and distance_to_sma21_perc > 0:
            this_reason +=  constant.CROSS_OVER_SMA21+'; '
            this_strength += round(distance_to_sma21_perc - distance_to_sma21_perc_2,1)
            this_action += constant.BUY_EARLY + "; "
            logging.info(this_reason)
        elif  distance_to_sma21_perc_2 > 0 and distance_to_sma21_perc < 0:
            this_reason += constant.CROSS_DOWN_SMA21+'; '
            this_strength += round((distance_to_sma21_perc - distance_to_sma21_perc_2)*-1)
            this_action += constant.SELL_EARLY + "; "
            logging.info(this_reason)


        if distance_to_sma60_perc_2 < 0 and distance_to_sma60_perc > 0:
            this_reason +=  constant.CROSS_OVER_SMA60+'; '
            this_strength += round(distance_to_sma60_perc - distance_to_sma60_perc_2,1)
            this_action += constant.BUY_EARLY + "; "
            logging.info(this_reason)
        elif  distance_to_sma60_perc_2 > 0 and distance_to_sma60_perc < 0:
            this_reason += constant.CROSS_DOWN_SMA60+'; '
            this_strength += round((distance_to_sma60_perc - distance_to_sma60_perc_2)*-1,1)
            this_action += constant.SELL_EARLY + "; "
            logging.info(this_reason)


#####################
#  Conditions:  SELL EASY, BUY HARD
#####################
    #if c2 < sma60_2 and  c1 > sma60_1 and dif1 < 0 : #use this criteria, cross over ensure the curve are up trend.
    if c1 > sma60_1 and dif1 < 0 : #Don't use this criterial
        this_reason +=  'SELL_MUST,'+ constant.ABOVE_SMA60 + "but "+constant.DIF_LT_0+', price expected to be drop back to under sma60, close '+str(c1)+" ,sma60 "+str(sma60_1)+"; "
        this_strength += 1
        this_action +=  constant.SELL_MUST + "; "
        logging.info(this_reason)


    if c2 < sma60_2 and  c1 > sma60_1 and dif2 < 0 and dif1 > 0 :
        this_reason +='BUY_MUST, '+ constant.CROSS_OVER_SMA60+' and '+ constant.DIF_CROSS_OVER_0 +', price expected to continue rise. close '+str(c1)+" ,sma60 "+str(sma60_1)+"; "
        this_strength += 2
        this_action += constant.BUY_MUST + "; "
        logging.info(this_reason)

    if c2 > sma60_2 and  c1 < sma60_1 and dif2 > 0 and dif1 < 0 :
        this_reason += str(this_code)+" "+str(this_date) + ',SELL_MUST, '+constant.CROSS_DOWN_SMA60+' and  '+constant.DIF_CROSS_DOWN_0+  ', price expected to continue drop. close '+str(c1)+" ,sma60 "+str(sma60_1)+"; "
        this_strength += 2
        this_action += constant.SELL_MUST + "; "
        logging.info(this_reason)



#####################
#####################
# dif cross above dea
    if dif2 < dea2 and dif1 > dea1:
        this_reason +=  constant.DIF_CROSS_OVER_SIG + "; "
        this_strength += 1
        this_action += constant.BUY_CHECK+'; '
        logging.info(this_reason)


    if dif2 > dea2 and dif1 < dea1:
        this_reason += constant.DIF_CROSS_DOWN_SIG + "; "
        this_strength += 1
        this_action += constant.SELL_CHECK+'; '
        logging.info(this_reason)

#####################
#####################
    if dif1 > 50 and dea1 > 50:
        if (macd1 < macd2) and macd1 > 0 and macd1 < 10:
            this_reason += constant.PRICE_HIGH + ", " +constant.MACD_DECLINE_NEAR_0 +"; "
            this_strength += 1
            this_action += constant.SELL_EARLY+'; '
            logging.info(this_reason + csv_f)
        elif dif2 > 0 and dea2 > 0 and macd1 < 0 and macd2 > 0:
            this_reason +=  constant.PRICE_HIGH + ", " +constant.MACD_CROSS_DOWN_0 +"; "
            this_strength += 1
            this_action += constant.SELL_MUST+'; '
            logging.info(this_reason)

#####################
#####################

    if dif1 < -50 and dea1 < -50:
        if macd2 < 0 and (macd1 > macd2) and macd1 < 0 and macd1 > -10:
            this_reason += constant.PRICE_LOW + ", " +constant.MACD_CLIMB_NEAR_0 +"; "
            this_strength += 1
            this_action += constant.BUY_EARLY+'; '
            logging.info(this_reason)
        elif dif2 < -50 and dea2 < -50 and macd1 > 0 and macd2 < 0:
            this_reason += constant.PRICE_LOW + ", " +constant.MACD_CROSS_OVER_0 +"; "
            this_strength += 1
            this_action += constant.BUY_MUST+'; '
            logging.info(this_reason)

    rtn = {
        "reason": [this_reason],
        "strength": [this_strength],
        "action": [this_action],
        "code": [this_code],
        "date": [this_date],
        "close": [c1],
        "dif1": [round(dif1, 1)],
        "dea1": [round(dea1, 1)],
        "macd1": [round(macd1, 1)],
        "dif2": [round(dif2, 1)],
        "dea2": [round(dea2, 1)],
        "macd2": [round(macd2, 1)],
        "sma60_1": [round(sma60_1, 1)],
        "ema60_1": [round(ema60_1, 1)],

        "distance_to_sma5_perc": [round(distance_to_sma5_perc, 1)],
        "distance_to_sma12_perc": [round(distance_to_sma12_perc, 1)],
        "distance_to_sma21_perc": [round(distance_to_sma21_perc, 1)],
        "distance_to_sma55_perc": [round(distance_to_sma55_perc, 1)],
        "distance_to_sma60_perc": [round(distance_to_sma60_perc, 1)],
    }

    return (rtn)


def entry(period, debug=False):
    output_csv = "/home/ryan/DATA/result/turtle_selection_" + period + ".csv"  #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)

    #stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
    if debug:
        stock_list = stock_list.head(3) #debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i += 1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/' + c + ".csv"
        logging.info(str(i) + " of " + str(cnt) + " " + c)

        r = _entry(csv_f=csv_f, period=period)

        if r['action'] != ['']:
            df = pd.DataFrame(data=r)
            df_rtn = df_rtn.append(df)

    if df_rtn.empty:
        logging.info("MACD no qualified stock found.")
    else:
        df_rtn = pd.merge(df_rtn, stock_list, how='inner', on='code')
        df_rtn = finlib.Finlib().adjust_column(df_rtn, [ 'date','code', 'name', 'close', 'action', 'reason', 'strength'])
        df_rtn.to_csv(output_csv, encoding='UTF-8', index=False)

        logging.info(__file__+" "+"MACD selection saved to " + output_csv + " . len " + str(df_rtn.__len__()))




def analyze(indicator, debug=False):
    dir = "/home/ryan/DATA/result"
    if indicator == 'MACD':
        input_csv_m = dir + "/macd_selection_M.csv"
        input_csv_w = dir + "/macd_selection_W.csv"
        input_csv_d = dir + "/macd_selection_D.csv"

        csv_o = dir + "/under_sma60_M.csv"
        finlib_indicator.Finlib_indicator().get_under_sma60(period='M').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved "+csv_o)

        csv_o = dir + "/under_sma60_W.csv"
        finlib_indicator.Finlib_indicator().get_under_sma60(period='W').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved " + csv_o)

        csv_o = dir + "/under_sma60_D.csv"
        finlib_indicator.Finlib_indicator().get_under_sma60(period='D').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved " + csv_o)

    if indicator == 'KDJ':
        input_csv_m = dir + "/kdj_selection_M.csv"
        input_csv_w = dir + "/kdj_selection_W.csv"
        input_csv_d = dir + "/kdj_selection_D.csv"


    df_m = pd.read_csv(input_csv_m)
    df_m.rename(columns={
        'action': 'action_m',
        'date': 'date_m',
        'reason': 'reason_m',
        'strength': 'strength_m',
    }, inplace=True)

    df_w = pd.read_csv(input_csv_w)
    df_w.rename(columns={
        'action': 'action_w',
        'date': 'date_w',
        'reason': 'reason_w',
        'strength': 'strength_w',
    }, inplace=True)

    df_d = pd.read_csv(input_csv_d)
    df_d.rename(columns={
        'action': 'action_d',
        'date': 'date_d',
        'reason': 'reason_d',
        'strength': 'strength_d',
    }, inplace=True)

    df_merge = pd.merge(df_m, df_w, on=['code', 'name'], how='outer')
    df_merge = pd.merge(df_merge, df_d, on=['code', 'name'], how='outer')

    cols = ['code', 'name', 'date_m', 'date_w', 'date_d', 'action_m', 'action_w', 'action_d', 'reason_m', 'reason_w', 'reason_d', 'strength_m', 'strength_w', 'strength_d']

    df_merge = df_merge[cols]
    print(tabulate.tabulate(df_merge[-20:], headers='keys', tablefmt='psql'))

    # df_merge[df_merge.action_d.str.contains(r'BUY.*', na=True)]


def main():
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-i","--indicator", type="str", dest="indicator_f", default=None, help="indicator, one of [KDJ|MACD|SMA|PriceCounter]")

    parser.add_option("-p","--period", type="str", dest="period_f", default=None, help="period, one of [M|W|D]")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on [MACD|KDJ] M|W|D result.")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug")

    (options, args) = parser.parse_args()

    indicator = options.indicator_f
    period = options.period_f
    analyze_f = options.analyze_f
    debug = options.debug_f


    entry(period=period, debug=debug)


### MAIN ####
if __name__ == '__main__':
    main()
