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


def _kdj(csv_f, period):

    rtn = {
        "reason": [''],
        "strength": [''],
        "action": [''],
        "code": [''],
        "date": [''],
        "k1": [''],
        "d1": [''],
        "j1": [''],
        "k2": [''],
        "d2": [''],
        "j2": [''],
    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    #sys.stdout.write(csv_f+": ")

    df_rtn = pd.DataFrame()

    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug


    df = pd.read_csv(csv_f, converters={'code': str}, header=None, skiprows=1, names=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'tnv'])

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)

    this_code = df.iloc[0]['code']  #'SH603999'
    this_date = ''  #monthly period, end date of the month.
    this_reason = ''
    this_strength = ''
    this_action = ''

    # print(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #ryan debug
    # print(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-1000:]  #last 4 years.

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    #KDJ, default to 9 days
    period_kdj = 9
    df_period = df_period[-1 * (period_kdj * 3):]

    stock = stockstats.StockDataFrame.retype(df_period)

    df_kdj = stock[['close', 'kdjk', 'kdjd', 'kdjj']]

    if df_kdj.__len__() < 2:  #at least two month.
        return (rtn)

    d2 = df_kdj.iloc[-2]
    d1 = df_kdj.iloc[-1]

    this_date = d1.name.strftime("%Y-%m-%d")  #'2019-03-31'
    _k1 = d1.kdjk
    _d1 = d1.kdjd
    _j1 = d1.kdjj

    _k2 = d2.kdjk
    _d2 = d2.kdjd
    _j2 = d2.kdjj

    if abs(_k1) >= 90:
        this_reason += "excessive buy, k " + str(round(_k1, 0)) + "; "
        this_strength = round(_k1 / 100, 2)
        this_action += 'SELL' + "; "
        print('excessive buy. Should sell. ' + csv_f)
    elif abs(_k1) <= 10:
        this_reason += "excessive sell, k " + str(round(_k1, 0)) + "; "
        this_strength = round((1 - _k1 / 100), 2)
        this_action = 'BUY' + "; "
        print('excessive sell. Should buy. ' + csv_f)

    if (_d2 - _k2) / abs(_d2) >= 0.1:  #d more than 10% of k. Means low price. undervalued
        if _k1 >= _d1:
            this_strength = round((_k1 - _d1) / abs(_d1), 2)
            this_reason += "K cross over D,  K " + str(_k1) + ", D " + str(_d1) + ", strength " + str(this_strength) + "; "
            this_action += 'BUY' + "; "
            print("K cross over D, should buy. " + csv_f)

        elif (_d1 - _k1) / abs(_d1) <= 0.05:  # k still less than d, but very close
            t = round((_d1 - _k1) / abs(_d1), 2)
            this_reason += 'K is less than 5% to D, ' + str(t) + "; "
            this_strength += round((1 - t), 2)
            this_action = 'BUY_EARLY' + "; "
            print("K is less than 5% to D, early stage, should buy little " + csv_f)

    if (_k2 - _d2) / abs(_k2) >= 0.1:  #k more than 10% of d. Means high price. overvalued
        if (_d1 - _k1) / abs(_d1) >= 0.05:  #k less than d more than 5%
            t = round((_d1 - _k1) / abs(_d1), 2)
            this_reason += 'K cross down D, K ' + str(_k1) + ", D " + str(_d1) + ", Percent " + str(t) + "; "
            this_strength = round(t, 2)
            this_action += 'SELL' + "; "
            print("K cross down D, should sell. " + csv_f)

        #elif (d1.kdjk - d1.kdjd)/ d1.kdjk <= 0.05: #k more than d, but very close
        #    print("K is less than 5% to D, early stage, should sell little")
        #return

    rtn = {
        "reason": [this_reason],
        "strength": [this_strength],
        "action": [this_action],
        "code": [this_code],
        "date": [this_date],
        "k1": [round(_k1, 1)],
        "d1": [round(_d1, 1)],
        "j1": [round(_j1, 1)],
        "k2": [round(_k2, 1)],
        "d2": [round(_d2, 1)],
        "j2": [round(_j2, 1)],
    }

    return (rtn)



def kdj(period, debug=False):
    output_csv = "/home/ryan/DATA/result/kdj_selection_" + period + ".csv"  #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)
    #stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')

    if debug:
        stock_list = stock_list.head(100) #ryan debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i += 1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/' + c + ".csv"
        logging.info(str(i) + " of " + str(cnt) + " " + c)

        r = _kdj(csv_f=csv_f, period=period)

        if r['action'] != ['']:
            df = pd.DataFrame(data=r)
            df_rtn = df_rtn.append(df)

    if df_rtn.empty:
        print("KDJ no qualified  stock found.")
    else:
        df_rtn = pd.merge(df_rtn, stock_list, how='inner', on='code')
        df_rtn = df_rtn[['code', 'name', 'date', 'action', 'strength', 'reason', 'k1', 'd1', 'j1', 'k2', 'd2', 'j2']]
        df_rtn.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(__file__+" "+"kdj selection saved to " + output_csv + " . len " + str(df_rtn.__len__()))


def _macd(csv_f, period):
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

        "MACD_DIF_MAIN_OVER_0_N_DAYS": 0,
        "MACD_DEA_SIGNAL_OVER_0_N_DAYS": 0,
        "MACD_HISTOGRAM_OVER_0_N_DAYS": 0,
    }

    distance_to_sma5_perc = 0
    distance_to_sma12_perc = 0
    distance_to_sma21_perc = 0
    distance_to_sma55_perc = 0
    distance_to_sma60_perc = 0

    _MACD_DIF_MAIN_OVER_0_N_DAYS  = 0
    _MACD_DEA_SIGNAL_OVER_0_N_DAYS  = 0
    _MACD_HISTOGRAM_OVER_0_N_DAYS  = 0


    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug
    #csv_f = '/home/ryan/DATA/DAY_Global/AG/SZ000008.csv'
    #csv_f = "/home/ryan/DATA/DAY_Global/AG/SZ000039.csv"


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

    df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=5, middle=21, long=55)
    df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=12, middle=26, long=60)
    df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=90, middle=120, long=200)

    df_period = df_period[-100:]

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

    ### MACD basic
    if d1.DIF_main > 0 and (not df_macd[df_macd['DIF_main'] < 0].empty):
        _MACD_DIF_MAIN_OVER_0_N_DAYS = d1.name-df_macd[df_macd['DIF_main'] < 0].iloc[-1].name
        this_reason += constant.MACD_DIF_MAIN_OVER_0_N_DAYS + "_"+ str(_MACD_DIF_MAIN_OVER_0_N_DAYS)+'; '
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)


    if d1.DEA_signal > 0 and (not df_macd[df_macd['DEA_signal'] < 0].empty):
        _MACD_DEA_SIGNAL_OVER_0_N_DAYS = d1.name-df_macd[df_macd['DEA_signal'] < 0].iloc[-1].name
        this_reason += constant.MACD_DEA_SIGNAL_OVER_0_N_DAYS + "_"+ str( _MACD_DEA_SIGNAL_OVER_0_N_DAYS)+'; '
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)

    if d1.MACD_histogram > 0 and (not df_macd[df_macd['MACD_histogram'] < 0].empty):
        _MACD_HISTOGRAM_OVER_0_N_DAYS = d1.name-df_macd[df_macd['MACD_histogram'] < 0].iloc[-1].name
        this_reason += constant.MACD_HISTOGRAM_OVER_0_N_DAYS + "_"+ str(_MACD_HISTOGRAM_OVER_0_N_DAYS)+'; '
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)



    #MA nianlian
    if d1.close > 0  and abs((d1.close_55_sma - d1.close_21_sma)/d1.close) < 0.02:
        this_reason += constant.MA55_NEAR_MA21 + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)


    if c1 < sma60_1:
        this_reason += constant.CLOSE_UNDER_SMA60 + "; "
        this_action += constant.SELL_CHECK + "; "
        logging.info(this_reason)


    if d1.close_21_sma < sma60_1:
        this_reason += constant.SMA21_UNDER_SMA60 + "; "
        this_action += constant.SELL_CHECK + "; "
        logging.info(this_reason)


    if c1 > sma60_1 * 1.05:
        this_reason += constant.CLOSE_GT_SMA60_5_perc + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)

    if c1 > d1.close_200_sma * 1.2:
        this_reason += constant.CLOSE_GT_SMA200_20_perc + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)





#close price distance to MA
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
        this_reason +=  'SELL_MUST,'+ constant.CLOSE_ABOVE_SMA60 + "but "+constant.DIF_LT_0+', price expected to be drop back to under sma60, close '+str(c1)+" ,sma60 "+str(sma60_1)+"; "
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

    if macd2 > 0  and macd1 < 0:
        this_reason += constant.MACD_CROSS_DOWN_0 + "; "
        this_strength += 0
        this_action += constant.SELL_CHECK+'; '
        logging.info(this_reason)
    elif macd2 < 0  and macd1 > 0:
        this_reason += constant.MACD_CROSS_OVER_0 + "; "
        this_strength += 0
        this_action += constant.BUY_CHECK+'; '
        logging.info(this_reason)


    if dif2 > 0  and dif1 < 0:
        this_reason += constant.DIF_CROSS_DOWN_0 + "; "
        this_strength += 0
        this_action += constant.SELL_CHECK+'; '
        logging.info(this_reason)
    elif dif2 < 0  and dif1 > 0:
        this_reason += constant.DIF_CROSS_OVER_0 + "; "
        this_strength += 0
        this_action += constant.BUY_CHECK+'; '
        logging.info(this_reason)


    if dea2 > 0  and dea1 < 0:
        this_reason += constant.SIG_CROSS_DOWN_0 + "; "
        this_strength += 0
        this_action += constant.SELL_CHECK+'; '
        logging.info(this_reason)
    elif dea2 < 0  and dea1 > 0:
        this_reason += constant.SIG_CROSS_OVER_0 + "; "
        this_strength += 0
        this_action += constant.BUY_CHECK+'; '
        logging.info(this_reason)


    if dif1 < 0:
       this_reason += constant.DIF_LT_0 + "; "
    elif dif1 > 0:
        this_reason += constant.DIF_GT_0 + "; "



    if dea1 < 0:
       this_reason += constant.SIG_LT_0 + "; "
    elif dea1 > 0:
        this_reason += constant.SIG_GT_0 + "; "



    if dif1 < dea1:
       this_reason += constant.DIF_LT_SIG + "; "
       this_action += constant.SELL_CHECK + '; '
    elif dif1 > dea1:
        this_reason += constant.DIF_GT_SIG + "; "
        this_action += constant.BUY_CHECK+'; '


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
######################
######################


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

        "MACD_DIF_MAIN_OVER_0_N_DAYS": [round(_MACD_DIF_MAIN_OVER_0_N_DAYS, 1)],
        "MACD_DEA_SIGNAL_OVER_0_N_DAYS": [round(_MACD_DEA_SIGNAL_OVER_0_N_DAYS, 1)],
        "MACD_HISTOGRAM_OVER_0_N_DAYS": [round(_MACD_HISTOGRAM_OVER_0_N_DAYS, 1)],


    }

    return (rtn)


def macd(period, debug=False):
    output_csv = "/home/ryan/DATA/result/macd_selection_" + period + ".csv"  #head: code, name, date, action(b/s), reason, strength.
    # Term: Middle(because based on Monthly data,
    # Type: Cheap

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)

    #stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
    if debug:
        stock_list = stock_list.head(100) #debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i += 1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/' + c + ".csv"
        logging.info(str(i) + " of " + str(cnt) + " " + c)

        r = _macd(csv_f=csv_f, period=period)

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


def _ma_cross(csv_f,period, period_fast,period_slow):
    rtn = {
        "reason": [''],
        "strength": [''],
        "action": [''],
        "code": [''],
        "date": [''],
        "close": [''],
        'close_above_ma5_n_days':[''],
        'close_nearly_above_ma5_n_days':[''],
        'ma5_near_ma21_days':[''],
        'ma21_near_ma55_days':[''],
    }

    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    # csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv' #ryan debug
    # csv_f = '/home/ryan/DATA/DAY_Global/AG/SZ000008.csv'

    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)
    # df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv") ##ryan debug for ag index

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)

    this_code = df.iloc[0]['code']  # 'SH603999'
    this_date = ''  # monthly period, end date of the month.
    this_reason = ''
    this_strength = 0
    this_action = ''

    # print(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #debug
    # print(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    df = df[-1000:]  # last 4 years.

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    df_period = finlib_indicator.Finlib_indicator().add_ma_ema(df_period, short=5, middle=period_fast, long=period_slow)
    df_period = df_period[-100:]

    # print(tabulate.tabulate(df_period[-10:], headers='keys', tablefmt='psql'))

    df_ma_cross = stockstats.StockDataFrame.retype(df_period).reset_index()

    #ma fast cross over ma slow
    df_ma_cross['ma_fast_cross_over_slow'] = df_ma_cross['close_' + str(period_fast) + '_sma_xu_close_' + str(period_slow) + '_sma']


    # close stand upon MA5
    df_ma_cross['close_stand_upon_ma5'] = df_ma_cross['close'] >= df_ma_cross['close_5_sma']

    # close_stand_upon_ma5_nearly includes close_stand_upon_ma5
    df_ma_cross['close_stand_upon_ma5_nearly'] = ((df_ma_cross['close'] - df_ma_cross['close_5_sma'])/df_ma_cross['close']) > -0.01


    if df_ma_cross.iloc[-1]['close_stand_upon_ma5'] == True and df_ma_cross.index[df_ma_cross['close_stand_upon_ma5']==False].__len__()>0:
        last_under_ma5 = df_ma_cross.index[df_ma_cross['close_stand_upon_ma5']==False][-1]
        bars_above_ma5 = str(df_ma_cross.index[-1] - last_under_ma5)
        logging.info(this_code+" close has been above sma5 for "+bars_above_ma5 + " days")
        this_reason += constant.CLOSE_ABOVE_MA5_N_DAYS+"_"+bars_above_ma5 + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)
        rtn['close_above_ma5_n_days']=bars_above_ma5


    if df_ma_cross.iloc[-1]['close_stand_upon_ma5_nearly'] == True and df_ma_cross.index[df_ma_cross['close_stand_upon_ma5_nearly']==False].__len__()>0:
        last_under_ma5 = df_ma_cross.index[df_ma_cross['close_stand_upon_ma5_nearly']==False][-1]
        bars_near_ma5 = str(df_ma_cross.index[-1] - last_under_ma5)
        logging.info(this_code+" close has been nearly (more than -0.01) sma5 for "+bars_near_ma5 + " days")
        this_reason += constant.CLOSE_NEAR_MA5_N_DAYS+"_"+bars_near_ma5 + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)
        rtn['close_nearly_above_ma5_n_days'] = bars_near_ma5


    # df_ma_cross['ma21_stand_upon_ma55_nearly'] = ((df_ma_cross['close_21_sma'] - df_ma_cross['close_55_sma'])/df_ma_cross['close_21_sma']) > -0.01
    df_ma_cross['ma21_stand_upon_ma55_nearly'] = abs((df_ma_cross['close_21_sma'] - df_ma_cross['close_55_sma'])/df_ma_cross['close_21_sma']) < 0.02

    if df_ma_cross.iloc[-1]['ma21_stand_upon_ma55_nearly'] == True and df_ma_cross.index[df_ma_cross['ma21_stand_upon_ma55_nearly']==False].__len__()>0:
        last_false = df_ma_cross.index[df_ma_cross['ma21_stand_upon_ma55_nearly']==False][-1]
        bars = str(df_ma_cross.index[-1] - last_false)
        logging.info(this_code+" ma21 has been nearly (more than -0.01) ma55 for "+bars + " days")
        this_reason += constant.MA21_NEAR_MA55_N_DAYS+"_"+bars + "; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)
        rtn['ma21_near_ma55_days'] = bars



    if df_ma_cross.__len__() < 2:  # at least two records.
        return (rtn)

    d1 = df_ma_cross.iloc[-1]

    this_date = d1.date.strftime("%Y-%m-%d")  # '2019-03-31'
    c1 = d1.close

    if d1['ma_fast_cross_over_slow'] == True:
        this_reason += constant.SMA_CROSS_OVER + "_"+period_fast+"_"+period_slow+"; "
        this_action += constant.BUY_CHECK + "; "
        logging.info(this_reason)



    rtn = {
        "reason": [this_reason],
        "strength": [this_strength],
        "action": [this_action],
        "code": [this_code],
        "date": [this_date],
        "close": [c1],

    }

    return(rtn)


def ma_cross(period, period_fast, period_slow, debug=False):
    output_csv = "/home/ryan/DATA/result/ma_cross_over_selection_" + period_fast+"_"+period_slow + ".csv"  #head: code, name, date, action(b/s), reason, strength.

    df_rtn = pd.DataFrame()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)

    #stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
    if debug:
        stock_list = stock_list.head(100) #debug

    cnt = stock_list.__len__()
    i = 0
    for c in stock_list['code']:
        i += 1
        csv_f = '/home/ryan/DATA/DAY_Global/AG/' + c + ".csv"
        logging.info(str(i) + " of " + str(cnt) + " " + c)

        r = _ma_cross(csv_f=csv_f, period=period, period_fast=period_fast, period_slow=period_slow)

        if r['action'] != ['']:
            print(r) #ryan debug
            df = pd.DataFrame(data=r)
            df_rtn = df_rtn.append(df)

    if df_rtn.empty:
        logging.info("MA cross over no qualified stock found.")
    else:
        df_rtn = pd.merge(df_rtn, stock_list, how='inner', on='code')
        df_rtn = finlib.Finlib().adjust_column(df_rtn, [ 'date','code', 'name', 'close', 'action', 'reason', 'strength'])
        df_rtn.to_csv(output_csv, encoding='UTF-8', index=False)

        logging.info(__file__+" "+"MA cross over saved to " + output_csv + " . len " + str(df_rtn.__len__()))


def calculate(indicator, period,period_fast,period_slow, debug):
    if indicator == 'KDJ':
        kdj(period=period, debug=debug)

    if indicator == 'MACD':
        macd(period=period, debug=debug)

    if indicator == 'MA_CROSS':
        ma_cross(period=period, period_fast=period_fast, period_slow=period_slow, debug=debug)





def analyze(indicator, debug=False):
    df = finlib.Finlib().get_A_stock_instrment()
    df2 = finlib.Finlib()._remove_garbage_macd_ma(df)



    dir = "/home/ryan/DATA/result"
    if indicator == 'MACD':
        input_csv_m = dir + "/macd_selection_M.csv"
        input_csv_w = dir + "/macd_selection_W.csv"
        input_csv_d = dir + "/macd_selection_D.csv"

        csv_o = dir + "/under_sma60_M.csv"
        finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.CLOSE_UNDER_SMA60, period='M').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved "+csv_o)

        csv_o = dir + "/under_sma60_W.csv"
        finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.CLOSE_UNDER_SMA60, period='W').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved " + csv_o)

        csv_o = dir + "/under_sma60_D.csv"
        finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.CLOSE_UNDER_SMA60, period='D').to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("Saved " + csv_o)


        finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.DIF_CROSS_OVER_0, period='D').to_csv(csv_o, encoding='UTF-8', index=False)


    if indicator == 'KDJ':
        input_csv_m = dir + "/kdj_selection_M.csv"
        input_csv_w = dir + "/kdj_selection_W.csv"
        input_csv_d = dir + "/kdj_selection_D.csv"

    if indicator == 'MA_CROSS':
        input_csv_d = dir + "/home/ryan/DATA/result/ma_cross_over_selection_5_21.csv"


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

    parser.add_option("-i","--indicator", type="str", dest="indicator_f", default=None, help="indicator, one of [KDJ|MACD|MA_CROSS]")

    parser.add_option("-p","--period", type="str", dest="period_f", default=None, help="period, one of [M|W|D]")
    parser.add_option("-f","--period_fast", type="str", dest="period_fast_f", default=None, help="fast period of MA, 21 e.g")
    parser.add_option("-s","--period_slow", type="str", dest="period_slow_f", default=None, help="slow period of MA, 55 e.g")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on [MACD|KDJ] M|W|D result.")
    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug")

    (options, args) = parser.parse_args()

    indicator = options.indicator_f
    period = options.period_f
    period_fast = options.period_fast_f
    period_slow = options.period_slow_f
    analyze_f = options.analyze_f
    debug = options.debug_f



    if indicator == None:
        print("missing indicator [MACD|KDJ|MA_CROSS]")

    if analyze_f:
        analyze(indicator=indicator,  debug=debug)
    elif not period == None:
        calculate(indicator=indicator, period=period, period_fast=period_fast,period_slow=period_slow,debug=debug)


### MAIN ####
if __name__ == '__main__':
    main()
