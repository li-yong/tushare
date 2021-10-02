# coding: utf-8

import tushare as ts
import tushare.util.conns as ts_cs
import tushare.stock.trading as ts_stock_trading

import talib
import pickle
import os
import os.path
import pandas as pd
import time
import numpy as np
import tabulate
import collections
import stat
import constant
from scipy import stats
import shutil
from selenium import webdriver

# import matplotlib.pyplot as plt
# from pandas.plotting import register_matplotlib_converters
# register_matplotlib_converters()

# import pandas
# import mysql.connector
from sqlalchemy import create_engine
import re
import math
from datetime import datetime, timedelta
from scipy import stats
import sys
import traceback
# from jaqs.data.dataapi import DataApi
import glob
import stockstats

import finlib
import logging
import yaml
import warnings
import constant
from operator import sub


pd.options.mode.chained_assignment = None

# warnings.filterwarnings("error")
# warnings.filterwarnings("default")




from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# reduce webdriver session log for every request.
from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER
from selenium.webdriver.remote.remote_connection import logging as SELENIUM_logging
SELENIUM_LOGGER.setLevel(SELENIUM_logging.ERROR)


from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains


class Finlib_indicator:
    def add_ma_ema_simple(self, df):
        logging.info("adding ma to df")

        #sma short
        close_sma_2 = df['close'].rolling(window=2).mean()
        close_sma_5 = df['close'].rolling(window=5).mean()
        close_sma_8 = df['close'].rolling(window=8).mean()
        close_sma_13 = df['close'].rolling(window=13).mean()

        #sma long
        close_sma_9 = df['close'].rolling(window=9).mean()
        close_sma_21 = df['close'].rolling(window=21).mean()
        close_sma_55 = df['close'].rolling(window=55).mean()

        df['close_sma_2'] = close_sma_2
        df['close_sma_5'] = close_sma_5
        df['close_sma_8'] = close_sma_8
        df['close_sma_13'] = close_sma_13

        df['close_sma_9'] = close_sma_9
        df['close_sma_21'] = close_sma_21
        df['close_sma_55'] = close_sma_55

        #ema short
        close_ema_2 = df['close'].ewm(span=2, min_periods=0, adjust=False, ignore_na=False).mean()  # exponential weighted.
        close_ema_5 = df['close'].ewm(span=5, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_8 = df['close'].ewm(span=8, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_10 = df['close'].ewm(span=10, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_13 = df['close'].ewm(span=13, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_20 = df['close'].ewm(span=20, min_periods=0, adjust=False, ignore_na=False).mean()

        #ema long
        close_ema_9 = df['close'].ewm(span=9, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_21 = df['close'].ewm(span=21, min_periods=0, adjust=False, ignore_na=False).mean()
        close_ema_55 = df['close'].ewm(span=55, min_periods=0, adjust=False, ignore_na=False).mean()

        df['close_ema_2'] = close_ema_2
        df['close_ema_5'] = close_ema_5
        df['close_ema_8'] = close_ema_8
        df['close_ema_13'] = close_ema_13

        df['close_ema_9'] = close_ema_9
        df['close_ema_21'] = close_ema_21
        df['close_ema_55'] = close_ema_55

        df['close_ema_10'] = close_ema_10
        df['close_ema_20'] = close_ema_20

        return (df)

    #Average True Range
    def ATR(self, df, n):
        ### Prepare.  Adding t-1 days' value to t ###
        df1 = df[['date', 'close', 'volume']]
        df1 = df1.rename(columns={'date': 'date_pre', 'close': 'close_pre', 'volume': 'volume_pre'})
        df1 = df1.shift(periods=1)
        df = df.merge(df1, left_index=True, right_index=True)
        df = df.drop(columns=['date_pre'], axis=1)

        #### ATR  ####
        TR_l = [0]
        for i in range(1, df.__len__()):
            TR = max(df.at[i, 'high'], df.at[i, 'close_pre']) - min(df.at[i, 'low'], df.at[i, 'close_pre'])
            TR_l.append(TR)
        TR_s = pd.Series(TR_l).rename("TR")
        df = df.join(TR_s)
        ATR = TR_s.ewm(span=n, min_periods=n).mean().rename("ATR_" + str(n))
        df = df.join(ATR)

        return df


    #not recommend long df, the shorter the faster
    # df.__len__ == 7 recommend
    def upper_body_lower_shadow(self, df,ma_short, ma_middle,ma_long):
        ###### Upper_shadow, Body, Lower_shadow ####
        unit = [['']*2+[0]*3+[False]*6]
        df_a = pd.DataFrame( unit * df.__len__(), columns=['reason', 'action', 'upper_shadow_len', 'body_len', 'lower_shadow_len',
                                                                 'guangtou','guangjiao',
                                                                 'small_body','cross_star',
                                                                 'long_upper_shadow','long_lower_shadow',
                                                                 ])

        # df_a = df_a.assign('reason', '') #adding column 'reason' with empty string
        # df_a = df_a.assign('action', '')
        df = df.merge(df_a, left_index=True, right_index=True)

        # df.iloc[i, df.columns.get_loc('reason')] = ''
        # df.iloc[i, df.columns.get_loc('reason')] = ''

        threshold_small = 1.0/20
        threshold_large = 5
        for i in range(df.__len__()):
            upper_shadow_len = df.at[i, 'high'] - max(df.at[i, 'open'], df.at[i, 'close'])
            body_len = abs(df.at[i, 'close'] - df.at[i, 'open'])+0.00001 #prevent 0
            lower_shadow_len = min(df.at[i, 'open'], df.at[i, 'close']) - df.at[i, 'low']


            df.iloc[i, df.columns.get_loc('upper_shadow_len')] = upper_shadow_len
            df.iloc[i, df.columns.get_loc('body_len')] = body_len
            df.iloc[i, df.columns.get_loc('lower_shadow_len')] = lower_shadow_len

            if body_len / (df.at[i, 'open']+0.001) < 0.001:
                df.iloc[i, df.columns.get_loc('small_body')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_SMALL_BODY+'; '

                if  upper_shadow_len > body_len and lower_shadow_len > body_len:
                    df.iloc[i, df.columns.get_loc('cross_star')] = True
                    df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_CROSS_STAR+'; '

            if upper_shadow_len/body_len < threshold_small:
                df.iloc[i, df.columns.get_loc('guangtou')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_GUANG_TOU+'; '
            if lower_shadow_len/body_len < threshold_small:
                df.iloc[i, df.columns.get_loc('guangjiao')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_GUANG_JIAO+'; '

            if upper_shadow_len/body_len > threshold_large:
                df.iloc[i, df.columns.get_loc('long_upper_shadow')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_LONG_UPPER_SHADOW+'; '
            if lower_shadow_len/body_len > threshold_large:
                df.iloc[i, df.columns.get_loc('long_lower_shadow')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_LONG_LOWER_SHADOW+'; '

        #yun xian

        df_a = pd.DataFrame( [[False,False]] * df.__len__(), columns=['yunxian_buy','yunxian_sell'])
        df = df.merge(df_a, left_index=True, right_index=True)

        if df.__len__() < 30:
            logging.info("bar number too small (<30) to calculate yunxian "+str( df.__len__() ))

        #check yunxian for the latest 5 bars
        for i in range(df.__len__() - 5, df.__len__()):
            print("i is "+str(i))
            df_tmp = df.iloc[:i]
            junxian_seri = self.sma_jincha_sicha_duotou_koutou(df_tmp, short=ma_short, middle=ma_middle, long=ma_long).iloc[-1]

            #yunxian_buy: down trend, down_bar large bar.
            if (junxian_seri['kongtou_pailie']):
                if df.iloc[i-1]['open']>df.iloc[i-1]['close']:
                        if (not df.iloc[i-1]['long_upper_shadow'] ):
                            if (not df.iloc[i-1]['long_lower_shadow'] ):
                                if (not df.iloc[i-1]['small_body'] ):
                                    if df.iloc[i-1]['tr'] > 1.0* df.iloc[i-2]['atr_short_'+str(ma_short)]:
                                            # increase_bar,
                                            if df.iloc[i]['open']< df.iloc[i]['close']:
                                                if df.iloc[i]['low'] > df.iloc[i-1]['low']:
                                                    if df.iloc[i]['high'] < df.iloc[i-1]['high'] :
                                                        df.iloc[i, df.columns.get_loc('yunxian_buy')] = True
                                                        df.iloc[i, df.columns.get_loc(
                                                            'reason')] += constant.BAR_YUNXIAN_BUY+'; '

                                                        logging.info("yunxian buy point")


            #yunxian_sell: up trend, up_bar large bar.
            if (junxian_seri['duotou_pailie']):
                if df.iloc[i-1]['open']<df.iloc[i-1]['close']:
                        if (not df.iloc[i-1]['long_upper_shadow'] ):
                            if (not df.iloc[i-1]['long_lower_shadow'] ):
                                if (not df.iloc[i-1]['small_body'] ):
                                    if df.iloc[i-1]['tr'] > 1.0* df.iloc[i-2]['atr_short_'+str(ma_short)]:
                                            if df.iloc[i]['open']< df.iloc[i]['close']:# decrease_bar,
                                                if df.iloc[i]['low'] > df.iloc[i-1]['low']:
                                                    if df.iloc[i]['high'] < df.iloc[i-1]['high'] :
                                                        df.iloc[i, df.columns.get_loc('yunxian_sell')] = True
                                                        df.iloc[i, df.columns.get_loc(
                                                            'reason')] += constant.BAR_YUNXIAN_SELL+'; '
                                                        print("yunxian sell point")

        return df

    #Keltner Channel
    def KELCH(self, df, n):
        KelChM = ((df['high'] + df['low'] + df['close']) / 3).rolling(n).mean().rename('KelChM_' + str(n))
        KelChU = ((4 * df['high'] - 2 * df['low'] + df['close']) / 3).rolling(n).mean().rename('KelChU_' + str(n))
        KelChD = ((-2 * df['high'] + 4 * df['low'] + df['close']) / 3).rolling(n).mean().rename('KelChD_' + str(n))

        df = df.join(KelChU)
        df = df.join(KelChM)
        df = df.join(KelChD)
        return df

    def add_ma_ema(self, df, short=5, middle=10, long=20):
        #if(df.__len__() < long):
        #    logging.fatal("df don't have enough bars , must large than long "+str(long)+" , "+str(df.__len__()))
        #    exit(1)

        short = int(short)
        middle = int(middle)
        long = int(long)

        stock = stockstats.StockDataFrame.retype(df)

        df['sma_short_' + str(short)] = stock['close_' + str(short) + '_sma']
        df['sma_middle_' + str(middle)] = stock['close_' + str(middle) + '_sma']
        df['sma_long_' + str(long)] = stock['close_' + str(long) + '_sma']

        df['p_ma_dikou_'+ str(short)] = df['close'].shift(short-1)
        df['p_ma_dikou_'+ str(middle)] = df['close'].shift(middle-1)
        df['p_ma_dikou_'+ str(long)] = df['close'].shift(long-1)

        df['ema_short_' + str(short)] = stock['close_' + str(short) + '_ema']
        df['ema_middle_' + str(middle)] = stock['close_' + str(middle) + '_ema']
        df['ema_long_' + str(long)] = stock['close_' + str(long) + '_ema']

        # #standard deviation of (biao zhun fang cha) of close. 表示数据大致扩散到多远
        # df['std_close_short_' + str(short)] = df['close'].rolling(window=short).std()
        # df['std_close_middle_' + str(middle)] = df['close'].rolling(window=middle).std()
        # df['std_close_long_' + str(long)] = df['close'].rolling(window=long).std()

        # #standard deviationof (biao zhun fang cha) of sma_short 表示数据大致扩散到多远
        # df['std_sma_short_' + str(short)] = df['sma_short_' + str(short)].rolling(window=short).std()
        # df['std_sma_middle_' + str(middle)] = df['sma_middle_' + str(middle)].rolling(window=middle).std()
        # df['std_sma_long_' + str(long)] = df['sma_long_' + str(long)].rolling(window=long).std()

        _df_tmp = df['sma_short_' + str(short)].rolling(window=10) #evaluate last two weeks.
        df['two_week_fluctuation_sma_short_'+ str(short)] = round((_df_tmp.max() - _df_tmp.min())/_df_tmp.mean()*100.0,1)

        _df_tmp = df['sma_middle_' + str(middle)].rolling(window=10) #evaluate last two weeks.
        df['two_week_fluctuation_sma_middle_'+ str(middle)] = round((_df_tmp.max() - _df_tmp.min())/_df_tmp.mean()*100.0,1)

        _df_tmp = df['sma_long_' + str(long)].rolling(window=10) #evaluate last two weeks.
        df['two_week_fluctuation_sma_long_'+ str(long)] = round((_df_tmp.max() - _df_tmp.min())/_df_tmp.mean()*100.0,1)




        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        return (df)

    def add_tr_atr(self, df, short=5, middle=10, long=20):
        stock = stockstats.StockDataFrame.retype(df)

        df['tr'] = stock['tr']

        df['atr_short_' + str(short)] = stock[ 'atr_'+str(short)]
        df['atr_middle_' + str(middle)] = stock[ 'atr_'+str(middle)]
        df['atr_long_' + str(long)] = stock[ 'atr_'+str(long)]

        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column

        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        return (df)


    def add_rsi(self, df, short=5, middle=10, long=20):
        stock = stockstats.StockDataFrame.retype(df)

        df['rsi_short_' + str(short)] = stock[ 'rsi_'+str(short)]
        df['rsi_middle_' + str(middle)] = stock[ 'rsi_'+str(middle)]
        df['rsi_long_' + str(long)] = stock[ 'rsi_'+str(long)]

        df = df.reset_index()   # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        return (df)

    #########################################################
    #must call fristly: df = self.add_ma_ema(df=df, short=short, middle=middle, long=long)
    #look back last 30 days, recommended df len is 30
    #########################################################
    def sma_jincha_sicha_duotou_koutou(self, df, short=5, middle=10, long=20):


        rtn_dict = {
            'code': None,
            'date': None,
            'close': None,
            'reason': '',
            'action': '',

            "short_period": short,
            "middle_period": middle,
            "long_period": long,
            'jincha_minor': None,
            'jincha_minor_strength': None,
            'sicha_minor': None,
            'sicha_minor_strength': None,
            'jincha_major': None,
            'jincha_major_strength': None,
            'sicha_major': None,
            'sicha_major_strength': None,
            'trend_short': None,
            'trend_short_strength': None,
            'trend_middle': None,
            'trend_middle_strength': None,
            'duotou_pailie': None,
            'trend_long': None,
            'very_strong_up_trend': None,
            'duotou_pailie_last_bars': None,
            'last_kongtou_pailie_n_days_before': None,
            'last_kongtou_pailie_date': None,
            'kongtou_pailie': None,
            'very_strong_down_trend': None,
            'kongtou_pailie_last_bars': None,
            'last_duotou_pailie_n_days_before': None,
            'last_duotou_pailie_date': None,
        }

        df_sma_short = df['sma_short_' + str(short)]
        df_sma_middle = df['sma_middle_' + str(middle)]
        df_sma_long = df['sma_long_' + str(long)]

        rtn_dict['date'] = df['date'].iloc[-1]
        rtn_dict['code'] = df['code'].iloc[-1]
        rtn_dict['close'] = df['close'].iloc[-1]

        sma_short = rtn_dict['sma_short'] = df_sma_short.iloc[-1]
        sma_middle = rtn_dict['sma_middle'] = df_sma_middle.iloc[-1]
        sma_long = rtn_dict['sma_long'] = df_sma_long.iloc[-1]

        #print("stockstats sma short,middle,long " + str(sma_short) + " " + str(sma_middle) + " " + str(sma_long))

        df_ema_short = df['ema_short_' + str(short)]
        df_ema_middle = df['ema_middle_' + str(middle)]
        df_ema_long = df['ema_long_' + str(long)]
        #print("stockstats ema short,middle,long " + str(df_ema_short) + " " + str(df_ema_middle) + " " + str(df_ema_long))
        ema_short = rtn_dict['ema_short'] = df_ema_short.iloc[-1]
        ema_middle = rtn_dict['ema_middle'] = df_ema_middle.iloc[-1]
        ema_long = rtn_dict['ema_long'] = df_ema_long.iloc[-1]

        sma_short_p1 = df_sma_short.iloc[-2]
        sma_middle_p1 = df_sma_middle.iloc[-2]
        sma_long_p1 = df_sma_long.iloc[-2]

        #middle tier start
        ma_short = sma_short
        ma_short_p1 = sma_short_p1

        ma_middle = sma_middle
        ma_middle_p1 = sma_middle_p1

        ma_long = sma_long
        ma_long_p1 = sma_long_p1
        #middle tier end

        if ma_short > ma_middle and ma_short_p1 < ma_middle_p1:
            logging.info("short up across middle, jin cha minor")
            rtn_dict['reason'] += constant.MA_JIN_CHA_MINOR+'; '
            rtn_dict['jincha_minor'] = True
            rtn_dict['jincha_minor_strength'] = round(2 * ((ma_short - ma_middle) / (ma_short + ma_middle) + (ma_middle_p1 - ma_short_p1) / (ma_middle_p1 + ma_short_p1)), 2)
        elif ma_short < ma_middle and ma_short_p1 > ma_middle_p1:
            logging.info("short down across middle, si cha minor")
            rtn_dict['sicha_minor'] = True
            rtn_dict['reason'] += constant.MA_SI_CHA_MINOR+'; '

            rtn_dict['sicha_minor_strength'] = round(2 * ((ma_middle - ma_short) / (ma_short + ma_middle) + (ma_short_p1 - ma_middle_p1) / (ma_middle_p1 + ma_short_p1)), 2)

        if ma_middle > ma_long and ma_middle_p1 < ma_long_p1:
            logging.info("middle up across long, jin cha major")
            rtn_dict['jincha_major'] = True
            rtn_dict['reason'] += constant.MA_JIN_CHA_MAJOR+'; '

            rtn_dict['jincha_major_strength'] = round(2 * ((ma_middle - ma_long) / (ma_long + ma_middle) + (ma_long_p1 - ma_middle_p1) / (ma_middle_p1 + ma_long_p1)), 2)

        elif ma_middle < ma_long and ma_middle_p1 > ma_long_p1:
            logging.info("middle down across long, si cha major")
            rtn_dict['sicha_major'] = True
            rtn_dict['reason'] += constant.MA_SI_CHA_MAJOR+'; '

            rtn_dict['sicha_major_strength'] = round(2 * ((ma_long - ma_middle) / (ma_long + ma_middle) + (ma_middle_p1 - ma_long_p1) / (ma_middle_p1 + ma_long_p1)), 2)

        if ma_short > ma_middle * 1.05:
            trend_short = 'up'
            rtn_dict['trend_short'] = 'up'
            rtn_dict['reason'] += constant.SHORT_TREND_UP+'; '

            rtn_dict['trend_short_strength'] = round(ma_short / ma_middle, 2)
        elif ma_short * 1.05 < ma_middle:
            trend_short = 'down'
            rtn_dict['trend_short'] = 'down'
            rtn_dict['reason'] += constant.SHORT_TREND_DOWN+'; '
            rtn_dict['trend_short_strength'] = round(ma_middle / ma_short, 2)

        if ma_middle > ma_long * 1.05:
            trend_middle = 'up'
            rtn_dict['trend_middle'] = 'up'
            rtn_dict['reason'] += constant.MIDDLE_TREND_UP+'; '
            rtn_dict['trend_middle_strength'] = round(ma_middle / ma_long, 2)
        elif ma_middle * 1.05 < ma_long:
            trend_middle = 'down'
            rtn_dict['trend_middle'] = 'down'
            rtn_dict['reason'] += constant.MIDDLE_TREND_DOWN+'; '
            rtn_dict['trend_middle_strength'] = round(ma_long / ma_middle, 2)

        if (ma_short > ma_middle > ma_long):
            rtn_dict['duotou_pailie'] = True
            rtn_dict['reason'] += constant.MA_DUO_TOU_PAI_LIE+'; '

            rtn_dict['trend_long'] = 'up'
            rtn_dict['reason'] += constant.LONG_TREND_UP+'; '

            logging.info("duo tou pai lie")
            if df.iloc[-1]['low'] > ma_short:
                logging.info("verify strong up trend")
                rtn_dict['very_strong_up_trend'] = True
                rtn_dict['reason'] += constant.VERY_STONG_UP_TREND+'; '

            logging.info("check back last 30 bars")

            rtn_dict['duotou_pailie_last_bars'] = 0
            rtn_dict['last_kongtou_pailie_n_days_before'] = 0
            for i in range(30):
                if (df_sma_short.iloc[-i] > df_sma_middle.iloc[-i] > df_sma_long.iloc[-i]):
                    logging.info("duo tou lasts " + str(i) + "days")
                    n_ma_dtpl_days = i
                    rtn_dict['duotou_pailie_last_bars'] = i
                    continue

                if (df_sma_short.iloc[-i] < df_sma_middle.iloc[-i] < df_sma_long.iloc[-i]):
                    logging.info("latest kong tou pailie is " + str(i) + " days before at " + df.iloc[-i]['date'])
                    n_last_ma_dtpl_days = i
                    rtn_dict['last_kongtou_pailie_n_days_before'] = i
                    rtn_dict['last_kongtou_pailie_date'] = df.iloc[-i]['date']
                    break

            rtn_dict['reason'] += constant.MA_DUO_TOU_PAI_LIE_N_days + "_" + str(rtn_dict['duotou_pailie_last_bars']) + '; '
            rtn_dict['reason'] += constant.MA_LAST_KONG_TOU_PAI_LIE_N_days + "_" + str(rtn_dict['last_kongtou_pailie_n_days_before']) + '; '

        if (ma_short < ma_middle < ma_long):  #more interesting enter when price is up break
            rtn_dict['kongtou_pailie'] = True
            rtn_dict['reason'] += constant.MA_KONG_TOU_PAI_LIE+'; '

            rtn_dict['trend_long'] = 'down'
            rtn_dict['reason'] += constant.LONG_TREND_DOWN+'; '

            logging.info("kong tou pai lie")
            if df.iloc[-1]['high'] < ma_short:
                logging.info("verify strong down trend")
                rtn_dict['very_strong_down_trend'] = True
                rtn_dict['reason'] += constant.VERY_STONG_DOWN_TREND+'; '

            logging.info("check back last 30 bars")
            for i in range(30):
                if (df_sma_short.iloc[-i] < df_sma_middle.iloc[-i] < df_sma_long.iloc[-i]):
                    logging.info("kong tou lasts " + str(i) + "days")
                    rtn_dict['kongtou_pailie_last_bars'] = i
                    rtn_dict['reason'] += constant.MA_KONG_TOU_PAI_LIE_N_days+"_"+str(i)+'; '
                    continue

                if (df_sma_short.iloc[-i] > df_sma_middle.iloc[-i] > df_sma_long.iloc[-i]):
                    logging.info("latest duo tou pailie is " + str(i) + " days before at " + df.iloc[-i]['date'])
                    rtn_dict['last_duotou_pailie_n_days_before'] = i
                    rtn_dict['reason'] += constant.MA_LAST_DUO_TOU_PAI_LIE_N_days+"_"+str(i)+'; '
                    rtn_dict['last_duotou_pailie_date'] = df.iloc[-i]['date']
                    break

        d = {}
        for k in rtn_dict.keys():
            d[k] = [rtn_dict[k]]

        return (pd.DataFrame(d))

    #########################################################
    # recommended df len is 300.
    #
    #         {58.0: {'price': 58.0,
    #          'frequency_rank': 1,
    #          'frequency_perc': 100.0,
    #          'occurrence': 141,
    #          'sum': 1176,
    #          'occurrence_perc': 12.0}
    #
    #########################################################
    def price_counter(self, df, accuracy=0):
        rtn_dict = {}
        ser_price = df['close'].append(df['open']).append(df['high']).append(df['low'])
        ser_price = ser_price[ser_price > 0]

        #round determin the precision.
        common_prices = collections.Counter(round(ser_price, accuracy)).most_common()

        sum = 0
        occu_list = []
        for i in common_prices:
            sum += i[1]
            occu_list.append(i[1])

        new_dict = {}

        for i in common_prices:
            price = i[0]
            frequency_percent = round(stats.percentileofscore(occu_list, i[1]), 1)
            occurrence_percent = round(i[1] * 100 / sum, 1)
            new_dict[price] = {'price': price, 'frequency_percent': frequency_percent, 'occurrence_percent': occurrence_percent, 'occurrence': i[1], 'sum': sum}

        sorted_price_list = list(collections.OrderedDict(sorted(new_dict.items(), reverse=True)).keys())  #[71,70,...44]

        rtn_dict['price_freq_dict'] = new_dict
        rtn_dict['sorted_price_list'] = sorted_price_list

        current_price = df['close'].iloc[-1]
        logging.info("current price " + str(current_price))
        rtn_dict['close'] = current_price

        v = min(sorted_price_list, key=lambda x: abs(x - current_price))
        idx = sorted_price_list.index(v)

        if v > current_price:
            idx_h1 = idx
            idx_l1 = idx + 1
        elif v == current_price:
            idx_h1 = idx - 1
            idx_l1 = idx + 1
        elif v < current_price:
            idx_h1 = idx - 1
            idx_l1 = idx

        if idx_h1 - 4 >= 0:
            H5 = new_dict[sorted_price_list[idx_h1 - 4]]
            rtn_dict['h5'] = H5
            rtn_dict['h5_frequency_percent'] = H5['frequency_percent']
            logging.info("H5, price " + str(H5['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(H5['frequency_percent']) + " freq " + str(H5['occurrence_percent']))
        if idx_h1 - 3 >= 0:
            H4 = new_dict[sorted_price_list[idx_h1 - 3]]
            rtn_dict['h4'] = H4
            rtn_dict['h4_frequency_percent'] = H4['frequency_percent']
            logging.info("H4, price " + str(H4['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(H4['frequency_percent']) + " freq " + str(H4['occurrence_percent']))
        if idx_h1 - 2 >= 0:
            H3 = new_dict[sorted_price_list[idx_h1 - 2]]
            rtn_dict['h3'] = H3
            rtn_dict['h3_frequency_percent'] = H3['frequency_percent']
            logging.info("H3, price " + str(H3['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(H3['frequency_percent']) + " freq " + str(H3['occurrence_percent']))
        if idx_h1 - 1 >= 0:
            H2 = new_dict[sorted_price_list[idx_h1 - 1]]
            rtn_dict['h2'] = H2
            rtn_dict['h2_frequency_percent'] = H2['frequency_percent']
            logging.info("H2, price " + str(H2['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(H2['frequency_percent']) + " freq " + str(H2['occurrence_percent']))
        if idx_h1 >= 0:
            H1 = new_dict[sorted_price_list[idx_h1]]
            rtn_dict['h1'] = H1
            rtn_dict['h1_frequency_percent'] = H1['frequency_percent']
            logging.info("H1, price " + str(H1['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(H1['frequency_percent']) + " freq " + str(H1['occurrence_percent']))

        if idx_l1 < sorted_price_list.__len__():
            L1 = new_dict[sorted_price_list[idx_l1]]
            rtn_dict['l1'] = L1
            rtn_dict['l1_frequency_percent'] = L1['frequency_percent']
            logging.info("L1, price " + str(L1['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(L1['frequency_percent']) + " freq " + str(L1['occurrence_percent']))

        if idx_l1 + 1 < sorted_price_list.__len__():
            L2 = new_dict[sorted_price_list[idx_l1 + 1]]
            rtn_dict['l2'] = L2
            rtn_dict['l2_frequency_percent'] = L2['frequency_percent']
            logging.info("L2, price " + str(L2['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(L2['frequency_percent']) + "  freq " + str(L2['occurrence_percent']))

        if idx_l1 + 2 < sorted_price_list.__len__():
            L3 = new_dict[sorted_price_list[idx_l1 + 2]]
            rtn_dict['l3'] = L3
            rtn_dict['l3_frequency_percent'] = L3['frequency_percent']
            logging.info("L3, price " + str(L3['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(L3['frequency_percent']) + " freq " + str(L3['occurrence_percent']))

        if idx_l1 + 3 < sorted_price_list.__len__():
            L4 = new_dict[sorted_price_list[idx_l1 + 3]]
            rtn_dict['l4'] = L4
            rtn_dict['l4_frequency_percent'] = L4['frequency_percent']
            logging.info("L4, price " + str(L4['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(L4['frequency_percent']) + " freq " + str(L4['occurrence_percent']))

        if idx_l1 + 4 < sorted_price_list.__len__():
            L5 = new_dict[sorted_price_list[idx_l1 + 4]]
            rtn_dict['l5'] = L5
            rtn_dict['l5_frequency_percent'] = L5['frequency_percent']
            logging.info("L5, price " + str(L5['price']) + ", freq perc in " + str(df.__len__()) + " bars " + str(L5['frequency_percent']) + " freq " + str(L5['occurrence_percent']))

        pass
        return (rtn_dict)

    # Query stocks match 'query', e.g get stock price under 60 days sma.
    #  The source_csv generated by
    #  python t_daily_indicator_kdj_macd.py --indicator MACD --period D
    #  python t_daily_indicator_kdj_macd.py --indicator MA_CROSS_OVER --period D
    #  python t_daily_junxian_barstyle.py -x AG --selected

    def get_indicator_critirial(self, query, period='D', fastMa=21, slowMa=55,market='ag', selected=False):

        # if query == constant.HS300_INDEX_BUY_CANDIDATE:
        #     print('debug stop')

        #column name for the query, default is 'reason'. The index candidate csv is 'predict'
        column_name = 'reason'

        #how many top records be returned.  The index candidate is about 10% of the index capacity.
        top_n = 0

        if selected:
            dir = "/home/ryan/DATA/result/selected"
        else:
            dir = "/home/ryan/DATA/result"

        if query in [constant.CLOSE_UNDER_SMA60,
                     constant.CLOSE_ABOVE_SMA60,
                     constant.MACD_DIF_MAIN_OVER_0_N_DAYS,
                     constant.MACD_DEA_SIGNAL_OVER_0_N_DAYS,
                     constant.MACD_HISTOGRAM_OVER_0_N_DAYS,
                     constant.MA55_NEAR_MA21,
                     constant.MACD_CLIMB_NEAR_0,
                     constant.MACD_DECLINE_NEAR_0,

                     constant.MACD_CROSS_OVER_0,
                     constant.MACD_CROSS_DOWN_0,
                     constant.MACD_DIF_CROSS_OVER_0,
                     constant.MACD_DIF_CROSS_DOWN_0,
                     constant.MACD_SIG_CROSS_OVER_0,
                     constant.MACD_SIG_CROSS_DOWN_0,

                     constant.MACD_DIF_CROSS_DOWN_SIG,
                     constant.MACD_DIF_CROSS_OVER_SIG,

                     constant.MACD_DIF_LT_0,
                     constant.MACD_DIF_GT_0,
                     constant.MACD_SIG_LT_0,
                     constant.MACD_SIG_GT_0,
                     constant.MACD_DIF_LT_SIG,
                     constant.MACD_DIF_GT_SIG,

                     constant.CLOSE_ABOVE_SMA60,
                     constant.CLOSE_ABOVE_SMA60,

                     constant.SMA21_UNDER_SMA60,

                     constant.SELL_MUST,
                     constant.BUY_MUST,

                     ]:
            source_csv = dir + "/macd_selection_"+period+".csv"
        elif query in [constant.CLOSE_ABOVE_MA5_N_DAYS,
                        constant.CLOSE_NEAR_MA5_N_DAYS,
                        constant.MA21_NEAR_MA55_N_DAYS,
                        constant.SMA_CROSS_OVER,
                        ]:
            source_csv = dir + "/ma_cross_over_selection_"+str(fastMa)+"_"+str(slowMa)+".csv"
        elif query in [constant.BAR_SMALL_BODY,
                       constant.BAR_CROSS_STAR,
                       constant.BAR_GUANG_TOU,
                       constant.BAR_GUANG_JIAO,
                       constant.BAR_LONG_UPPER_SHADOW,
                       constant.BAR_LONG_LOWER_SHADOW,
                       constant.BAR_YUNXIAN_BUY,
                       constant.BAR_YUNXIAN_SELL,

                       constant.VERY_STONG_DOWN_TREND,
                       constant.VERY_STONG_UP_TREND,
                       constant.MA_JIN_CHA_MINOR,
                       constant.MA_JIN_CHA_MAJOR,
                       constant.MA_SI_CHA_MINOR,
                       constant.MA_SI_CHA_MAJOR,
                       constant.MA_DUO_TOU_PAI_LIE,
                       constant.MA_KONG_TOU_PAI_LIE,
                       constant.SHORT_TREND_UP,
                       constant.SHORT_TREND_DOWN,
                       constant.MIDDLE_TREND_UP,
                       constant.MIDDLE_TREND_DOWN,
                       constant.LONG_TREND_UP,
                       constant.LONG_TREND_DOWN,

                       ]:
            source_csv = dir+'/'+market+'_junxian_barstyle.csv'

        elif query in [constant.HS300_INDEX_BUY_CANDIDATE,
                       ]:
            source_csv = dir+'/hs300_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_ADDED

        elif query in [constant.HS300_INDEX_SELL_CANDIDATE,
                       ]:
            source_csv = dir+'/hs300_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_REMOVED



        elif query in [constant.SZ100_INDEX_BUY_CANDIDATE,
                       ]:
            source_csv = dir+'/sz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_ADDED

        elif query in [constant.SZ100_INDEX_SELL_CANDIDATE,
                       ]:
            source_csv = dir+'/sz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_REMOVED


        elif query in [constant.ZZ100_INDEX_BUY_CANDIDATE,
                       ]:
            source_csv = dir+'/zz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_ADDED

        elif query in [constant.ZZ100_INDEX_SELL_CANDIDATE,
                       ]:
            source_csv = dir+'/zz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_REMOVED


        elif query in [constant.SZCZ_INDEX_BUY_CANDIDATE,
                       ]:
            source_csv = dir+'/szcz_candidate_list.csv'
            column_name='predict'
            top_n = 32
            query = constant.TO_BE_ADDED

        elif query in [constant.SZCZ_INDEX_SELL_CANDIDATE,
                       ]:
            source_csv = dir+'/szcz_candidate_list.csv'
            column_name='predict'
            top_n = 32
            query = constant.TO_BE_REMOVED


        elif query in [constant.MA5_UP_KOUDI_DISTANCE_GT_5,
                       constant.MA21_UP_KOUDI_DISTANCE_GT_5,
                       constant.MA55_UP_KOUDI_DISTANCE_GT_5,

                       constant.MA5_UP_KOUDI_DISTANCE_LT_1,
                       constant.MA21_UP_KOUDI_DISTANCE_LT_1,
                       constant.MA55_UP_KOUDI_DISTANCE_LT_1,

                       constant.TWO_WEEK_FLUC_SMA_5_LT_3,
                       constant.TWO_WEEK_FLUC_SMA_21_LT_3,
                       constant.TWO_WEEK_FLUC_SMA_55_LT_3,

                       ]:
            source_csv = dir+'/latest_ma_koudi.csv'
            column_name='reason'


        elif query in [constant.PV2_VOLUME_RATIO_BOTTOM_10P]:
            return(pd.read_csv(dir+'/pv_2/latest/volume_ratio_bottom_10p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_VOLUME_RATIO_TOP_20P]:
            return(pd.read_csv(dir+'/pv_2/latest/volume_ratio_top_20p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANGTING_VOLUME_RATIO_LT_1]:
            return(pd.read_csv(dir+'/pv_2/latest/zhangting_volume_ration_lt_1.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANGTING_VOLUME_RATIO_LT_1]:
            return(pd.read_csv(dir+'/pv_2/latest/zhangting_volume_ration_lt_1.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_POCKET_PIVOT]:
            return(pd.read_csv(dir+'/pv_2/latest/pocket_pivot.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_DIE_TING]:
            return(pd.read_csv(dir+'/pv_2/latest/die_ting.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANG_TING]:
            return(pd.read_csv(dir+'/pv_2/latest/zhang_ting.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_PE_TOP_30P]:
            return(pd.read_csv(dir+'/pv_2/latest/pe_top_30p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_PE_BOTTOM_30P]:
            return(pd.read_csv(dir+'/pv_2/latest/pe_bottom_30p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_STABLE_PRICE_VOLUME]:
            return(pd.read_csv(dir+'/pv_2/latest/stable_price_volume.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [
                       # constant.DOUBLE_BOTTOM_AG_SELECTED,
                       # constant.DOUBLE_BOTTOM_AG,

                       constant.DOUBLE_BOTTOM_123_LONG_TREND_REVERSE,
                       constant.DOUBLE_BOTTOM_123_LONG_TREND_CONTINUE,
                       constant.DOUBLE_BOTTOM_VERY_GOOD_RIGHT_MIN_SLOP_DEGREE,
                       constant.DOUBLE_BOTTOM_VERY_GOOD_RIGHT_MAX_SLOP_DEGREE,
                       ]:
            # df = pd.read_csv(dir+'/ag_curve_shape.csv', converters={'code': str}, encoding="utf-8")
            # df = df[df['hit']==True].reset_index().drop('index', axis=1)
            # return(df)

            source_csv = dir+'/ag_curve_shape.csv'
            column_name='reason'

        else:
            logging.error("Unknow source csv that matching query "+query)
            exit(0)

        df = pd.read_csv(source_csv, encoding="utf-8")

        df[column_name] = df[column_name].fillna('')
        df_match = df[df[column_name].str.contains(query)].reset_index()

        if top_n > 0:
            df_match = df_match.head(top_n)


        if 'index' in df_match.columns:
            df_match = df_match.drop('index', axis=1)

        perc = round(df_match.__len__()*100/df.__len__(), 2)
        logging.info("Period: "+period+", Query: "+query+", "+str(df_match.__len__())+" of "+str(df.__len__())+", perc "+str(perc))
        finlib.Finlib().pprint(df_match.head(2))

        col = ['code', 'name', 'date', 'close', 'action','strength','reason','operation',"total_mv_perc","amount_perc",
        "my_index_weight",
        "weight",
        "mkt_cap",
        "predict",
        ]
        df_match = finlib.Finlib().keep_column(df_match, col)

        return(df_match)





    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def data_smoother(self, data_list, fill_na=False, fill_prev=True):
        #data_list = [7, 6, 5, 5.1, 4, 3, 3.1, 2, 1, 2, 3, 4, 5]
        df = pd.DataFrame.from_dict({'data': data_list})
        df['perc_chg'] = df['data'].pct_change() * 100
        df['perc_chg_mean_win2'] = df['perc_chg'].rolling(2).mean().shift()

        df['condition1'] = df['perc_chg'] * df['perc_chg_mean_win2']
        df['condition2'] = (df['perc_chg_mean_win2'].abs() + df['perc_chg'].abs()) * 100 / df['perc_chg'].abs()

        # if trend reversed, and reverse is very slightly ( previous 2 windows mean change MUCH GREAT 2times as this change)
        # then ignore this reverse, by filling data with data in previous row.
        df_outlier = df[(df['condition1'] < 0) & (df['condition2'] > 200)]

        for i in df_outlier.index.values:
            if fill_prev:
                # df.iloc[i].data = df.iloc[i - 1].data
                df.iloc[i]['data'] = df.iloc[i - 1]['data']
            if fill_na:
                df.iloc[i].data = np.nan

        rtn_list = list(df['data'])
        # logging.info("after smoothing, rtn_list " + str(rtn_list))
        return(rtn_list)

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    # zscore = (value - mean)/std
    #The basic z score formula for a sample is:
    # z = (x – μ) / σ
    # For example, let’s say you have a test score of 190. The test has a mean (μ) of 150 and a standard deviation (σ) of 25. Assuming a normal distribution, your z score would be:
    # z = (x – μ) / σ
    # = (190 – 150) / 25 = 1.6.

    def get_outier(self, df, on_column, zscore_threshold=3):
        rtn_dict={}

        df = df[df[on_column].notna()]
        df['zscore_'+on_column] = stats.zscore(df[on_column])

        o_all = df[abs(df['zscore_'+on_column]) > zscore_threshold].reset_index().drop('index', axis=1)
        o_min = df[df['zscore_'+on_column] < -1*zscore_threshold].reset_index().drop('index', axis=1)
        o_max = df[df['zscore_'+on_column] > zscore_threshold ].reset_index().drop('index', axis=1)

        return(o_all,o_min, o_max)


    #input: df [open,high, low, close]
    #output:
    def _get_key_support_price_from_pv(self, df, period):
        df['increase'] = df['close'].pct_change()
        df['inday_fluctuation'] = round((df['high'] - df['low'])/df['low'], 2)
        df['inday_increase'] = round((df['close'] - df['open'])/df['open'],2)
        #
        # df_t = df[['code','date','increase', 'volume','inday_fluctuation', 'inday_increase']]
        # print(df_t.corr())

        (df_outier_increase, df_low_outier_increase, df_high_outier_increase) = self.get_outier(df=df, on_column='increase',zscore_threshold=1)
        (df_outier_inday_increase, df_low_outier_inday_increase, df_high_outier_inday_increase) = self.get_outier(df=df, on_column='inday_increase',zscore_threshold=1)
        (df_outier_inday_fluctuation, _df_low_outier_inday_fluctuation, df_high_outier_inday_fluctuation) = self.get_outier(df=df, on_column='inday_fluctuation',zscore_threshold=1)

        (df_outier_volume, _df_low_outier_volume, df_high_outier_volume) = self.get_outier(df=df, on_column='volume',zscore_threshold=1)

        max_increase_dict = {}
        if df_high_outier_increase.__len__() > 0:
            _row = df_high_outier_increase.sort_values(by='zscore_increase', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_increase_dict = {
                "date": _row.date,
                "open": _row.open,
                "high" : _row.high,
                "low" : _row.low,
                "close" : _row.close,
            }


        max_inday_fluctuation_dict = {}
        if df_high_outier_inday_fluctuation.__len__() > 0:
            _row = df_high_outier_inday_fluctuation.sort_values(by='zscore_inday_fluctuation', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_inday_fluctuation_dict = {
                "date": _row.date,
                "open": _row.open,
                "high" : _row.high,
                "low" : _row.low,
                "close" : _row.close,
            }



        max_volume_dict = {}
        if df_high_outier_volume.__len__() > 0:
            _row = df_high_outier_volume.sort_values(by='zscore_volume', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_volume_dict = {
                "date": _row.date,
                "open": _row.open,
                "high" : _row.high,
                "low" : _row.low,
                "close" : _row.close,

            }

        return({
            "period":period,
            "period_cnt":df.__len__(),
            "max_increase":max_increase_dict,
            "max_inday_fluctuation":max_inday_fluctuation_dict,
            "max_volume":max_volume_dict,
        })

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    # trading days of 2021 : 252 , half year 126,  quarter: 63, month: 21, half month: 10, week: 5
    def get_support_price_by_price_volume(self, df_daily_ohlc_volume, verify_last_n_days=120):
        df_daily_ohlc_volume = df_daily_ohlc_volume.tail(verify_last_n_days)
        _t = finlib.Finlib().daily_to_monthly_bar(df_daily_ohlc_volume)
        df_weekly = _t['df_weekly']
        df_monthly = _t['df_monthly']
        daily_support_dict = self._get_key_support_price_from_pv(df=df_daily_ohlc_volume, period='D')
        weekly_support_dict = self._get_key_support_price_from_pv(df=df_weekly, period='W')
        monthly_support_dict = self._get_key_support_price_from_pv(df=df_monthly, period='M')

        return(
            {
                'daily_support':daily_support_dict,
                'weekly_support':weekly_support_dict,
                'monthly_support':monthly_support_dict,
            }

        )


    def print_support_price_by_price_volume(self, data_csv):

        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=data_csv)
        last_price = df.iloc[-1].close
        last_date = df.iloc[-1].date
        code = df.iloc[-1].code

        a_dict = self.get_support_price_by_price_volume(df_daily_ohlc_volume=df,  verify_last_n_days=250)

        p_list = []
        for k1 in a_dict.keys():
            for k2 in a_dict[k1].keys():
                if type(a_dict[k1][k2]) is dict  and a_dict[k1][k2].__len__() > 0:
                    # print(a_dict[k1][k2])
                    p_list.append(a_dict[k1][k2]['open'])
                    p_list.append(a_dict[k1][k2]['high'])
                    p_list.append(a_dict[k1][k2]['low'])
                    p_list.append(a_dict[k1][k2]['close'])

        support = pd.Series(p_list).sort_values().reset_index().drop('index', axis=1).T
        delta_perc = round((support - last_price) * 100 / last_price, 2)
        s = support.append(delta_perc).reset_index().drop('index', axis=1)

        spt2 = support.T
        last_price_rank = spt2[spt2[0] < last_price].__len__()

        logging.info("\n\nkey price list and perctage distance, code "+str(code)+", date "+last_date+ ", close "+str(round(last_price,2))+", rank "+str(last_price_rank) +"/"+str(spt2.__len__()))

        # print s every 10 columns
        col_p = 0
        for i in range(s.columns.__len__()//10):
            # print(list(range(col_p,col_p+10)))
            logging.info(finlib.Finlib().pprint(df=s[list(range(col_p,col_p+10))]))
            col_p = col_p+10

        # print(list(range(col_p, s.columns.__len__())))
        logging.info(finlib.Finlib().pprint(df=s[list(range(col_p, s.columns.__len__()))]))

        return(s)


    def my_ma_koudi(self, df):
        code = df.iloc[0].code
        period = 5  # using MA5
        look_back_records = 3  # check last three records. eg 3: Day_b4_MA, Day_b3_MA, Day_b2_MA.
        last_N = period + look_back_records + 1
        # last_N = 100 #ryan debug

        if df.__len__() < last_N:
            logging.info("No enough data in df, expected df len "+str(last_N))
            return

        name = ''
        if 'name' in df.columns:
            name = df.iloc[-1]['name']

        df = self.add_ma_ema_simple(df=df)
        # df = self.add_tr_atr(df=df)
        df = df.tail(last_N)
        # df_simple = df[['code', 'date', 'close', 'close_sma_5', 'tr', 'atr_short_5']]
        df_simple = df[['code', 'date', 'close', 'close_sma_5']]

        a1 = df_simple[['close']].shift(0).fillna(0) - df_simple[['close']].shift(1).fillna(0) +df_simple[['close']].shift(period+1).fillna(0) - df_simple[['close']].shift(period).fillna(0)  # consider today close, suppose tomorror close is zero.
        a2 = df_simple[['close']].shift(period).fillna(0) - df_simple[['close']].shift(period - 1).fillna(0)  # assume tomorrow close is same as today.
        b = df_simple[['close_sma_5']].shift(1).fillna(0) - df_simple[['close_sma_5']].shift(2).fillna(0)
        df_simple['delta_MA1'] = a1['close'] / period + b['close_sma_5']
        df_simple['delta_MA2'] = a2['close'] / period + b['close_sma_5']
        df_simple['delta_MA3'] = df_simple['close_sma_5'] - df_simple['close_sma_5'].shift(1)
        df_simple['delta_MA_chg_perc'] = round(df_simple['delta_MA3']*100/df_simple['close_sma_5'].shift(1), 2)

        df_simple = df_simple[['code', 'date', 'close', 'delta_MA_chg_perc']]
        # print(finlib.Finlib().pprint(df_simple.tail(50)))
        # exit(0)  #ryan debug

        Day_b4_delta_MA_chg_perc = round(df_simple.iloc[-4].delta_MA_chg_perc, 2)
        Day_b3_delta_MA_chg_perc = round(df_simple.iloc[-3].delta_MA_chg_perc, 2)
        Day_b2_delta_MA_chg_perc = round(df_simple.iloc[-2].delta_MA_chg_perc, 2)
        today_predicated_delta_MA_chg_perc = round(df_simple.iloc[-1].delta_MA_chg_perc, 2)
        strength = round(today_predicated_delta_MA_chg_perc - Day_b2_delta_MA_chg_perc, 2)

        # -0.1 in after times 100, it is -0.1 percent. original number is -0.001
        if Day_b4_delta_MA_chg_perc < 0 and Day_b3_delta_MA_chg_perc < 0 and Day_b2_delta_MA_chg_perc < 0 and today_predicated_delta_MA_chg_perc > 0:
            logging.info("strength "+str(strength)+", BUY " + code +" "+name+ " before today market close. based on price " + str(df_simple.iloc[-1].close)
                  + " delta_MAs: " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(Day_b2_delta_MA_chg_perc) + " " + str(
                today_predicated_delta_MA_chg_perc)
                  )
        elif Day_b4_delta_MA_chg_perc > 0 and Day_b3_delta_MA_chg_perc > 0 and Day_b2_delta_MA_chg_perc > 0 and today_predicated_delta_MA_chg_perc < 0:
            logging.info("strength "+str(strength)+", SELL " + code +" "+name+ " before today market close. based on price " + str(df_simple.iloc[-1].close)
                  + " delta_MAs: " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(Day_b2_delta_MA_chg_perc) + " " + str(
                today_predicated_delta_MA_chg_perc)
                  )
        else:
            logging.info("strength "+str(strength)+" code " + code+" "+name+ " No operation. based on price " + str(df_simple.iloc[-1].close)+" delta_MAs " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(
                Day_b2_delta_MA_chg_perc) + " " + str(today_predicated_delta_MA_chg_perc))

        return()

    def check_my_ma(self, selected=True, stock_global='AG_HOLD',allow_delay_min = 30, force_fetch=False):
        rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
        out_dir = rst['out_dir']
        csv_dir = rst['csv_dir']
        stock_list = rst['stock_list']

        root_dir = '/home/ryan/DATA/DAY_Global'
        if stock_global in ['US', 'US_INDEX']:
            root_dir = root_dir + "/stooq/" + stock_global
        else:
            root_dir = root_dir + "/" + stock_global

        df_rtn = pd.DataFrame()
        #################

        ############## Get live price before market closure.

        if stock_global in ['HK_HOLD','HK']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='HK', allow_delay_min=allow_delay_min,force_fetch=force_fetch)
        elif stock_global in ['AG_HOLD','AG']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='AG', allow_delay_min=allow_delay_min,force_fetch=force_fetch)
        elif stock_global in ['US_HOLD','US']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='US', allow_delay_min=allow_delay_min,force_fetch=force_fetch)

        logging.info("loaded in_day_price_df")

        ###############
        for index, row in stock_list.iterrows():
            code = row['code']  # SH600519
            data_csv = csv_dir + '/' + str(code).upper() + '.csv'

            df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=data_csv, exit_if_not_exist=False)

            if type(df) is str: # "FILE_NOT_EXIT"
                continue

            df = df[['code', 'date', 'open', 'high', 'low', 'close']]

            a_live_df = in_day_price_df[in_day_price_df['code'] == code]
            if a_live_df.__len__() == 0:
                logging.warning("not found current price of " + code)
                continue

            df_today = pd.DataFrame.from_dict(
                {
                    'code': [code],
                    'date': [datetime.today().strftime('%Y%m%d')],
                    'open': [a_live_df.open.values[0]],
                    'high': [a_live_df.high.values[0]],
                    'low': [a_live_df.low.values[0]],
                    'close': [a_live_df.close.values[0]],
                }
            )
            df = df.append(df_today).reset_index().drop('index', axis=1)
            df['name'] = a_live_df.iloc[0]['name']  # add name column. AK returns name in df.

            rtn = self.my_ma_koudi(df=df)

    def tv_login(self, browser,target_uri='https://www.tradingview.com/'):

        cookie_f = os.path.expanduser("~")+'/DATA/pickle/tradingview.cookie'

        if finlib.Finlib().is_cached(cookie_f, day=2):
            logging.info('tvlogin, load cookies from ' + cookie_f)

            browser.get('https://www.tradingview.com/')
            # time.sleep(10)
            self.tv_wait_page_to_ready(browser=browser, timeout=10)

            with open(cookie_f, "rb") as f:
                cookies = pickle.load(f)

            for c in cookies:
                browser.add_cookie(c)

            browser.get(target_uri)
            self.tv_wait_page_to_ready(browser=browser, timeout=10)


        else:
            browser.get(target_uri+'#signin')

            # browser.find_element_by_class_name('tv-header__area tv-header__area--user').click()

            browser.find_element_by_class_name('tv-signin-dialog__toggle-email').click()


            usr_box = browser.find_element_by_name('username')
            pwd_box = browser.find_element_by_name('password')


            usr_box.send_keys('sunraise2005@gmail.com')
            pwd_box.send_keys('fav8@Apple!_tv')

            browser.find_element_by_class_name('tv-button__loader').click()

            time.sleep(10)

            WebDriverWait(browser, 10).until(EC.title_contains("TradingView"))

            pickle.dump(browser.get_cookies(), open(cookie_f, "wb"))
            logging.info("tradingview login cookie saved to " + cookie_f)

        return(browser)

    def tv_screener_set_interval(self, browser, interval='1D'):
        # xp_interval = '/html/body/div[8]/div/div[2]/div[7]/div[2]'
        obj_interval = browser.find_element_by_css_selector('[data-name="screener-time-interval"]')

        # try:
        #     obj_interval = browser.find_element_by_xpath(xp_interval)
        # except:
        #     logging.warning("get interval error, "+xp_interval+" retry in 10sec")
        #     time.sleep(10)
        #     obj_cf = browser.find_element_by_xpath(xp_interval)

        if obj_interval.text == interval:
            logging.info("interval already be " + interval)
            return (browser)


        obj_interval.click()
        interval_list = browser.find_elements_by_class_name('js-select-interval')
        for i in interval_list:
            print(i.text)  # 1M 1W 1D 1h, 4h, 15m 5m  1m
            if i.text == interval:
                i.click()

        time.sleep(1)
        while browser.find_element_by_css_selector('[data-name="screener-time-interval"]').text != interval:
            logging.warning("interval has not set to " + interval)
            time.sleep(1)
        logging.info("interval has set to " + interval)
        return (browser)


    def tv_wait_page_to_ready(self,browser,timeout):
        _bs = browser.execute_script("return document.readyState")
        t = 0

        while _bs != "complete":
            print("page readystate: " + _bs)
            time.sleep(1)
            t += 1

            if t> timeout:
                print("timeout, page not ready,readystate: " + _bs)
                break

        return()

    def tv_screener_set_column_field(self, browser, column_filed='MA_CROSS'):
        # xp_cf = '/html/body/div[8]/div/div[2]/div[3]/div[1]'

        obj_cf = browser.find_element_by_css_selector('[data-name="screener-field-sets"]')

        #test

        # try:
        #     obj_cf = browser.find_element_by_xpath(xp_cf)
        # except:
        #     logging.warning("get column_filed error, "+xp_cf+" retry in 10sec")
        #     time.sleep(10)

        # if browser.find_element_by_xpath(xp_cf).text == column_filed:
        if obj_cf.text == column_filed:
            logging.info("column field already be " + column_filed)
            return(browser)

        # browser.find_element_by_xpath(xp_cf).click()
        obj_cf.click()
        self.tv_wait_page_to_ready(browser, timeout=10)

        column_layout_list = browser.find_elements_by_class_name('js-field-set-name')
        for layout in column_layout_list:
            # print(layout.text)
            if layout.text == column_filed:
                layout.click()
                self.tv_wait_page_to_ready(browser, timeout=10)
                break


        time.sleep(1)
        while obj_cf.text != column_filed:
            logging.warning("column filed has not set to " + column_filed)
            time.sleep(1)
        logging.info("column field has set to " + column_filed)
        return(browser)

    def tv_screener_set_market(self, browser, market='US'):
        # market has to be in ['SH','SZ', 'US', 'HK'], compliant with Tradingview, don't use other name like USA.
        if market in ['SH', 'SZ', 'CN']:
            market = 'CN'

        # xp_m = '/html/body/div[8]/div/div[2]/div[8]/div[1]/img'
        # try:
            # obj_m = browser.find_element_by_xpath(xp_m)
        obj_m = browser.find_element_by_css_selector('[data-name="screener-markets"]')
        # except:
            # logging.warning("get market error, "+xp_m+" retry in 10sec")
            # time.sleep(10)
            # obj_cf = browser.find_element_by_xpath(xp_m)

        if obj_m.find_element_by_xpath('img').get_attribute('alt').upper() == market:
            logging.info("market already be " + market)
            return(browser)

        obj_m.click()
        self.tv_wait_page_to_ready(browser, timeout=10)

        # #scroll down entire window 200 from current position
        # browser.execute_script("window.scrollTo(0, window.scrollY + 200);")
        if market == 'US':
            browser.find_element_by_css_selector('[data-market="america"]').click()
        elif market == 'CN':
            browser.find_element_by_css_selector('[data-market="china"]').click()
        elif market == 'HK':
            browser.find_element_by_css_selector('[data-market="hongkong"]').click()
        #
        # scroll_bar = browser.find_element_by_class_name("tv-screener-market-select").find_element_by_class_name("sb-scrollbar")
        # scroll_bar.location
        #
        # action = ActionChains(browser)
        # # action.move_to_element(scroll_bar).click()
        #
        # mkt_clicked = False
        # for j in range(100):
        #     if mkt_clicked:
        #         break
        #
        #     action.drag_and_drop_by_offset(source=scroll_bar, xoffset=0, yoffset=10)
        #     action.perform()
        #
        #     mkt_list = browser.find_element_by_class_name("tv-screener-market-select").find_element_by_class_name("tv-dropdown-behavior__inscroll").find_elements_by_class_name("tv-control-select__option-wrap")
        #
        #     for i in mkt_list:
        #
        #         im = i.get_attribute("data-market")
        #         # print(im) #USA (NASDAQ, NYSE, NYSE ARCA, OTC),  China (SSE, SZSE)
        #
        #         if market == 'US' and im == "america" and i.is_displayed():
        #             i.click()
        #             mkt_clicked = True
        #             break
        #         elif (market == 'CN') and im == "china" and i.is_displayed():
        #             i.click()
        #             mkt_clicked = True
        #             break
        #         elif market == 'HK' and im == "hongkong" and i.is_displayed():
        #             i.click()
        #             mkt_clicked = True
        #             break

        self.tv_wait_page_to_ready(browser, timeout=10)

        # obj_m =   # get element again. otherwise staled obj
        while browser.find_element_by_css_selector('[data-name="screener-markets"]').find_element_by_xpath('img').get_attribute('alt').upper() != market:
            logging.warning("market has not set to " + market)
            time.sleep(1)
            # obj_m = browser.find_element_by_xpath(xp_m) #refresh
        logging.info("market has set to " + market)
        return(browser)

    def tv_screener_set_filter(self, browser, filter):
        # xp_f = '/html/body/div[8]/div/div[2]/div[12]/div[1]'
        obj_f = browser.find_element_by_css_selector('[data-name="screener-filter-sets"]')

        # try:
        #     obj_f = browser.find_element_by_xpath(xp_f)
        # except:
        #     logging.warning("get filter error, "+xp_f+" retry in 10sec")
        #     time.sleep(10)
        #     obj_cf = browser.find_element_by_xpath(xp_f)

        if obj_f.text == filter:
            logging.info("filter already be " + filter)
            return(browser)

        obj_f.click()
        self.tv_wait_page_to_ready(browser, timeout=10)

        filter_list = browser.find_elements_by_class_name('js-filter-set-name')
        for f in filter_list:
            print(f.text)
            if f.text == filter:
                f.click()
                self.tv_wait_page_to_ready(browser, timeout=10)
                break
                time.sleep(3)



        time.sleep(5)  # waiting filter result, sometime slow.
        while browser.find_element_by_css_selector('[data-name="screener-filter-sets"]').text != filter:
            logging.warning("filter has not set to " + filter)
            time.sleep(1)

        logging.info("filter has set to " + filter)
        return(browser)


    def tv_save_result_table(self, browser, market='CN', parse_ticker_only=False, max_row = 20):
        columns = []
        delay_data_flag = True

        if market in ['SH','SZ']:
            market = 'CN'

        result_tbl = browser.find_elements_by_class_name('tv-data-table')
        tbl_header = result_tbl[0].find_elements_by_class_name('tv-data-table__th')

        if parse_ticker_only:
            columns.append(tbl_header[0].text)
        else:
            for h in tbl_header:
                # print(h.text)
                columns.append(h.text)

        df = pd.DataFrame(columns=columns)

        rows = result_tbl[1].find_elements_by_class_name('tv-data-table__row')
        row_index = 0

        #check if it's Delayed Data.
        if rows.__len__()>0:
            cell0_0 = rows[0].find_elements_by_class_name('tv-data-table__cell')[0]
            try:
                delay = cell0_0.find_element_by_class_name("tv-data-mode--delayed--for-screener")
                logging.info("Dalay Data")
            except:
                logging.info("No delay of the data")
                delay_data_flag = False


        r_cnt = 0
        for r in rows:
            if (max_row > 0) and (r_cnt > max_row):
                break

            r_cnt += 1


            r_data_list = []
            cells = r.find_elements_by_class_name('tv-data-table__cell')



            if parse_ticker_only:
                r_data_list.append(cells[0].text)
            else:
                for c in cells:
                    # print(c.text)
                    r_data_list.append(c.text)

            df.loc[row_index] = r_data_list
            row_index += 1

        if df.columns[0].startswith('TICKER'):
            col_raw_code_name = [df.columns[0]]

            df = pd.DataFrame([''] * df.__len__(), columns=["name_en"]).join(df)
            df = pd.DataFrame([''] * df.__len__(), columns=["code"]).join(df)

            for index, row in df.iterrows():
                v = row[col_raw_code_name][0]
                g = v.split('\n')

                if g.__len__() == 2:  # US
                    code = g[0]
                    name = g[1]
                elif g.__len__() == 3:  # CN
                    prefix = g[0]
                    code = g[1]
                    name = g[2]

                # remove Delay (D) flag from code
                if delay_data_flag and code.endswith('D'):
                    code = code.split('D')[0]

                df.iloc[index]['code'] = code
                df.iloc[index]['name_en'] = name

        df = df.drop(col_raw_code_name, axis=1)
        logging.info("result have parsed to df")

        if market == 'CN':
            df = finlib.Finlib().add_market_to_code(df)
            df = finlib.Finlib().add_stock_name_to_df(df)
        else:
            df = finlib.Finlib().add_stock_name_to_df_us_hk(df)
            # df = df.rename(columns={"name_en": "name"}, inplace=False)


        return(df)

    def newChromeBrowser(self, headless=False):
        # reduce webdriver session log for every request.
        logging.getLogger("urllib3").setLevel(logging.WARNING)  # This supress post/get log in console.

        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('headless')

        # Download Path
        prefs = {}
        prefs["profile.default_content_settings.popups"] = 0
        prefs["download.default_directory"] = os.getenv('CHROME_TMP_DOWNLOAD_DIR')
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
        browser = webdriver.Chrome(options=options)

        return browser


    def empty_chrome_tmp_download_dir(self):
        downloadPath = os.getenv('CHROME_TMP_DOWNLOAD_DIR')
        if os.path.isdir(downloadPath):
            shutil.rmtree(downloadPath)
            logging.info("rmdir "+downloadPath)

        os.mkdir(downloadPath)
        logging.info("mkdir "+downloadPath)

        return(downloadPath)

    def tv_screener_export(self, browser, to_dir, interval, symbol_link_f=None):
        dir = self.empty_chrome_tmp_download_dir()

        for e in browser.find_elements_by_class_name("tv-screener-toolbar__button"):
            tx = e.get_attribute("data-name")
            if tx == 'screener-export-data':
                e.click()

        # 20210111_IndexData_SH000300.xls
        while not os.listdir(dir):
            logging.info("waiting download complete, file not appear")
            time.sleep(5)

        while (os.listdir(dir)[0].rfind(".crdownload") > 1): #the index position of .crdownload, -1 if not include .crdownload
            logging.info("waiting download complete, file in .crdownload")
            time.sleep(5)

        fr_file = dir+"/"+os.listdir(dir)[0]
        to_file = to_dir+"/"+interval+"_"+os.listdir(dir)[0]
        time.sleep(10) #wait 10 seconds to let the download file completed.

        shutil.move(fr_file, to_file)
        logging.info("downloaded to  " + to_file)

        if symbol_link_f:
            if os.path.exists(symbol_link_f):
                os.unlink(symbol_link_f)

            os.symlink(to_file, symbol_link_f)
            logging.info("symbol link created. " + symbol_link_f + " --> " + to_file)


        return(to_file)




    def tv_screener_start(self,browser, column_filed, interval, market, filter):
        ######################################
        # Set Column fields
        ######################################
        browser = self.tv_screener_set_column_field(browser=browser, column_filed=column_filed)

        ######################################
        # Set period time window (4h, 1d etc)
        ######################################
        browser = self.tv_screener_set_interval(browser=browser, interval=interval)

        ######################################
        # Set market
        ######################################
        browser = self.tv_screener_set_market(browser=browser, market=market)

        ######################################
        # Set Filter
        ######################################
        browser = self.tv_screener_set_filter(browser=browser, filter=filter)

        return(browser)




    def _get_grid_spec(self,market='AG', high_field='52 Week High', low_field='52 Week Low', period='1D',all_columns=True):

        df = finlib.Finlib().load_tv_fund(market=market, period=period)

        code = df['code']
        p = df['close']
        high = df[high_field]
        low = df[low_field]
        atr_14d = df['atr_14']
        df['volatility'] = df['volatility'].apply(lambda _d: round(_d,2))



        delta = high - low
        df['eq_pos'] = round((high - p) / delta, 3) #current price to high, equity position percentage.
        df['cs_pos'] = round((p - low) / delta, 3) #current price to low, cash position percentage.

        df['l1'] = round(high,2)
        df['l2'] = round(low + delta * 0.764, 2)
        df['l3'] = round(low + delta * 0.618, 2)
        df['l4'] = round(low + delta * 0.5, 2)
        df['l5'] = round(low + delta * 0.382, 2)
        df['l6'] = round(low + delta * 0.236, 2)
        df['l7'] = round(low,2)

        cols = ['grid_cash_perc', 'grid','grid_support','grid_resistance','grid_perc_to_support','grid_perc_to_resistance']

        idx = df.close < df.l7
        df.loc[idx, cols]=[0,-4,None,df.loc[idx].l7,None,round((df.loc[idx].l7-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx = (df.l7 <= df.close) & (df.close< df.l6)
        df.loc[idx,cols]=[0.235, -3, df.loc[idx].l7, df.loc[idx].l6, round((df.loc[idx].close-df.loc[idx].l7)*100/df.loc[idx].close,1), round((df.loc[idx].l6-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx = (df.l6 <= df.close) & (df.close < df.l5)
        df.loc[idx, cols]=[0.382, -2, df.loc[idx].l6, df.loc[idx].l5, round((df.loc[idx].close-df.loc[idx].l6)*100/df.loc[idx].close,1), round((df.loc[idx].l5-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx = (df.l5 <= df.close) & (df.close< df.l4)
        df.loc[idx, cols]=[0.5, -1, df.loc[idx].l5, df.loc[idx].l4, round((df.loc[idx].close-df.loc[idx].l5)*100/df.loc[idx].close,1), round((df.loc[idx].l4-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx = (df.l4 <= df.close) & (df.close< df.l3)
        df.loc[idx, cols]=[0.618, 1, df.loc[idx].l4, df.loc[idx].l3, round((df.loc[idx].close-df.loc[idx].l4)*100/df.loc[idx].close,1), round((df.loc[idx].l3-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx=(df.l3 <= df.close) & (df.close< df.l2)
        df.loc[idx, cols]=[0.764, 2, df.loc[idx].l3, df.loc[idx].l2, round((df.loc[idx].close-df.loc[idx].l3)*100/df.loc[idx].close,1), round((df.loc[idx].l2-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx=(df.l2 <= df.close) & (df.close< df.l1)
        df.loc[idx, cols]=[1, 3, df.loc[idx].l2, df.loc[idx].l1, round((df.loc[idx].close-df.loc[idx].l2)*100/df.loc[idx].close,1), round((df.loc[idx].l1-df.loc[idx].close)*100/df.loc[idx].close,1)]

        idx=df.l1 <= df.close
        df.loc[idx, cols]=[1, 4,df.loc[idx].l1,None, round((df.loc[idx].close-df.loc[idx].l1)*100/df.loc[idx].close,1), None]

        df['grid_perc_resis_spt_dist']=df['grid_perc_to_resistance']-df['grid_perc_to_support']
        cols=['code', 'mcap','volatility']+cols+['close',high_field, low_field,'eq_pos','cs_pos','grid_perc_resis_spt_dist',"l1","l2","l3","l4","l5","l6","l7" ,'description']

        if not all_columns:
            df = df[cols]
            df = finlib.Finlib().adjust_column(df=df, col_name_list=['code', 'name', 'close', 'eq_pos', 'cs_pos','grid_perc_resis_spt_dist',
                                                                     'grid_perc_to_support', 'grid_perc_to_resistance',
                                                                     'mcap', 'volatility', 'grid', 'grid_support',
                                                                     'grid_resistance', ])

        return(df)

    def grid_market_overview(self,market,high_field='52 Week High', low_field='52 Week Low',all_columns=True):

        df = self._get_grid_spec(market=market,high_field=high_field,low_field=low_field, period='1D',all_columns=all_columns)

        if market == 'AG':
            df = finlib.Finlib().add_stock_name_to_df(df)
        elif market == 'US':
            df = finlib.Finlib().add_stock_name_to_df_us_hk(df, market='US')
        elif market == 'HK':
            df = finlib.Finlib().add_stock_name_to_df_us_hk(df, market='HK')

        df_g_n4 = df[df.grid == -4].reset_index().drop('index', axis=1)
        logging.info(market + " grid -4 stocks len " + str(df_g_n4.__len__()))
        df_g_n3 = df[df.grid == -3].reset_index().drop('index', axis=1)
        logging.info(market + " grid -3 stocks len " + str(df_g_n3.__len__()))
        df_g_n2 = df[df.grid == -2].reset_index().drop('index', axis=1)
        logging.info(market + " grid -2 stocks len " + str(df_g_n2.__len__()))
        df_g_n1 = df[df.grid == -1].reset_index().drop('index', axis=1)
        logging.info(market + " grid -1 stocks len " + str(df_g_n1.__len__()))

        df_g_p1 = df[df.grid == 1].reset_index().drop('index', axis=1)
        logging.info(market + " grid  1 stocks len " + str(df_g_p1.__len__()))
        df_g_p2 = df[df.grid == 2].reset_index().drop('index', axis=1)
        logging.info(market + " grid  2 stocks len " + str(df_g_p2.__len__()))
        df_g_p3 = df[df.grid == 3].reset_index().drop('index', axis=1)
        logging.info(market + " grid  3 stocks len " + str(df_g_p3.__len__()))
        df_g_p4 = df[df.grid == 4].reset_index().drop('index', axis=1)
        logging.info(market + " grid  4 stocks len " + str(df_g_p4.__len__()))

        return(df,df_g_n4,df_g_n3,df_g_n2,df_g_n1,df_g_p1,df_g_p2,df_g_p3,df_g_p4)

    def graham_intrinsic_value(self):
        f = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_indicator.csv'
        df0 = pd.read_csv(f)

        csv = "/home/ryan/DATA/result/graham_intrinsic_value_all.csv"
        csv_sel = "/home/ryan/DATA/result/graham_intrinsic_value_selected.csv"

        # df_last_n_years = df0[df0['end_date'].isin([20201231, 20191231, 20181231, 20171231, 20161231, 20151231, 20141231, 20131231, 20121231, 20111231])]
        df_last_n_years = df0[df0['end_date'].isin([20201231, 20191231, 20181231])]

        # #test start
        # df_test = df_last_n_years[df_last_n_years['ts_code']=="300146.SZ"]
        # print(df_test[['ts_code','end_date', 'quick_ratio','basic_eps_yoy','eps']])
        # #test end

        # df_last_n_years = df_last_n_years.groupby(by='ts_code').mean().reset_index()
        df_last_n_years = df_last_n_years.groupby(by='ts_code').median().reset_index()

        # df = df0[df0['end_date']==20201231]
        df = df_last_n_years

        df = df.sort_values('quick_ratio', ascending=False)[
            ['ts_code', 'end_date', 'quick_ratio', 'basic_eps_yoy', 'eps']].reset_index().drop('index', axis=1)


        df['inner_value'] = round(df['eps'] * (2 * df['basic_eps_yoy'] + 8.5) * 4.4 / 3.78, 2)
        df = finlib.Finlib().ts_code_to_code(df=df)
        df = finlib.Finlib().add_stock_name_to_df(df=df)

        last_trading_day = finlib.Finlib().get_last_trading_day()

        # today_df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/pickle/daily_update_source/ag_daily_20210511.csv')
        today_df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/pickle/daily_update_source/ag_daily_'+last_trading_day+'.csv')
        today_df = today_df[['code', 'close']]
        df = pd.merge(df, today_df, on=['code'], how='inner')

        # percent drop from current close to reach the inner value, smaller better, negative means close < inner value
        df['diff_inner_market_value'] = round((df['close'] - df['inner_value']) * 100 / df['close'], 2)
        df = df.sort_values('diff_inner_market_value', ascending=True).reset_index().drop('index', axis=1)

        df = finlib.Finlib().add_amount_mktcap(df=df)
        df = finlib.Finlib().add_tr_pe(df=df, df_daily=finlib.Finlib().get_last_n_days_daily_basic(ndays=1,
                                                                                                   dayE=finlib.Finlib().get_last_trading_day()),
                                       df_ts_all=finlib.Finlib().add_ts_code_to_column(
                                           df=finlib.Finlib().load_fund_n_years()))
        df = finlib.Finlib().df_format_column(df=df, precision='%.1e')


        df = finlib.Finlib().df_format_column(df=df, precision='%.1e')
        print(finlib.Finlib().pprint(df.head(10)))

        df.to_csv(csv,encoding='UTF-8', index=False)
        logging.info("saved all to "+csv+" len"+str(df.__len__()))

        #selected
        df_sel = df[df['eps'] > 0]  # 基本每股收益
        df_sel = df_sel[df_sel['basic_eps_yoy'] > 0]  # 基本每股收益同比增长率(%)

        df_sel = finlib.Finlib().remove_garbage(df=df_sel)
        df_sel.to_csv(csv_sel,encoding='UTF-8', index=False)
        logging.info("selected saved to "+csv_sel+" len"+str(df_sel.__len__()))

        return(df,df_sel)

    def hong_san_bin(self):
        output_csv = '/home/ryan/DATA/result/hong_san_bin.csv'

        dir = '/home/ryan/DATA/pickle/daily_update_source'
        day0 = finlib.Finlib().get_last_trading_day()

        day_list = []

        for i in range(6):
            day_dt = datetime.strptime(day0, "%Y%m%d") - timedelta(days=i)
            day_s = datetime.strftime(day_dt, "%Y%m%d")
            if finlib.Finlib().is_a_trading_day_ag(day_s):
                day_list.append(day_s)
                if day_list.__len__() == 3:
                    logging.info("got enough days ")
                    break

        df_rst = pd.DataFrame()

        day_list.reverse()  # ['20210823', '20210824', '20210825']

        for d in day_list:
            f = dir + "/" + "ag_daily_" + d + ".csv"
            df_a_day = self._hong_san_bin_day_bar_analysis(f)

            if df_a_day.empty:
                logging.info("no match stocks on day " + d + " , thus the days in roll donot match , quit.")
                break

            if df_rst.empty:
                df_rst = df_a_day

            df_rst = pd.merge(df_rst[['code', 'date', 'close']], df_a_day[['code', 'date', 'close']], how='inner',
                              on='code', suffixes=('_x', ''))
            logging.info("after merging day " + d + " ,result csv len " + str(df_rst.__len__()))

        df_rst = df_rst[['code', 'date', 'close']]
        df_rst = finlib.Finlib().add_stock_name_to_df(df_rst)

        df_rst.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(finlib.Finlib().pprint(df_rst))
        logging.info("saved to " + output_csv + " , len " + str(df_rst.__len__()))
        return(df_rst)

    def _hong_san_bin_day_bar_analysis(self,f):
        if not os.path.exists(f):
            logging.error("file not exists. " + str(f))
            return (pd.DataFrame())

        # df = pd.read_csv(f)
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)
        df_in = df[df['open'] < df['close']]
        df_in = df_in[df_in['low'] < df_in['pre_close']]
        df_in = df_in[df_in['open'] < df_in['pre_close']]
        df_in = df_in[df_in['high'] > df_in['pre_close']]
        df_in = df_in[
            (df_in['high'] - df_in['close']) / df_in['close'] < 0.007]  # high to close less than 0.7%. short up shadow.
        # df_in = df_in[ (df_in['open'] - df_in['low'])/ df_in['open'] < 0.01]
        return (df_in)

    def get_price_let_mashort_equal_malong(self, ma_short, ma_middle, debug=False):
        csv_in = '/home/ryan/DATA/result/stocks_amount_365_days.csv'
        csv_out = '/home/ryan/DATA/result/price_let_mashort_equal_malong.csv'

        df = pd.read_csv(csv_in)
        df_rtn = pd.DataFrame()

        code_list = df['code'].unique().tolist()

        if debug:
            code_list = ['SH600519']

        i = 1
        for code in code_list:
            logging.info(str(i) + " of " + str(code_list.__len__()) + " " + code)
            i += 1

            df_sub = df[df['code'] == code].reset_index().drop('index', axis=1)

            p1 = ma_short / (ma_middle - ma_short)
            p2 = df_sub[-1 * (ma_middle - 1):].head(ma_middle - ma_short)
            p2 = p2['close'].sum()

            p3 = df_sub.tail(ma_short - 1)
            p3 = p3['close'].sum()

            d0 = round(p1 * p2 - p3, 2)
            d1 = df_sub['close'].iloc[-1]
            da1 = df_sub['date'].iloc[-1]

            delta = round(d0 - d1, 2)
            delta_perc = round(delta * 100 / d1, 1)

            if delta < 0:
                trend = "bull"  # P(ma_short) > P(ma_long)
                action = "sell"
            else:
                trend = "bear"
                action = "buy"

            df_rtn = df_rtn.append(pd.DataFrame().from_dict({'code': [code], 'date': [da1], 'close': [d1],
                                                             'trend': [trend], 'action':[action],
                                                             'ma_short': [ma_short],
                                                             'ma_middle': [ma_middle],
                                                             'p_make_ma_across': [d0],
                                                             'delta': [delta],
                                                             'delta_perc': [delta_perc],
                                                             }))

            logging.info(str(code) + ", day " + str(da1) + " close " + str(d1)
                         + " , price to get mashort" + str(ma_short)
                         + " equal malong" + str(ma_middle) + " " + str(d0)
                         + " delta " + str(delta)
                         + " delta_perc " + str(delta_perc)
                         + " trend " + str(trend)
                         )

        df_rtn = finlib.Finlib().add_stock_name_to_df(df_rtn)
        df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
        logging.info("file saved to " + csv_out + " ,len " + str(df_rtn.__len__()))
        return(df_rtn)

    #calculate and compare different sector's volume and price change percent.
    # startD and endD have to be trading day.
    def price_amount_increase(self, startD, endD):

        if startD == None and endD == None: #check latest 5 days
            this_year = datetime.today().strftime("%Y") #2020
            csv_f = "/home/ryan/DATA/pickle/trading_day_"+this_year+".csv"
            df_trading_day = pd.read_csv(csv_f, converters={'cal_date': str})
            df_trading_day = df_trading_day[df_trading_day['is_open'] == 1].reset_index().drop('index',axis=1)

            today_index = df_trading_day[df_trading_day['cal_date'] == finlib.Finlib().get_last_trading_day()].index.values[0]

            endD = df_trading_day.iloc[today_index-1].cal_date
            startD = df_trading_day.iloc[today_index-6].cal_date


        # df_rtn=pd.DataFrame(columns=['group_name','price_change','amount_change'])
        df_rtn = pd.DataFrame()
        r_idx = 0

        # prepare amount df
        df_amount = finlib.Finlib().get_last_n_days_stocks_amount(ndays=5, dayS=str(startD), dayE=str(endD), daily_update=None,
                                                       short_period=True, debug=False, force_run=False)
        df_close_start = df_amount[df_amount['date'] == int(startD)]
        df_close_end = df_amount[df_amount['date'] == int(endD)]
        df_amount = pd.merge(df_close_start[['code', 'date', 'close', 'amount']],
                             df_close_end[['code', 'date', 'close', 'amount']], on='code', how='inner',
                             suffixes=('_dayS', '_dayE'))
        df_amount['amount_increase'] = round(
            (df_amount['amount_dayE'] - df_amount['amount_dayS']) * 100.0 / df_amount['amount_dayS'], 2)

        # prepare close df
        # df_basic = finlib.Finlib().get_last_n_days_daily_basic(ndays=30,dayS=None,dayE=None,daily_update=None,debug=False, force_run=False)
        df_basic = finlib.Finlib().get_last_n_days_daily_basic(ndays=10, dayS=str(startD), dayE=str(endD), daily_update=None,
                                                    debug=False, force_run=False)
        df_close_start = df_basic[df_basic['trade_date'] == int(startD)]
        df_close_end = df_basic[df_basic['trade_date'] == int(endD)]
        df_close = pd.merge(df_close_start[['ts_code', 'close', 'trade_date']],
                            df_close_end[['ts_code', 'close', 'trade_date']], on='ts_code', how='inner',
                            suffixes=('_dayS', '_dayE'))
        df_close = finlib.Finlib().ts_code_to_code(df=df_close)
        df_close = finlib.Finlib().add_stock_name_to_df(df=df_close)

        if df_close.empty:
            logging.fatal("unexpected empty dataframe df_close, cannot contine")
            return

        # calculate HS300
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="HS300", df_code_column_only=pd.read_csv(
            "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000300.csv"), df_close=df_close,
                                                              df_amount=df_amount))
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="ZhongZhen100", df_code_column_only=pd.read_csv(
            "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000903.csv"), df_close=df_close,
                                                              df_amount=df_amount))
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="ZhongZhen500", df_code_column_only=pd.read_csv(
            "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000905.csv"), df_close=df_close,
                                                              df_amount=df_amount))
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="ShenZhenChenZhi",
                                                              df_code_column_only=pd.read_csv(
                                                                  "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399001.csv"),
                                                              df_close=df_close, df_amount=df_amount))
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="ShenZhen100", df_code_column_only=pd.read_csv(
            "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399330.csv"), df_close=df_close,
                                                              df_amount=df_amount))
        df_rtn = df_rtn.append(self._get_avg_chg_of_code_list(list_name="KeJiLongTou", df_code_column_only=pd.read_csv(
            "/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/CSI931087.csv"), df_close=df_close,
                                                              df_amount=df_amount))

        # calculate garbage stocks close/amount increase
        df_rtn_garb = pd.DataFrame()
        for csv in glob.glob("/home/ryan/DATA/result/garbage/latest_*.csv"):
            # logging.info("reading "+csv)
            df = pd.read_csv(csv)
            df_rtn_garb = df_rtn_garb.append(
                self._get_avg_chg_of_code_list(list_name=csv.split(sep='/')[-1], df_code_column_only=df[['code']],
                                          df_close=df_close, df_amount=df_amount))

        logging.info("\n===== INDEX Increase ======")
        logging.info(finlib.Finlib().pprint(df_rtn.sort_values('price_change', ascending=False, inplace=False)))

        logging.info("\n===== Garbage Increase ======")
        logging.info(finlib.Finlib().pprint(df_rtn_garb.sort_values('price_change', ascending=False, inplace=False)))
        # exit(0)

    def _get_avg_chg_of_code_list(self, list_name, df_code_column_only, df_close, df_amount):
        if df_close.empty:
            logging.error("Unexpected empty input df df_close.")
            return ()

        df_2 = pd.merge(df_code_column_only[['code']].drop_duplicates(),
                        df_close[['code', 'name', 'close_dayS', 'trade_date_dayS', 'close_dayE', 'trade_date_dayE']],
                        on='code', how='inner')
        df_2 = pd.merge(df_2, df_amount[['code', 'amount_increase']], on='code', how='inner')
        df_2['close_delta'] = round((df_2['close_dayE'] - df_2['close_dayS']) * 100.0 / df_2['close_dayS'], 2)
        chg_mean_perc_close = round(df_2['close_delta'].mean(), 2)
        chg_mean_perc_amt = round(df_2['amount_increase'].mean(), 2)

        print(str(df_close.trade_date_dayS.iloc[0]) + "->" + str(df_close.trade_date_dayE.iloc[0]) + " len " + str(
            df_2.__len__()) + " " + list_name + ",  change average close " + str(
            chg_mean_perc_close) + "%,  change average amount " + str(chg_mean_perc_amt) + "%")

        return (pd.DataFrame.from_dict({'date_s': [df_close['trade_date_dayS'].iloc[0]],
                                        'date_e': [df_close['trade_date_dayE'].iloc[0]],

                                        'group_name': [list_name],
                                        'price_change': [chg_mean_perc_close],
                                        'amount_change': [chg_mean_perc_amt],
                                        'len': [df_2.__len__()],
                                        }))



    def count_jin_cha_si_cha(self, df, check_days=220, code='',name='',ma_short=4,ma_middle=27):
        df = df.tail(check_days).reset_index().drop('index', axis=1)

        code = df.iloc[0]['code']
        start_date = df.iloc[0]['date']
        end_date = df.iloc[-1]['date']


        df = self.add_ma_ema(df=df, short=ma_short, middle=ma_middle, long=60)

        (df, df_si_cha, df_jin_cha) = self.slow_fast_across(df=df, fast_col_name='close_'+str(ma_short)+'_sma', slow_col_name='close_'+str(ma_middle)+'_sma')

        a_dict = self._cnt_jin_cha_si_cha_days(df_all=df, df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)
        jincha_days = a_dict['jincha_days']
        sicha_days = a_dict['sicha_days']

        logging.info('\n'+str(code)+" "+str(name)+ ' SI CHA DAYS:')
        logging.info(finlib.Finlib().pprint(df_si_cha))

        logging.info('\n' + str(code) + " " + str(name) + ' JIN CHA DAYS:')
        logging.info(finlib.Finlib().pprint(df_jin_cha))

        cnt_days = df.__len__()
        cnt_jincha = df_jin_cha.__len__()
        cnt_sicha = df_si_cha.__len__()

        df_profit_details = pd.DataFrame()
        profit_over_all = 0

        if df_jin_cha.__len__() > 0 and df_si_cha.__len__() > 0:
            df_profit_details = self._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)

            if df_profit_details.__len__() > 0:
                profit_over_all = df_profit_details.iloc[-1]['profit_overall']


        df_rtn = pd.DataFrame({
            'code': [code],
            'df_profit_details':[df_profit_details],
            'profit_over_all':profit_over_all,
            'day_cnt': [cnt_days],
            'daystart': [str(start_date)],
            'dayend': [str(end_date)],
            'jincha_cnt': [cnt_jincha],
            'sicha_cnt': [cnt_sicha],
            'jincha_days':[jincha_days],
            'sicha_days':[sicha_days],
            # 'jincha_perc': [round(cnt_jincha * 100 / cnt_days, 1)],
            # 'sicha_perc': [round(cnt_sicha * 100 / cnt_days, 1)],
            'sum_perc': [round((cnt_jincha + cnt_sicha) * 100 /cnt_days, 1)],
            'jincha_sicha_days_ratio' : [round(jincha_days/(sicha_days+1), 2)],
        })


        logging.info(str(code)+" "+name+", days " + str(cnt_days)
                     + ", jincha cnt: "   + str(cnt_jincha)
                     + "  sicha cnt: " + str(cnt_sicha)
                     + "  jincha days: " + str(jincha_days)
                     + "  sicha days: " + str(sicha_days)
                     + "  profit_over_all: " + str(profit_over_all)
                     )

        return(df_rtn)


    def _cnt_jin_cha_si_cha_days(self, df_all, df_jin_cha, df_si_cha):
        sicha_days = 0
        jincha_days = 0

        jidx = df_jin_cha.index.tolist()
        sidx = df_si_cha.index.tolist()


        if sidx.__len__()==0 and jidx.__len__() == 0:
            return({'jincha_days':jincha_days,'sicha_days':sicha_days})

        if sidx.__len__()==0 and jidx.__len__() > 0:
            jincha_days = df_all.index.to_list()[-1] - jidx[0]
            sicha_days = jidx[0] -  df_all.index.to_list()[0]
            return({'jincha_days':jincha_days,'sicha_days':sicha_days})


        if jidx.__len__()==0 and sidx.__len__() > 0:
            sicha_days = df_all.index.to_list()[-1] - sidx[0]
            jincha_days = sidx[0] -  df_all.index.to_list()[0]
            return({'jincha_days':jincha_days,'sicha_days':sicha_days})




        if jidx.__len__() > sidx.__len__():
            trim_days = sidx[-1] - jidx[0]
            sicha_days += jidx[-1] - sidx[-1] #days of the latest sicha period
            jincha_days += df_all.index.to_list()[-1] - jidx[-1] # current is jincha perido, days it has been lasted.
        elif sidx.__len__() > jidx.__len__():
            trim_days = jidx[-1] - sidx[0]
            jincha_days += sidx[-1] - jidx[-1] #days of the latest jincha period
            sicha_days += df_all.index.to_list()[-1] - sidx[-1] #current is sicha perido, days it has been lasted.
        elif sidx.__len__() > 0 and jidx.__len__() > 0 and (sidx.__len__() == jidx.__len__()):
            if sidx[0] < jidx[0] :
                trim_days = jidx[-1] - sidx[0]
                jincha_days += df_all.index.to_list()[-1] - jidx[-1]

            elif jidx[0] < sidx[0]:
                trim_days = sidx[-1] - jidx[0]
                sicha_days += df_all.index.to_list()[-1] - sidx[-1]

        if sidx[0] < jidx[0]:
            logging.info("start with sicha")
            sicha_days_trim = pd.Series( list(map(sub, jidx, sidx)) ).sum()
            sicha_days += sicha_days_trim
            jincha_days += trim_days - sicha_days_trim
            sicha_days += sidx[0]
        elif jidx[0] < sidx[0]:
            logging.info("start with jincha")
            jincha_days_trim = pd.Series(list(map(sub, sidx, jidx))).sum()
            jincha_days += jincha_days_trim #middle body of jincha
            sicha_days += trim_days - jincha_days_trim
            jincha_days += jidx[0] #head of jincha

        return({'jincha_days':jincha_days,'sicha_days':sicha_days})


    def _calc_jin_cha_si_cha_profit(self, df_jin_cha, df_si_cha):
        df_jin_cha['action']="B"
        df_si_cha['action']="S"

        profit_this = 0
        profit_overall = 0

        if df_jin_cha.__len__()>0:
            code = df_jin_cha['code'].iloc[0]
        elif df_si_cha.__len__()>0:
            code = df_si_cha['code'].iloc[0]
        else:
            logging.error("df_jin_cha and df_si_cha all empty, quit")
            exit()

        rtn_df = pd.DataFrame()

        df_tmp = df_jin_cha.append(df_si_cha).sort_values(by='date').reset_index().drop('index', axis=1)

        while True:
            if df_tmp.__len__() >= 2 and not (df_tmp.iloc[0]['action'] == 'B' and df_tmp.iloc[1]['action'] == 'S'):
                df_tmp = df_tmp.drop(index=df_tmp.head(1).index.values[0])
            else:
                break
        while True:
            if df_tmp.__len__() >= 2 and not (df_tmp.iloc[-1]['action'] == 'S' and df_tmp.iloc[-2]['action'] =='B'):
                df_tmp = df_tmp.drop(index=df_tmp.tail(1).index.values[0])
            else:
                break

        df_tmp = df_tmp.reset_index().drop('index', axis=1)

        #
        # for tmp_cnt in range(6): #found a record have two S in header.
        #     if df_tmp.__len__() > 0 and df_tmp.iloc[0]['action'] == 'S':
        #         df_tmp = df_tmp.drop(index=df_tmp.head(1).index.values[0])
        #     if df_tmp.__len__() > 0 and df_tmp.iloc[-1]['action'] == 'B':
        #         df_tmp = df_tmp.drop(index=df_tmp.tail(1).index.values[0])


        for index,row in df_tmp.iterrows():
            action = row['action']
            close = row['close']
            date = row['date']
            if action == 'B':
                b_date = date
                b_price = close
                # logging.info("Buy at "+row['date']+" "+str(close))
            if action == 'S':
                profit_this = round((close - b_price)*100/b_price,2)
                profit_overall = round(profit_overall+ profit_this,2)
                # logging.info("Sell at " + row['date'] + " " + str(close))
                # logging.info("profit% this "+str(profit_this)+" profit% overall "+str(profit_overall))

                tmp_df= pd.DataFrame(
                    {
                        'code':[code],
                        'buy_date':[b_date],
                        'buy_price':[b_price],
                        'sell_date':[date],
                        'sell_price':[close],
                        'profit_this':[profit_this],
                        'profit_overall':[profit_overall],
                     }
                )

                rtn_df = rtn_df.append(tmp_df)

        rtn_df = rtn_df.reset_index().drop('index', axis=1)
        return(rtn_df)

    # general function to return jincha sicha on TWO columns.
    def slow_fast_across(self, df,fast_col_name,slow_col_name):

        df['tmp_col_fast_minor_slow'] = df[fast_col_name] - df[slow_col_name]

        df['b1_tmp_col_fast_minor_slow'] = df['tmp_col_fast_minor_slow'].shift(1)

        df_si_cha = df[(df['b1_tmp_col_fast_minor_slow'] > 0) & (df['tmp_col_fast_minor_slow'] < 0)]
        df_jin_cha = df[(df['b1_tmp_col_fast_minor_slow'] < 0) & (df['tmp_col_fast_minor_slow'] > 0)]
        return(df, df_si_cha, df_jin_cha)


    # general function to return jincha sicha on SINGLE column.
    def single_column_across(self, df,col_name,threshod=0):
        df['b1_tmp_col'] = df[col_name].shift(1)

        df_si_cha = df[(df['b1_tmp_col'] >= threshod) & (df[col_name] < threshod)]
        df_jin_cha = df[(df['b1_tmp_col'] <= threshod) & (df[col_name] > threshod)]
        return(df_si_cha, df_jin_cha)



    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass


    def w_shape_exam(self, df):
        pass