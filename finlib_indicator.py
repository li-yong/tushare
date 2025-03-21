# coding: utf-8

import finlib
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

# import zigzag
# from pandas_datareader import get_data_yahoo

logging.getLogger('matplotlib.font_manager').disabled = True
import matplotlib.pyplot as plt

# plt.rcParams['font.family'] = ['WenQuanYi Micro Hei']
plt.rcParams['font.family'] = ['SimSun']
plt.rcParams['font.sans-serif'] = ['SimSun']

from matplotlib.font_manager import FontProperties
font_path = r"/usr/share/fonts/truetype/windows-font/SIMSUN.TTC"
# font = FontProperties(fname=font_path,size=30)
font = FontProperties(fname=font_path)


import numpy
import math

import copy

class Finlib_indicator:
    def add_rsi(self, df, short=5, middle=10, long=20):
        stock = stockstats.StockDataFrame.retype(df)

        df['rsi_short_' + str(short)] = stock['rsi_' + str(short)]
        df['rsi_middle_' + str(middle)] = stock['rsi_' + str(middle)]
        df['rsi_long_' + str(long)] = stock['rsi_' + str(long)]

        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        df = df.round({'rsi_short_' + str(short): 2, 'rsi_middle_' + str(middle): 2, 'rsi_long_' + str(long): 2})

        return (df)

    def add_kdj(self, df, period=9):
        df = stockstats.StockDataFrame.retype(df)
        df[['kdjk']]  # will add columns 'rsv_9', 'kdjk_9', 'kdjk', 'kdjd_9', 'kdjd', 'kdjj_9', 'kdjj'
        df[['kdjk_' + str(period)]]
        df[['kdjd_' + str(period)]]
        df[['kdjj_' + str(period)]]

        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        df = df.round({'kdjk': 2, 'kdjd': 2, 'kdjj': 2})

        return (df)

        return (df_kdj)

    def add_macd(self, df):
        '''
        MACD: (12-day EMA - 26-day EMA)
        Signal Line: 9-day EMA of MACD
        MACD Histogram: 2*(MACD - Signal Line)

        df = finlib_indicator.Finlib_indicator().add_ma_ema(df[['close']], short=12, middle=26, long=60)
        df['ema_12_minus_26'] = df['ema_short_12'] - df['ema_middle_26']  # named DIF in tradeview/eastmoney/MooMoo
        df['signal'] = df['ema_12_minus_26'].ewm(span=9, min_periods=0, adjust=False, ignore_na=False).mean() # named DEA in tradeview/eastmoney/MooMoo
        df['Histogram'] = 2 * (df['ema_12_minus_26'] - df['signal'])  # named MACD in tradeview/eastmoney/MooMoo
        '''
        df_macd = stockstats.StockDataFrame.retype(df).reset_index()
        df_macd[['macd', 'macds', 'macdh', 'date']]  # macds: # MACD signal line, macdh: # MACD histogram
        df_macd.rename(
            columns={
                "macd": "DIF_main",  # DIF_Main called DIF in tradeview/eastmoney/Moomoo
                "macds": "DEA_signal",  # DEA_signal called DEA in tradeview/eastmoney/Moomoo
                "macdh": "MACD_histogram",  # MACD_histogram called 'MACD' in tradeview/eastmoney/MooMoo
            },
            inplace=True)
        df_macd = df_macd.round({'DIF_main': 2, 'DEA_signal': 2, 'MACD_histogram': 2})
        return (df_macd)

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
    def upper_body_lower_shadow(self, df, ma_short, ma_middle, ma_long):
        ###### Upper_shadow, Body, Lower_shadow ####
        unit = [[''] * 2 + [0] * 3 + [False] * 6]
        df_a = pd.DataFrame(unit * df.__len__(), columns=[
            'reason',
            'action',
            'upper_shadow_len',
            'body_len',
            'lower_shadow_len',
            'guangtou',
            'guangjiao',
            'small_body',
            'cross_star',
            'long_upper_shadow',
            'long_lower_shadow',
        ])

        # df_a = df_a.assign('reason', '') #adding column 'reason' with empty string
        # df_a = df_a.assign('action', '')
        df = df.merge(df_a, left_index=True, right_index=True)

        # df.iloc[i, df.columns.get_loc('reason')] = ''
        # df.iloc[i, df.columns.get_loc('reason')] = ''

        threshold_small = 1.0 / 20
        threshold_large = 5
        for i in range(df.__len__()):
            upper_shadow_len = df.at[i, 'high'] - max(df.at[i, 'open'], df.at[i, 'close'])
            body_len = abs(df.at[i, 'close'] - df.at[i, 'open']) + 0.00001  #prevent 0
            lower_shadow_len = min(df.at[i, 'open'], df.at[i, 'close']) - df.at[i, 'low']

            df.iloc[i, df.columns.get_loc('upper_shadow_len')] = upper_shadow_len
            df.iloc[i, df.columns.get_loc('body_len')] = body_len
            df.iloc[i, df.columns.get_loc('lower_shadow_len')] = lower_shadow_len

            if body_len / (df.at[i, 'open'] + 0.001) < 0.001:
                df.iloc[i, df.columns.get_loc('small_body')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_SMALL_BODY + '; '

                if upper_shadow_len > body_len and lower_shadow_len > body_len:
                    df.iloc[i, df.columns.get_loc('cross_star')] = True
                    df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_CROSS_STAR + '; '

            if upper_shadow_len / body_len < threshold_small:
                df.iloc[i, df.columns.get_loc('guangtou')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_GUANG_TOU + '; '
            if lower_shadow_len / body_len < threshold_small:
                df.iloc[i, df.columns.get_loc('guangjiao')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_GUANG_JIAO + '; '

            if upper_shadow_len / body_len > threshold_large:
                df.iloc[i, df.columns.get_loc('long_upper_shadow')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_LONG_UPPER_SHADOW + '; '
            if lower_shadow_len / body_len > threshold_large:
                df.iloc[i, df.columns.get_loc('long_lower_shadow')] = True
                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_LONG_LOWER_SHADOW + '; '

        #yun xian

        df_a = pd.DataFrame([[False, False]] * df.__len__(), columns=['yunxian_buy', 'yunxian_sell'])
        df = df.merge(df_a, left_index=True, right_index=True)

        if df.__len__() < 30:
            logging.info("bar number too small (<30) to calculate yunxian " + str(df.__len__()))

        #check yunxian for the latest 5 bars
        for i in range(df.__len__() - 5, df.__len__()):
            df_tmp = df.iloc[:i]
            junxian_seri = self.sma_jincha_sicha_duotou_koutou(df_tmp, short=ma_short, middle=ma_middle, long=ma_long).iloc[-1]

            #yunxian_buy: down trend, down_bar large bar.
            if (junxian_seri['kongtou_pailie']):
                if df.iloc[i - 1]['open'] > df.iloc[i - 1]['close']:
                    if (not df.iloc[i - 1]['long_upper_shadow']):
                        if (not df.iloc[i - 1]['long_lower_shadow']):
                            if (not df.iloc[i - 1]['small_body']):
                                if df.iloc[i - 1]['tr'] > 1.0 * df.iloc[i - 2]['atr_short_' + str(ma_short)]:
                                    # increase_bar,
                                    if df.iloc[i]['open'] < df.iloc[i]['close']:
                                        if df.iloc[i]['low'] > df.iloc[i - 1]['low']:
                                            if df.iloc[i]['high'] < df.iloc[i - 1]['high']:
                                                df.iloc[i, df.columns.get_loc('yunxian_buy')] = True
                                                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_YUNXIAN_BUY + '; '

                                                logging.info("yunxian buy point")

            #yunxian_sell: up trend, up_bar large bar.
            if (junxian_seri['duotou_pailie']):
                if df.iloc[i - 1]['open'] < df.iloc[i - 1]['close']:
                    if (not df.iloc[i - 1]['long_upper_shadow']):
                        if (not df.iloc[i - 1]['long_lower_shadow']):
                            if (not df.iloc[i - 1]['small_body']):
                                if df.iloc[i - 1]['tr'] > 1.0 * df.iloc[i - 2]['atr_short_' + str(ma_short)]:
                                    if df.iloc[i]['open'] < df.iloc[i]['close']:  # decrease_bar,
                                        if df.iloc[i]['low'] > df.iloc[i - 1]['low']:
                                            if df.iloc[i]['high'] < df.iloc[i - 1]['high']:
                                                df.iloc[i, df.columns.get_loc('yunxian_sell')] = True
                                                df.iloc[i, df.columns.get_loc('reason')] += constant.BAR_YUNXIAN_SELL + '; '
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

        df['sma_short_' + str(short)] = round(stock['close_' + str(short) + '_sma'],2)
        df['sma_middle_' + str(middle)] = round(stock['close_' + str(middle) + '_sma'],2)
        df['sma_long_' + str(long)] = round(stock['close_' + str(long) + '_sma'],2)

        df['close_' + str(short) + '_sma'] = round(df['close_' + str(short) + '_sma'],2)
        df['close_' + str(middle) + '_sma'] = round(df['close_' + str(middle) + '_sma'],2)
        df['close_' + str(long) + '_sma'] = round(df['close_' + str(long) + '_sma'],2)

        df['p_ma_dikou_' + str(short)] = df['close'].shift(short - 1)
        df['p_ma_dikou_' + str(middle)] = df['close'].shift(middle - 1)
        df['p_ma_dikou_' + str(long)] = df['close'].shift(long - 1)

        df['ema_short_' + str(short)] = round(stock['close_' + str(short) + '_ema'],2)
        df['ema_middle_' + str(middle)] = round(stock['close_' + str(middle) + '_ema'],2)
        df['ema_long_' + str(long)] = round(stock['close_' + str(long) + '_ema'],2)

        # #standard deviation of (biao zhun fang cha) of close. 表示数据大致扩散到多远
        # df['std_close_short_' + str(short)] = df['close'].rolling(window=short).std()
        # df['std_close_middle_' + str(middle)] = df['close'].rolling(window=middle).std()
        # df['std_close_long_' + str(long)] = df['close'].rolling(window=long).std()

        # #standard deviationof (biao zhun fang cha) of sma_short 表示数据大致扩散到多远
        # df['std_sma_short_' + str(short)] = df['sma_short_' + str(short)].rolling(window=short).std()
        # df['std_sma_middle_' + str(middle)] = df['sma_middle_' + str(middle)].rolling(window=middle).std()
        # df['std_sma_long_' + str(long)] = df['sma_long_' + str(long)].rolling(window=long).std()

        _df_tmp = df['sma_short_' + str(short)].rolling(window=10)  #evaluate last two weeks.
        df['two_week_fluctuation_sma_short_' + str(short)] = round((_df_tmp.max() - _df_tmp.min()) / _df_tmp.mean() * 100.0, 1)

        _df_tmp = df['sma_middle_' + str(middle)].rolling(window=10)  #evaluate last two weeks.
        df['two_week_fluctuation_sma_middle_' + str(middle)] = round((_df_tmp.max() - _df_tmp.min()) / _df_tmp.mean() * 100.0, 1)

        _df_tmp = df['sma_long_' + str(long)].rolling(window=10)  #evaluate last two weeks.
        df['two_week_fluctuation_sma_long_' + str(long)] = round((_df_tmp.max() - _df_tmp.min()) / _df_tmp.mean() * 100.0, 1)

        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        return (df)

    def add_tr_atr(self, df, short=5, middle=10, long=20):
        stock = stockstats.StockDataFrame.retype(df)

        df['tr'] = stock['tr']

        df['atr_short_' + str(short)] = stock['atr_' + str(short)]
        df['atr_middle_' + str(middle)] = stock['atr_' + str(middle)]
        df['atr_long_' + str(long)] = stock['atr_' + str(long)]

        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column

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
            rtn_dict['reason'] += constant.MA_JIN_CHA_MINOR + '; '
            rtn_dict['jincha_minor'] = True
            rtn_dict['jincha_minor_strength'] = round(2 * ((ma_short - ma_middle) / (ma_short + ma_middle) + (ma_middle_p1 - ma_short_p1) / (ma_middle_p1 + ma_short_p1)), 2)
        elif ma_short < ma_middle and ma_short_p1 > ma_middle_p1:
            logging.info("short down across middle, si cha minor")
            rtn_dict['sicha_minor'] = True
            rtn_dict['reason'] += constant.MA_SI_CHA_MINOR + '; '

            rtn_dict['sicha_minor_strength'] = round(2 * ((ma_middle - ma_short) / (ma_short + ma_middle) + (ma_short_p1 - ma_middle_p1) / (ma_middle_p1 + ma_short_p1)), 2)

        if ma_middle > ma_long and ma_middle_p1 < ma_long_p1:
            logging.info("middle up across long, jin cha major")
            rtn_dict['jincha_major'] = True
            rtn_dict['reason'] += constant.MA_JIN_CHA_MAJOR + '; '

            rtn_dict['jincha_major_strength'] = round(2 * ((ma_middle - ma_long) / (ma_long + ma_middle) + (ma_long_p1 - ma_middle_p1) / (ma_middle_p1 + ma_long_p1)), 2)

        elif ma_middle < ma_long and ma_middle_p1 > ma_long_p1:
            logging.info("middle down across long, si cha major")
            rtn_dict['sicha_major'] = True
            rtn_dict['reason'] += constant.MA_SI_CHA_MAJOR + '; '

            rtn_dict['sicha_major_strength'] = round(2 * ((ma_long - ma_middle) / (ma_long + ma_middle) + (ma_middle_p1 - ma_long_p1) / (ma_middle_p1 + ma_long_p1)), 2)

        if ma_short > ma_middle * 1.05:
            trend_short = 'up'
            rtn_dict['trend_short'] = 'up'
            rtn_dict['reason'] += constant.SHORT_TREND_UP + '; '

            rtn_dict['trend_short_strength'] = round(ma_short / ma_middle, 2)
        elif ma_short * 1.05 < ma_middle:
            trend_short = 'down'
            rtn_dict['trend_short'] = 'down'
            rtn_dict['reason'] += constant.SHORT_TREND_DOWN + '; '
            rtn_dict['trend_short_strength'] = round(ma_middle / ma_short, 2)

        if ma_middle > ma_long * 1.05:
            trend_middle = 'up'
            rtn_dict['trend_middle'] = 'up'
            rtn_dict['reason'] += constant.MIDDLE_TREND_UP + '; '
            rtn_dict['trend_middle_strength'] = round(ma_middle / ma_long, 2)
        elif ma_middle * 1.05 < ma_long:
            trend_middle = 'down'
            rtn_dict['trend_middle'] = 'down'
            rtn_dict['reason'] += constant.MIDDLE_TREND_DOWN + '; '
            rtn_dict['trend_middle_strength'] = round(ma_long / ma_middle, 2)

        if (ma_short > ma_middle > ma_long):
            rtn_dict['duotou_pailie'] = True
            rtn_dict['reason'] += constant.MA_DUO_TOU_PAI_LIE + '; '

            rtn_dict['trend_long'] = 'up'
            rtn_dict['reason'] += constant.LONG_TREND_UP + '; '

            logging.info("duo tou pai lie")
            if df.iloc[-1]['low'] > ma_short:
                logging.info("verify strong up trend")
                rtn_dict['very_strong_up_trend'] = True
                rtn_dict['reason'] += constant.VERY_STONG_UP_TREND + '; '

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
                    logging.info("latest kong tou pailie is " + str(i) + " days before at " + str(df.iloc[-i]['date']))
                    n_last_ma_dtpl_days = i
                    rtn_dict['last_kongtou_pailie_n_days_before'] = i
                    rtn_dict['last_kongtou_pailie_date'] = df.iloc[-i]['date']
                    break

            rtn_dict['reason'] += constant.MA_DUO_TOU_PAI_LIE_N_days + "_" + str(rtn_dict['duotou_pailie_last_bars']) + '; '
            rtn_dict['reason'] += constant.MA_LAST_KONG_TOU_PAI_LIE_N_days + "_" + str(rtn_dict['last_kongtou_pailie_n_days_before']) + '; '

        if (ma_short < ma_middle < ma_long):  #more interesting enter when price is up break
            rtn_dict['kongtou_pailie'] = True
            rtn_dict['reason'] += constant.MA_KONG_TOU_PAI_LIE + '; '

            rtn_dict['trend_long'] = 'down'
            rtn_dict['reason'] += constant.LONG_TREND_DOWN + '; '

            logging.info("kong tou pai lie")
            if df.iloc[-1]['high'] < ma_short:
                logging.info("verify strong down trend")
                rtn_dict['very_strong_down_trend'] = True
                rtn_dict['reason'] += constant.VERY_STONG_DOWN_TREND + '; '

            logging.info("check back last 30 bars")
            for i in range(30):
                if (df_sma_short.iloc[-i] < df_sma_middle.iloc[-i] < df_sma_long.iloc[-i]):
                    logging.info("kong tou lasts " + str(i) + "days")
                    rtn_dict['kongtou_pailie_last_bars'] = i
                    rtn_dict['reason'] += constant.MA_KONG_TOU_PAI_LIE_N_days + "_" + str(i) + '; '
                    continue

                if (df_sma_short.iloc[-i] > df_sma_middle.iloc[-i] > df_sma_long.iloc[-i]):
                    logging.info("latest duo tou pailie is " + str(i) + " days before at " + str(df.iloc[-i]['date']))
                    rtn_dict['last_duotou_pailie_n_days_before'] = i
                    rtn_dict['reason'] += constant.MA_LAST_DUO_TOU_PAI_LIE_N_days + "_" + str(i) + '; '
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
        # ser_price = df['close'].append(df['open']).append(df['high']).append(df['low'])
        ser_price = pd.concat([df['close'],df['open'],df['high'],df['low']])
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

    def get_indicator_critirial(self, query, period='D', fastMa=21, slowMa=55, market='ag', selected=False):

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

        if query in [
                constant.CLOSE_UNDER_SMA60,
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
            source_csv = dir + "/macd_selection_" + period + ".csv"
        elif query in [
                constant.CLOSE_ABOVE_MA5_N_DAYS,
                constant.CLOSE_NEAR_MA5_N_DAYS,
                constant.MA21_NEAR_MA55_N_DAYS,
                constant.SMA_CROSS_OVER,
        ]:
            source_csv = dir + "/ma_cross_over_selection_" + str(fastMa) + "_" + str(slowMa) + ".csv"
        elif query in [
                constant.BAR_SMALL_BODY,
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
                constant.BUY_MA_DISTANCE,
                constant.SELL_MA_DISTANCE,
        ]:
            source_csv = dir + '/' + market + '_junxian_barstyle.csv'
        elif query in [
                constant.BUY_MA_DISTANCE_WEEKLY,
                constant.SELL_MA_DISTANCE_WEEKLY,
        ]:
            source_csv = dir + '/' + market + '_junxian_barstyle_w.csv'
        elif query in [
                constant.BUY_MA_DISTANCE_MONTHLY,
                constant.SELL_MA_DISTANCE_MONTHLY,
        ]:
            source_csv = dir + '/' + market + '_junxian_barstyle_m.csv'

        elif query in [
                constant.HS300_INDEX_BUY_CANDIDATE,
        ]:
            source_csv = dir + '/hs300_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_ADDED

        elif query in [
                constant.HS300_INDEX_SELL_CANDIDATE,
        ]:
            source_csv = dir + '/hs300_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_REMOVED

        elif query in [
                constant.SZ100_INDEX_BUY_CANDIDATE,
        ]:
            source_csv = dir + '/sz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_ADDED

        elif query in [
                constant.SZ100_INDEX_SELL_CANDIDATE,
        ]:
            source_csv = dir + '/sz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_REMOVED

        elif query in [
                constant.ZZ100_INDEX_BUY_CANDIDATE,
        ]:
            source_csv = dir + '/zz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_ADDED

        elif query in [
                constant.ZZ100_INDEX_SELL_CANDIDATE,
        ]:
            source_csv = dir + '/zz100_candidate_list.csv'
            column_name = 'predict'
            top_n = 15
            query = constant.TO_BE_REMOVED

        elif query in [
                constant.SZCZ_INDEX_BUY_CANDIDATE,
        ]:
            source_csv = dir + '/szcz_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_ADDED

        elif query in [
                constant.SZCZ_INDEX_SELL_CANDIDATE,
        ]:
            source_csv = dir + '/szcz_candidate_list.csv'
            column_name = 'predict'
            top_n = 32
            query = constant.TO_BE_REMOVED

        elif query in [
                constant.MA5_UP_KOUDI_DISTANCE_GT_5,
                constant.MA21_UP_KOUDI_DISTANCE_GT_5,
                constant.MA55_UP_KOUDI_DISTANCE_GT_5,
                constant.MA5_UP_KOUDI_DISTANCE_LT_1,
                constant.MA21_UP_KOUDI_DISTANCE_LT_1,
                constant.MA55_UP_KOUDI_DISTANCE_LT_1,
                constant.TWO_WEEK_FLUC_SMA_5_LT_3,
                constant.TWO_WEEK_FLUC_SMA_21_LT_3,
                constant.TWO_WEEK_FLUC_SMA_55_LT_3,
        ]:
            source_csv = dir + '/latest_ma_koudi.csv'
            column_name = 'reason'

        elif query in [constant.PV2_VOLUME_RATIO_BOTTOM_10P]:
            return (pd.read_csv(dir + '/pv_2/latest/volume_ratio_bottom_10p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_VOLUME_RATIO_TOP_20P]:
            return (pd.read_csv(dir + '/pv_2/latest/volume_ratio_top_20p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANGTING_VOLUME_RATIO_LT_1]:
            return (pd.read_csv(dir + '/pv_2/latest/zhangting_volume_ration_lt_1.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANGTING_VOLUME_RATIO_LT_1]:
            return (pd.read_csv(dir + '/pv_2/latest/zhangting_volume_ration_lt_1.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_POCKET_PIVOT]:
            return (pd.read_csv(dir + '/pv_2/latest/pocket_pivot.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_DIE_TING]:
            return (pd.read_csv(dir + '/pv_2/latest/die_ting.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_ZHANG_TING]:
            return (pd.read_csv(dir + '/pv_2/latest/zhang_ting.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_PE_TOP_30P]:
            return (pd.read_csv(dir + '/pv_2/latest/pe_top_30p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_PE_BOTTOM_30P]:
            return (pd.read_csv(dir + '/pv_2/latest/pe_bottom_30p.csv', converters={'date': str}, encoding="utf-8"))

        elif query in [constant.PV2_STABLE_PRICE_VOLUME]:
            return (pd.read_csv(dir + '/pv_2/latest/stable_price_volume.csv', converters={'date': str}, encoding="utf-8"))

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

            source_csv = dir + '/ag_curve_shape.csv'
            column_name = 'reason'

        else:
            logging.error("Unknow source csv that matching query " + query)
            exit(0)

        df = pd.read_csv(source_csv, encoding="utf-8")

        df[column_name] = df[column_name].fillna('')
        df_match = df[df[column_name].str.contains(query)].reset_index()

        if top_n > 0:
            df_match = df_match.head(top_n)

        if 'index' in df_match.columns:
            df_match = df_match.drop('index', axis=1)

        perc = round(df_match.__len__() * 100 / df.__len__(), 2)
        logging.info("Period: " + period + ", Query: " + query + ", " + str(df_match.__len__()) + " of " + str(df.__len__()) + ", perc " + str(perc))
        finlib.Finlib().pprint(df_match.head(2))

        col = [
            'code',
            'name',
            'date',
            'close',
            'action',
            'strength',
            'reason',
            'operation',
            "total_mv_perc",
            "amount_perc",
            "my_index_weight",
            "weight",
            "mkt_cap",
            "predict",
        ]
        df_match = finlib.Finlib().keep_column(df_match, col)

        return (df_match)

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
        return (rtn_list)

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    # zscore = (value - mean)/std
    #The basic z score formula for a sample is:
    # z = (x – μ) / σ
    # For example, let’s say you have a test score of 190. The test has a mean (μ) of 150 and a standard deviation (σ) of 25. Assuming a normal distribution, your z score would be:
    # z = (x – μ) / σ
    # = (190 – 150) / 25 = 1.6.

    def get_outier(self, df, on_column, zscore_threshold=3):
        rtn_dict = {}

        df = df[df[on_column].notna()]
        df['zscore_' + on_column] = stats.zscore(df[on_column])

        o_all = df[abs(df['zscore_' + on_column]) > zscore_threshold].reset_index().drop('index', axis=1)
        o_min = df[df['zscore_' + on_column] < -1 * zscore_threshold].reset_index().drop('index', axis=1)
        o_max = df[df['zscore_' + on_column] > zscore_threshold].reset_index().drop('index', axis=1)

        return (o_all, o_min, o_max)

    #input: df [open,high, low, close]
    #output:
    def _get_key_support_price_from_pv(self, df, period):
        df['increase'] = df['close'].pct_change()
        df['inday_fluctuation'] = round((df['high'] - df['low']) / df['low'], 2)
        df['inday_increase'] = round((df['close'] - df['open']) / df['open'], 2)
        #
        # df_t = df[['code','date','increase', 'volume','inday_fluctuation', 'inday_increase']]
        # print(df_t.corr())

        (df_outier_increase, df_low_outier_increase, df_high_outier_increase) = self.get_outier(df=df, on_column='increase', zscore_threshold=1)
        (df_outier_inday_increase, df_low_outier_inday_increase, df_high_outier_inday_increase) = self.get_outier(df=df, on_column='inday_increase', zscore_threshold=1)
        (df_outier_inday_fluctuation, _df_low_outier_inday_fluctuation, df_high_outier_inday_fluctuation) = self.get_outier(df=df, on_column='inday_fluctuation', zscore_threshold=1)

        (df_outier_volume, _df_low_outier_volume, df_high_outier_volume) = self.get_outier(df=df, on_column='volume', zscore_threshold=1)

        max_increase_dict = {}
        if df_high_outier_increase.__len__() > 0:
            _row = df_high_outier_increase.sort_values(by='zscore_increase', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_increase_dict = {
                "date": _row.date,
                "open": _row.open,
                "high": _row.high,
                "low": _row.low,
                "close": _row.close,
            }

        max_inday_fluctuation_dict = {}
        if df_high_outier_inday_fluctuation.__len__() > 0:
            _row = df_high_outier_inday_fluctuation.sort_values(by='zscore_inday_fluctuation', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_inday_fluctuation_dict = {
                "date": _row.date,
                "open": _row.open,
                "high": _row.high,
                "low": _row.low,
                "close": _row.close,
            }

        max_volume_dict = {}
        if df_high_outier_volume.__len__() > 0:
            _row = df_high_outier_volume.sort_values(by='zscore_volume', ascending=False).reset_index().drop('index', axis=1).iloc[0]

            max_volume_dict = {
                "date": _row.date,
                "open": _row.open,
                "high": _row.high,
                "low": _row.low,
                "close": _row.close,
            }

        return ({
            "period": period,
            "period_cnt": df.__len__(),
            "max_increase": max_increase_dict,
            "max_inday_fluctuation": max_inday_fluctuation_dict,
            "max_volume": max_volume_dict,
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

        return ({
            'daily_support': daily_support_dict,
            'weekly_support': weekly_support_dict,
            'monthly_support': monthly_support_dict,
        })

    def print_support_price_by_price_volume(self, data_csv):

        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=data_csv)
        last_price = df.iloc[-1].close
        last_date = df.iloc[-1].date
        code = df.iloc[-1].code

        a_dict = self.get_support_price_by_price_volume(df_daily_ohlc_volume=df, verify_last_n_days=250)

        p_list = []
        for k1 in a_dict.keys():
            for k2 in a_dict[k1].keys():
                if type(a_dict[k1][k2]) is dict and a_dict[k1][k2].__len__() > 0:
                    # print(a_dict[k1][k2])
                    p_list.append(a_dict[k1][k2]['open'])
                    p_list.append(a_dict[k1][k2]['high'])
                    p_list.append(a_dict[k1][k2]['low'])
                    p_list.append(a_dict[k1][k2]['close'])

        support = pd.Series(p_list).sort_values().reset_index().drop('index', axis=1).T
        delta_perc = round((support - last_price) * 100 / last_price, 2)
        s = pd.concat([support,delta_perc]).reset_index().drop('index', axis=1)

        spt2 = support.T
        last_price_rank = spt2[spt2[0] < last_price].__len__()

        logging.info("\n\nkey price list and perctage distance, code " + str(code) + ", date " + last_date + ", close " + str(round(last_price, 2)) + ", rank " + str(last_price_rank) + "/" + str(spt2.__len__()))

        # print s every 10 columns
        col_p = 0
        for i in range(s.columns.__len__() // 10):
            # print(list(range(col_p,col_p+10)))
            logging.info(finlib.Finlib().pprint(df=s[list(range(col_p, col_p + 10))]))
            col_p = col_p + 10

        # print(list(range(col_p, s.columns.__len__())))
        logging.info(finlib.Finlib().pprint(df=s[list(range(col_p, s.columns.__len__()))]))

        return (s)

    def my_ma_koudi(self, df):
        code = df.iloc[0].code
        period = 5  # using MA5
        look_back_records = 3  # check last three records. eg 3: Day_b4_MA, Day_b3_MA, Day_b2_MA.
        last_N = period + look_back_records + 1
        # last_N = 100 #ryan debug

        if df.__len__() < last_N:
            logging.info("No enough data in df, expected df len " + str(last_N))
            return

        name = ''
        if 'name' in df.columns:
            name = df.iloc[-1]['name']

        df = self.add_ma_ema_simple(df=df)
        # df = self.add_tr_atr(df=df)
        df = df.tail(last_N)
        # df_simple = df[['code', 'date', 'close', 'close_sma_5', 'tr', 'atr_short_5']]
        df_simple = df[['code', 'date', 'close', 'close_sma_5']]

        a1 = df_simple[['close']].shift(0).fillna(0) - df_simple[['close']].shift(1).fillna(0) + df_simple[['close']].shift(period + 1).fillna(0) - df_simple[['close']].shift(period).fillna(0)  # consider today close, suppose tomorror close is zero.
        a2 = df_simple[['close']].shift(period).fillna(0) - df_simple[['close']].shift(period - 1).fillna(0)  # assume tomorrow close is same as today.
        b = df_simple[['close_sma_5']].shift(1).fillna(0) - df_simple[['close_sma_5']].shift(2).fillna(0)
        df_simple['delta_MA1'] = a1['close'] / period + b['close_sma_5']
        df_simple['delta_MA2'] = a2['close'] / period + b['close_sma_5']
        df_simple['delta_MA3'] = df_simple['close_sma_5'] - df_simple['close_sma_5'].shift(1)
        df_simple['delta_MA_chg_perc'] = round(df_simple['delta_MA3'] * 100 / df_simple['close_sma_5'].shift(1), 2)

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
            logging.info("strength " + str(strength) + ", BUY " + code + " " + name + " before today market close. based on price " + str(df_simple.iloc[-1].close) + " delta_MAs: " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(Day_b2_delta_MA_chg_perc) + " " + str(today_predicated_delta_MA_chg_perc))
        elif Day_b4_delta_MA_chg_perc > 0 and Day_b3_delta_MA_chg_perc > 0 and Day_b2_delta_MA_chg_perc > 0 and today_predicated_delta_MA_chg_perc < 0:
            logging.info("strength " + str(strength) + ", SELL " + code + " " + name + " before today market close. based on price " + str(df_simple.iloc[-1].close) + " delta_MAs: " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(Day_b2_delta_MA_chg_perc) + " " + str(today_predicated_delta_MA_chg_perc))
        else:
            logging.info("strength " + str(strength) + " code " + code + " " + name + " No operation. based on price " + str(df_simple.iloc[-1].close) + " delta_MAs " + str(Day_b4_delta_MA_chg_perc) + " " + str(Day_b3_delta_MA_chg_perc) + " " + str(Day_b2_delta_MA_chg_perc) + " " + str(today_predicated_delta_MA_chg_perc))

        return ()

    def check_my_ma(self, selected=True, stock_global='AG_HOLD', allow_delay_min=30, force_fetch=False):
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

        if stock_global in ['HK_HOLD', 'HK']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='HK', allow_delay_min=allow_delay_min, force_fetch=force_fetch)
        elif stock_global in ['AG_HOLD', 'AG']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='AG', allow_delay_min=allow_delay_min, force_fetch=force_fetch)
        elif stock_global in ['US_HOLD', 'US']:
            in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='US', allow_delay_min=allow_delay_min, force_fetch=force_fetch)

        logging.info("loaded in_day_price_df")

        ###############
        for index, row in stock_list.iterrows():
            code = row['code']  # SH600519
            data_csv = csv_dir + '/' + str(code).upper() + '.csv'

            df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=data_csv, exit_if_not_exist=False)

            if type(df) is str:  # "FILE_NOT_EXIT"
                continue

            df = df[['code', 'date', 'open', 'high', 'low', 'close']]

            a_live_df = in_day_price_df[in_day_price_df['code'] == code]
            if a_live_df.__len__() == 0:
                logging.warning("not found current price of " + code)
                continue

            df_today = pd.DataFrame.from_dict({
                'code': [code],
                'date': [datetime.today().strftime('%Y%m%d')],
                'open': [a_live_df.open.values[0]],
                'high': [a_live_df.high.values[0]],
                'low': [a_live_df.low.values[0]],
                'close': [a_live_df.close.values[0]],
            })
            df = pd.concat([df,df_today]).reset_index().drop('index', axis=1)
            df['name'] = a_live_df.iloc[0]['name']  # add name column. AK returns name in df.

            rtn = self.my_ma_koudi(df=df)

    def tv_login(self, browser, target_uri='https://www.tradingview.com/'):

        cookie_f = os.path.expanduser("~") + '/DATA/pickle/tradingview.cookie'

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
            browser.get(target_uri + '#signin')

            # browser.find_element_by_class_name('tv-header__area tv-header__area--user').click()

            browser.find_element_by_class_name('tv-signin-dialog__toggle-email').click()

            usr_box = browser.find_element_by_name('username')
            pwd_box = browser.find_element_by_name('password')

            usr_box.send_keys('sunraise2005@gmail.com')
            pwd_box.send_keys('fav8@Apple!_tv')

            browser.find_element_by_class_name('tv-button__loader').click()

            time.sleep(10)

            WebDriverWait(browser, 10).until(EC.title_contains("TradingView"))

            with open(cookie_f, "wb") as f:
                pickle.dump(browser.get_cookies(),f)

            logging.info("tradingview login cookie saved to " + cookie_f)

        return (browser)

    def wglh_login(self, browser, target_uri='https://wglh.com/user/account/'):

        cookie_f = os.path.expanduser("~") + '/DATA/pickle/wglh.cookie'

        if finlib.Finlib().is_cached(cookie_f, day=2):
            browser.get('https://wglh.com/')

            logging.info('wglh_login, load cookies from ' + cookie_f)

            with open(cookie_f, "rb") as f:
                cookies = pickle.load(f)

            for c in cookies:
                browser.add_cookie(c)

            browser.get(target_uri)
            WebDriverWait(browser, 360).until(EC.title_contains("我的账号信息"))
        else:
            browser.get(target_uri)
            logging.info("Please manually login in 360 sec")
            WebDriverWait(browser, 360).until(EC.title_contains("我的账号信息"))

            with open(cookie_f, "wb") as f:
                pickle.dump(browser.get_cookies(),f)

            logging.info("wglh login cookie saved to " + cookie_f)

        return (browser)


    def jsl_login(self, browser, target_uri='https://www.jisilu.cn/account/login/'):

        cookie_f = os.path.expanduser("~") + '/DATA/pickle/jsl.cookie'

        if finlib.Finlib().is_cached(cookie_f, day=2):
            browser.get('https://www.jisilu.cn/')

            logging.info('jsl_login, load cookies from ' + cookie_f)

            with open(cookie_f, "rb") as f:
                cookies = pickle.load(f)

            for c in cookies:
                browser.add_cookie(c)

            # browser.get(target_uri)
            browser.get("https://www.jisilu.cn/notifications/")
            WebDriverWait(browser, 60).until(EC.title_contains("通知"))
        else:
            browser.get(target_uri)

            # usr_box = browser.find_element_by_id('aw-login-user-name')
            usr_box = browser.find_element_by_name('user_name')
            pwd_box = browser.find_element_by_name('password')


            usr_box.send_keys('13651887669')
            pwd_box.send_keys('fav8@Apple!_jsl')

            browser.find_element_by_id('agreement_chk').click()
            browser.find_element_by_id('login_submit').click()
            # browser.find_element_by_class_name('tv-button__loader').click()



            logging.info("Please manually login in 360 sec")
            WebDriverWait(browser, 60).until(EC.title_contains("集思录"))

            with open(cookie_f, "wb") as f:
                pickle.dump(browser.get_cookies(),f)

            logging.info("jsl login cookie saved to " + cookie_f)

        return (browser)






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

    def tv_wait_page_to_ready(self, browser, timeout):
        _bs = browser.execute_script("return document.readyState")
        t = 0

        while _bs != "complete":
            print("page readystate: " + _bs)
            time.sleep(1)
            t += 1

            if t > timeout:
                print("timeout, page not ready,readystate: " + _bs)
                break

        return ()

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
            return (browser)

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
        return (browser)

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
            return (browser)

        obj_m.click()
        self.tv_wait_page_to_ready(browser, timeout=30)

        # #scroll down entire window 200 from current position
        # browser.execute_script("window.scrollTo(0, window.scrollY + 200);")

        while browser.find_elements_by_css_selector('[data-name="screener-market-dialog"]').__len__()==0:
            logging.info("wait for market select dialog load")
            time.sleep(1)

        dia_obj = browser.find_element_by_css_selector('[data-name="screener-market-dialog"]')

        input_obj = dia_obj.find_elements_by_tag_name("input")[1]
        if market == 'US':
            input_obj.send_keys("USA")
            browser.find_element_by_css_selector('[data-market="america"]').click()
        elif market == 'CN':
            input_obj.send_keys("China")
            browser.find_element_by_css_selector('[data-market="china"]').click()
        elif market == 'HK':
            input_obj.send_keys("Hong Kong")
            browser.find_element_by_css_selector('[data-market="hongkong"]').click()

        apply_obj = browser.find_element_by_xpath('//button[normalize-space()="Apply"]')
        apply_obj.click()
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

        self.tv_wait_page_to_ready(browser, timeout=30)

        # obj_m =   # get element again. otherwise staled obj
        while browser.find_element_by_css_selector('[data-name="screener-markets"]').find_element_by_xpath('img').get_attribute('alt').upper() != market:
            logging.warning("market has not set to " + market)
            time.sleep(1)
            # obj_m = browser.find_element_by_xpath(xp_m) #refresh
        logging.info("market has set to " + market)
        return (browser)

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
            return (browser)

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
        return (browser)

    def tv_save_result_table(self, browser, market='CN', parse_ticker_only=False, max_row=20):
        columns = []
        delay_data_flag = True

        if market in ['SH', 'SZ']:
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
        if rows.__len__() > 0:
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

        return (df)

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
            logging.info("rmdir " + downloadPath)

        os.mkdir(downloadPath)
        logging.info("mkdir " + downloadPath)

        return (downloadPath)

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

        while (os.listdir(dir)[0].rfind(".crdownload") > 1):  #the index position of .crdownload, -1 if not include .crdownload
            logging.info("waiting download complete, file in .crdownload")
            time.sleep(5)

        fr_file = dir + "/" + os.listdir(dir)[0]
        to_file = to_dir + "/" + interval + "_" + os.listdir(dir)[0]
        time.sleep(10)  #wait 10 seconds to let the download file completed.

        shutil.move(fr_file, to_file)
        logging.info("downloaded to  " + to_file)

        if symbol_link_f:
            if os.path.exists(symbol_link_f):
                os.unlink(symbol_link_f)

            os.symlink(to_file, symbol_link_f)
            logging.info("symbol link created. " + symbol_link_f + " --> " + to_file)

        return (to_file)

    def tv_screener_start(self, browser, column_filed, interval, market, filter):
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

        return (browser)

    def _get_grid_spec(self, market='AG', high_field='52 Week High', low_field='52 Week Low', period='1D', all_columns=True):

        df = finlib.Finlib().load_tv_fund(market=market, period=period)

        code = df['code']
        p = df['close']
        high = df[high_field]
        low = df[low_field]
        atr_14d = df['atr_14']
        df['volatility'] = df['volatility'].apply(lambda _d: round(_d, 2))

        delta = high - low
        df['eq_pos'] = round((high - p) / delta, 3)  #current price to high, equity position percentage.
        df['cs_pos'] = round((p - low) / delta, 3)  #current price to low, cash position percentage.

        df['l1'] = round(high, 2)
        df['l2'] = round(low + delta * 0.764, 2)
        df['l3'] = round(low + delta * 0.618, 2)
        df['l4'] = round(low + delta * 0.5, 2)
        df['l5'] = round(low + delta * 0.382, 2)
        df['l6'] = round(low + delta * 0.236, 2)
        df['l7'] = round(low, 2)

        cols = ['grid_cash_perc', 'grid', 'grid_support', 'grid_resistance', 'grid_perc_to_support', 'grid_perc_to_resistance']

        idx = df.close < df.l7
        df.loc[idx, cols] = [0, -4, None, df.loc[idx].l7, None, round((df.loc[idx].l7 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l7 <= df.close) & (df.close < df.l6)
        df.loc[idx, cols] = [0.235, -3, df.loc[idx].l7, df.loc[idx].l6, round((df.loc[idx].close - df.loc[idx].l7) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l6 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l6 <= df.close) & (df.close < df.l5)
        df.loc[idx, cols] = [0.382, -2, df.loc[idx].l6, df.loc[idx].l5, round((df.loc[idx].close - df.loc[idx].l6) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l5 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l5 <= df.close) & (df.close < df.l4)
        df.loc[idx, cols] = [0.5, -1, df.loc[idx].l5, df.loc[idx].l4, round((df.loc[idx].close - df.loc[idx].l5) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l4 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l4 <= df.close) & (df.close < df.l3)
        df.loc[idx, cols] = [0.618, 1, df.loc[idx].l4, df.loc[idx].l3, round((df.loc[idx].close - df.loc[idx].l4) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l3 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l3 <= df.close) & (df.close < df.l2)
        df.loc[idx, cols] = [0.764, 2, df.loc[idx].l3, df.loc[idx].l2, round((df.loc[idx].close - df.loc[idx].l3) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l2 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = (df.l2 <= df.close) & (df.close < df.l1)
        df.loc[idx, cols] = [1, 3, df.loc[idx].l2, df.loc[idx].l1, round((df.loc[idx].close - df.loc[idx].l2) * 100 / df.loc[idx].close, 1), round((df.loc[idx].l1 - df.loc[idx].close) * 100 / df.loc[idx].close, 1)]

        idx = df.l1 <= df.close
        df.loc[idx, cols] = [1, 4, df.loc[idx].l1, None, round((df.loc[idx].close - df.loc[idx].l1) * 100 / df.loc[idx].close, 1), None]

        df['grid_perc_resis_spt_dist'] = df['grid_perc_to_resistance'] - df['grid_perc_to_support']
        cols = ['code', 'mcap', 'volatility'] + cols + ['close', high_field, low_field, 'eq_pos', 'cs_pos', 'grid_perc_resis_spt_dist', "l1", "l2", "l3", "l4", "l5", "l6", "l7", 'description']

        if not all_columns:
            df = df[cols]
            df = finlib.Finlib().adjust_column(df=df, col_name_list=[
                'code',
                'name',
                'close',
                'eq_pos',
                'cs_pos',
                'grid_perc_resis_spt_dist',
                'grid_perc_to_support',
                'grid_perc_to_resistance',
                'mcap',
                'volatility',
                'grid',
                'grid_support',
                'grid_resistance',
            ])

        return (df)

    def grid_market_overview(self, market, high_field='52 Week High', low_field='52 Week Low', all_columns=True):

        df = self._get_grid_spec(market=market, high_field=high_field, low_field=low_field, period='1D', all_columns=all_columns)

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

        return (df, df_g_n4, df_g_n3, df_g_n2, df_g_n1, df_g_p1, df_g_p2, df_g_p3, df_g_p4)

    def graham_intrinsic_value(self):
        f = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_indicator.csv'
        df0 = pd.read_csv(f)

        csv = "/home/ryan/DATA/result/graham_intrinsic_value_all.csv"
        csv_sel = "/home/ryan/DATA/result/graham_intrinsic_value_selected.csv"

        # df_last_n_years = df0[df0['end_date'].isin([20201231, 20191231, 20181231, 20171231, 20161231, 20151231, 20141231, 20131231, 20121231, 20111231])]
        df_last_n_years = df0[df0['end_date'].isin([20211231, 20201231, 20191231])]

        # #test start
        # df_test = df_last_n_years[df_last_n_years['ts_code']=="300146.SZ"]
        # print(df_test[['ts_code','end_date', 'quick_ratio','basic_eps_yoy','eps']])
        # #test end

        # df_last_n_years = df_last_n_years.groupby(by='ts_code').mean().reset_index()
        df_last_n_years = df_last_n_years.groupby(by='ts_code').median().reset_index()

        # df = df0[df0['end_date']==20201231]
        df = df_last_n_years

        df = df.sort_values('quick_ratio', ascending=False)[['ts_code', 'end_date', 'quick_ratio', 'basic_eps_yoy', 'eps']].reset_index().drop('index', axis=1)

        df['inner_value'] = round(df['eps'] * (2 * df['basic_eps_yoy'] + 8.5) * 4.4 / 3.78, 2)
        df = finlib.Finlib().ts_code_to_code(df=df)
        df = finlib.Finlib().add_stock_name_to_df(df=df)

        last_trading_day = finlib.Finlib().get_last_trading_day()

        # today_df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/pickle/daily_update_source/ag_daily_20210511.csv')
        today_df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/pickle/daily_update_source/ag_daily_' + last_trading_day + '.csv')
        today_df = today_df[['code', 'close']]
        df = pd.merge(df, today_df, on=['code'], how='inner')

        # percent drop from current close to reach the inner value, smaller better, negative means close < inner value
        df['diff_inner_market_value'] = round((df['close'] - df['inner_value']) * 100 / df['close'], 2)
        df = df.sort_values('diff_inner_market_value', ascending=True).reset_index().drop('index', axis=1)

        df = finlib.Finlib().add_amount_mktcap(df=df)
        df = finlib.Finlib().add_tr_pe(df=df, df_daily=finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day()), df_ts_all=finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years()))
        df = finlib.Finlib().df_format_column(df=df, precision='%.1e')

        df = finlib.Finlib().df_format_column(df=df, precision='%.1e')
        print(finlib.Finlib().pprint(df.head(10)))

        df.to_csv(csv, encoding='UTF-8', index=False)
        logging.info("saved all to " + csv + " len" + str(df.__len__()))

        #selected
        df_sel = df[df['eps'] > 0]  # 基本每股收益
        df_sel = df_sel[df_sel['basic_eps_yoy'] > 0]  # 基本每股收益同比增长率(%)

        df_sel = finlib.Finlib().remove_garbage(df=df_sel)
        df_sel.to_csv(csv_sel, encoding='UTF-8', index=False)
        logging.info("selected saved to " + csv_sel + " len" + str(df_sel.__len__()))

        return (df, df_sel)

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

            df_rst = pd.merge(df_rst[['code', 'date', 'close']], df_a_day[['code', 'date', 'close']], how='inner', on='code', suffixes=('_x', ''))
            logging.info("after merging day " + d + " ,result csv len " + str(df_rst.__len__()))

        df_rst = df_rst[['code', 'date', 'close']]
        df_rst = finlib.Finlib().add_stock_name_to_df(df_rst)

        df_rst.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(finlib.Finlib().pprint(df_rst))
        logging.info("saved to " + output_csv + " , len " + str(df_rst.__len__()))
        return (df_rst)

    def _hong_san_bin_day_bar_analysis(self, f):
        if not os.path.exists(f):
            logging.error("file not exists. " + str(f))
            return (pd.DataFrame())

        # df = pd.read_csv(f)
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)
        df_in = df[df['open'] < df['close']]
        df_in = df_in[df_in['low'] < df_in['pre_close']]
        df_in = df_in[df_in['open'] < df_in['pre_close']]
        df_in = df_in[df_in['high'] > df_in['pre_close']]
        df_in = df_in[(df_in['high'] - df_in['close']) / df_in['close'] < 0.007]  # high to close less than 0.7%. short up shadow.
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

            df_rtn = pd.concat([df_rtn, pd.DataFrame().from_dict({
                'code': [code],
                'date': [da1],
                'close': [d1],
                'trend': [trend],
                'action': [action],
                'ma_short': [ma_short],
                'ma_middle': [ma_middle],
                'p_make_ma_across': [d0],
                'delta': [delta],
                'delta_perc': [delta_perc],
            })])

            logging.info(str(code) + ", day " + str(da1) + " close " + str(d1) + " , price to get mashort" + str(ma_short) + " equal malong" + str(ma_middle) + " " + str(d0) + " delta " + str(delta) + " delta_perc " + str(delta_perc) + " trend " + str(trend))

        df_rtn = finlib.Finlib().add_stock_name_to_df(df_rtn)
        df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
        logging.info("file saved to " + csv_out + " ,len " + str(df_rtn.__len__()))
        return (df_rtn)

    #calculate and compare different sector's volume and price change percent.
    # startD and endD have to be trading day.
    def price_amount_increase(self, startD, endD):

        if startD == None and endD == None:  #check latest 5 days
            this_year = datetime.today().strftime("%Y")  #2020
            csv_f = "/home/ryan/DATA/pickle/trading_day_" + this_year + ".csv"
            df_trading_day = pd.read_csv(csv_f, converters={'cal_date': str})
            df_trading_day = df_trading_day[df_trading_day['is_open'] == 1].reset_index().drop('index', axis=1)

            today_index = df_trading_day[df_trading_day['cal_date'] == finlib.Finlib().get_last_trading_day()].index.values[0]

            endD = df_trading_day.iloc[today_index - 1].cal_date
            startD = df_trading_day.iloc[today_index - 6].cal_date

        # df_rtn=pd.DataFrame(columns=['group_name','price_change','amount_change'])
        df_rtn = pd.DataFrame()
        r_idx = 0

        # prepare amount df
        df_amount = finlib.Finlib().get_last_n_days_stocks_amount(ndays=5, dayS=str(startD), dayE=str(endD), daily_update=None, short_period=True, debug=False, force_run=False)
        df_close_start = df_amount[df_amount['date'] == int(startD)]
        df_close_end = df_amount[df_amount['date'] == int(endD)]
        df_amount = pd.merge(df_close_start[['code', 'date', 'close', 'amount']], df_close_end[['code', 'date', 'close', 'amount']], on='code', how='inner', suffixes=('_dayS', '_dayE'))
        df_amount['amount_increase'] = round((df_amount['amount_dayE'] - df_amount['amount_dayS']) * 100.0 / df_amount['amount_dayS'], 2)

        # prepare close df
        # df_basic = finlib.Finlib().get_last_n_days_daily_basic(ndays=30,dayS=None,dayE=None,daily_update=None,debug=False, force_run=False)
        df_basic = finlib.Finlib().get_last_n_days_daily_basic(ndays=10, dayS=str(startD), dayE=str(endD), daily_update=None, debug=False, force_run=False)
        df_close_start = df_basic[df_basic['trade_date'] == int(startD)]
        df_close_end = df_basic[df_basic['trade_date'] == int(endD)]
        df_close = pd.merge(df_close_start[['ts_code', 'close', 'trade_date']], df_close_end[['ts_code', 'close', 'trade_date']], on='ts_code', how='inner', suffixes=('_dayS', '_dayE'))
        df_close = finlib.Finlib().ts_code_to_code(df=df_close)
        df_close = finlib.Finlib().add_stock_name_to_df(df=df_close)

        if df_close.empty:
            logging.fatal("unexpected empty dataframe df_close, cannot contine")
            return

        # calculate HS300
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="HS300", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000300.csv"), df_close=df_close, df_amount=df_amount)])
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="ZhongZhen100", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000903.csv"), df_close=df_close, df_amount=df_amount)])
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="ZhongZhen500", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000905.csv"), df_close=df_close, df_amount=df_amount)])
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="ShenZhenChenZhi", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399001.csv"), df_close=df_close, df_amount=df_amount)])
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="ShenZhen100", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399330.csv"), df_close=df_close, df_amount=df_amount)])
        df_rtn = pd.concat([df_rtn,self._get_avg_chg_of_code_list(list_name="KeJiLongTou", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/CSI931087.csv"), df_close=df_close, df_amount=df_amount)])

        # calculate garbage stocks close/amount increase
        df_rtn_garb = pd.DataFrame()
        for csv in glob.glob("/home/ryan/DATA/result/garbage/latest_*.csv"):
            # logging.info("reading "+csv)
            df = pd.read_csv(csv)
            df_rtn_garb = pd.concat([df_rtn_garb,self._get_avg_chg_of_code_list(list_name=csv.split(sep='/')[-1], df_code_column_only=df[['code']], df_close=df_close, df_amount=df_amount)])

        logging.info("\n===== INDEX Increase ======")
        logging.info(finlib.Finlib().pprint(df_rtn.sort_values('price_change', ascending=False, inplace=False)))

        logging.info("\n===== Garbage Increase ======")
        logging.info(finlib.Finlib().pprint(df_rtn_garb.sort_values('price_change', ascending=False, inplace=False)))
        # exit(0)

    def _get_avg_chg_of_code_list(self, list_name, df_code_column_only, df_close, df_amount):
        if df_close.empty:
            logging.error("Unexpected empty input df df_close.")
            return ()

        df_2 = pd.merge(df_code_column_only[['code']].drop_duplicates(), df_close[['code', 'name', 'close_dayS', 'trade_date_dayS', 'close_dayE', 'trade_date_dayE']], on='code', how='inner')
        df_2 = pd.merge(df_2, df_amount[['code', 'amount_increase']], on='code', how='inner')
        df_2['close_delta'] = round((df_2['close_dayE'] - df_2['close_dayS']) * 100.0 / df_2['close_dayS'], 2)
        chg_mean_perc_close = round(df_2['close_delta'].mean(), 2)
        chg_mean_perc_amt = round(df_2['amount_increase'].mean(), 2)

        print(str(df_close.trade_date_dayS.iloc[0]) + "->" + str(df_close.trade_date_dayE.iloc[0]) + " len " + str(df_2.__len__()) + " " + list_name + ",  change average close " + str(chg_mean_perc_close) + "%,  change average amount " + str(chg_mean_perc_amt) + "%")

        return (pd.DataFrame.from_dict({
            'date_s': [df_close['trade_date_dayS'].iloc[0]],
            'date_e': [df_close['trade_date_dayE'].iloc[0]],
            'group_name': [list_name],
            'price_change': [chg_mean_perc_close],
            'amount_change': [chg_mean_perc_amt],
            'len': [df_2.__len__()],
        }))

    def count_jin_cha_si_cha(self, df, check_days=220, code='', name='', ma_short=4, ma_middle=27):
        df = df.tail(check_days).reset_index().drop('index', axis=1)

        code = df.iloc[0]['code']
        start_date = df.iloc[0]['date']
        end_date = df.iloc[-1]['date']

        df = self.add_ma_ema(df=df, short=ma_short, middle=ma_middle, long=60)

        (df, df_si_cha, df_jin_cha) = self.slow_fast_across(df=df, fast_col_name='close_' + str(ma_short) + '_sma', slow_col_name='close_' + str(ma_middle) + '_sma')

        a_dict = self._cnt_jin_cha_si_cha_days(df_all=df, df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)
        jincha_days = a_dict['jincha_days']
        sicha_days = a_dict['sicha_days']

        logging.info('\n' + str(code) + " " + str(name) + ' SI CHA DAYS:')
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
            'df_profit_details': [df_profit_details],
            'profit_over_all': profit_over_all,
            'day_cnt': [cnt_days],
            'daystart': [str(start_date)],
            'dayend': [str(end_date)],
            'jincha_cnt': [cnt_jincha],
            'sicha_cnt': [cnt_sicha],
            'jincha_days': [jincha_days],
            'sicha_days': [sicha_days],
            # 'jincha_perc': [round(cnt_jincha * 100 / cnt_days, 1)],
            # 'sicha_perc': [round(cnt_sicha * 100 / cnt_days, 1)],
            'sum_perc': [round((cnt_jincha + cnt_sicha) * 100 / cnt_days, 1)],
            'jincha_sicha_days_ratio': [round(jincha_days / (sicha_days + 1), 2)],
        })

        logging.info(str(code) + " " + name + ", days " + str(cnt_days) + ", jincha cnt: " + str(cnt_jincha) + "  sicha cnt: " + str(cnt_sicha) + "  jincha days: " + str(jincha_days) + "  sicha days: " + str(sicha_days) + "  profit_over_all: " + str(profit_over_all))

        return (df_rtn)

    def _cnt_jin_cha_si_cha_days(self, df_all, df_jin_cha, df_si_cha):
        sicha_days = 0
        jincha_days = 0

        jidx = df_jin_cha.index.tolist()
        sidx = df_si_cha.index.tolist()

        if sidx.__len__() == 0 and jidx.__len__() == 0:
            return ({'jincha_days': jincha_days, 'sicha_days': sicha_days})

        if sidx.__len__() == 0 and jidx.__len__() > 0:
            jincha_days = df_all.index.to_list()[-1] - jidx[0]
            sicha_days = jidx[0] - df_all.index.to_list()[0]
            return ({'jincha_days': jincha_days, 'sicha_days': sicha_days})

        if jidx.__len__() == 0 and sidx.__len__() > 0:
            sicha_days = df_all.index.to_list()[-1] - sidx[0]
            jincha_days = sidx[0] - df_all.index.to_list()[0]
            return ({'jincha_days': jincha_days, 'sicha_days': sicha_days})

        if jidx.__len__() > sidx.__len__():
            trim_days = sidx[-1] - jidx[0]
            sicha_days += jidx[-1] - sidx[-1]  #days of the latest sicha period
            jincha_days += df_all.index.to_list()[-1] - jidx[-1]  # current is jincha perido, days it has been lasted.
        elif sidx.__len__() > jidx.__len__():
            trim_days = jidx[-1] - sidx[0]
            jincha_days += sidx[-1] - jidx[-1]  #days of the latest jincha period
            sicha_days += df_all.index.to_list()[-1] - sidx[-1]  #current is sicha perido, days it has been lasted.
        elif sidx.__len__() > 0 and jidx.__len__() > 0 and (sidx.__len__() == jidx.__len__()):
            if sidx[0] < jidx[0]:
                trim_days = jidx[-1] - sidx[0]
                jincha_days += df_all.index.to_list()[-1] - jidx[-1]

            elif jidx[0] < sidx[0]:
                trim_days = sidx[-1] - jidx[0]
                sicha_days += df_all.index.to_list()[-1] - sidx[-1]

        if sidx[0] < jidx[0]:
            logging.info("start with sicha")
            sicha_days_trim = pd.Series(list(map(sub, jidx, sidx))).sum()
            sicha_days += sicha_days_trim
            jincha_days += trim_days - sicha_days_trim
            sicha_days += sidx[0]
        elif jidx[0] < sidx[0]:
            logging.info("start with jincha")
            jincha_days_trim = pd.Series(list(map(sub, sidx, jidx))).sum()
            jincha_days += jincha_days_trim  #middle body of jincha
            sicha_days += trim_days - jincha_days_trim
            jincha_days += jidx[0]  #head of jincha

        return ({'jincha_days': jincha_days, 'sicha_days': sicha_days})

    def _calc_jin_cha_si_cha_profit(self, df_jin_cha, df_si_cha):
        df_jin_cha['action'] = "B"
        df_si_cha['action'] = "S"

        profit_this = 0
        profit_overall = 0

        if df_jin_cha.__len__() > 0:
            code = df_jin_cha['code'].iloc[0]
        elif df_si_cha.__len__() > 0:
            code = df_si_cha['code'].iloc[0]
        else:
            logging.error("df_jin_cha and df_si_cha all empty, quit")
            exit()

        rtn_df = pd.DataFrame()

        df_tmp = pd.concat([df_jin_cha,df_si_cha]).sort_values(by='date').reset_index().drop('index', axis=1)

        while True:
            if df_tmp.__len__() >= 2 and not (df_tmp.iloc[0]['action'] == 'B' and df_tmp.iloc[1]['action'] == 'S'):
                df_tmp = df_tmp.drop(index=df_tmp.head(1).index.values[0])
            else:
                break
        while True:
            if df_tmp.__len__() >= 2 and not (df_tmp.iloc[-1]['action'] == 'S' and df_tmp.iloc[-2]['action'] == 'B'):
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

        for index, row in df_tmp.iterrows():
            action = row['action']
            close = row['close']
            date = row['date']
            if action == 'B':
                b_date = date
                b_price = close
                # logging.info("Buy at "+row['date']+" "+str(close))
            if action == 'S':
                profit_this = round((close - b_price) * 100 / b_price, 2)
                profit_overall = round(profit_overall + profit_this, 2)
                # logging.info("Sell at " + row['date'] + " " + str(close))
                # logging.info("profit% this "+str(profit_this)+" profit% overall "+str(profit_overall))

                tmp_df = pd.DataFrame({
                    'code': [code],
                    'buy_date': [b_date],
                    'buy_price': [b_price],
                    'sell_date': [date],
                    'sell_price': [close],
                    'profit_this': [profit_this],
                    'profit_overall': [profit_overall],
                })

                rtn_df = pd.concat([rtn_df,tmp_df])

        rtn_df = rtn_df.reset_index().drop('index', axis=1)
        return (rtn_df)

    # general function to return jincha sicha on TWO columns.
    def slow_fast_across(self, df, fast_col_name, slow_col_name):

        df['tmp_col_fast_minor_slow'] = round(df[fast_col_name] - df[slow_col_name],1)

        df['b1_tmp_col_fast_minor_slow'] = df['tmp_col_fast_minor_slow'].shift(1)

        df_si_cha = df[(df['b1_tmp_col_fast_minor_slow'] > 0) & (df['tmp_col_fast_minor_slow'] < 0)]
        df_jin_cha = df[(df['b1_tmp_col_fast_minor_slow'] < 0) & (df['tmp_col_fast_minor_slow'] > 0)]
        return(df, df_si_cha, df_jin_cha)

    # general function to return jincha sicha on SINGLE column.
    def single_column_across(self, df, col_name, threshod=0):
        df['b1_tmp_col'] = df[col_name].shift(1)

        df_si_cha = df[(df['b1_tmp_col'] >= threshod) & (df[col_name] < threshod)]
        df_jin_cha = df[(df['b1_tmp_col'] <= threshod) & (df[col_name] > threshod)]
        return (df_si_cha, df_jin_cha)

    def plot_pivots(self, X, pivots):
        plt.xlim(0, len(X))
        plt.ylim(X.min() * 0.99, X.max() * 1.01)
        plt.plot(np.arange(len(X)), X, 'k:', alpha=0.5)
        plt.plot(np.arange(len(X))[pivots != 0], X[pivots != 0], 'k-')
        plt.scatter(np.arange(len(X))[pivots == 1], X[pivots == 1], color='g')
        plt.scatter(np.arange(len(X))[pivots == -1], X[pivots == -1], color='r')

    def n_days_peak(self, df, date, pivots_peak ):
        peak_price = df[df['date']==date].close.values[0]
        pivots_peak = pivots_peak[pivots_peak['date'] <= date]
        df = df[df['date'] <= date]

        N_days_peak = -1
        peak_start_date = -1
        peak_start_price = -1

        if pivots_peak.__len__() == 0:
            print("no before date records in pivots_peak ")
            return(-1,-1,-1)


        if df['close'].max() == peak_price:
            print("  Dbg, peak is all time high")
            N_days_peak = str(df.__len__())
            peak_start_date = df.iloc[0]['date']
            peak_start_price = df.iloc[0]['close']
        else:
            i = 2
            while i <= pivots_peak.__len__():

                if pivots_peak.iloc[-1 * i]['close'] > peak_price:
                    pk_higher_date = pivots_peak.iloc[-1 * i]['date']
                    print("  Dbg, pk higher date: " + pk_higher_date.strftime('%Y-%m-%d'))
                    df = df[df['date'] > pk_higher_date]
                    df = df[df['close'] < peak_price]

                    peak_start_date = df.iloc[0]['date']
                    peak_start_price = df.iloc[0]['close']
                    N_days_peak = (date - peak_start_date).days
                    break
                i = i + 1


        return(N_days_peak, peak_start_date, peak_start_price)

    def n_days_valley(self, df, date, pivots_valley ):
        valley_price = df[df['date']==date].close.values[0]
        pivots_valley = pivots_valley[pivots_valley['date'] <= date]
        df = df[df['date'] <= date]

        N_days_valley = -1
        valley_start_date = -1
        valley_start_price = -1


        if pivots_valley.__len__() == 0:
            print("no before date records in pivots_peak ")
            return(-1,-1,-1)


        if df['close'].min() == valley_price:
            print("  Dbg, valley is all time high")
            N_days_valley = str(df.__len__())
            valley_start_date = df.iloc[0]['date']
            valley_start_price = df.iloc[0]['close']
        else:
            i = 2
            while i <= pivots_valley.__len__():

                if pivots_valley.iloc[-1 * i]['close'] < valley_price:
                    pk_lower_date = pivots_valley.iloc[-1 * i]['date']
                    print("  Dbg, pk lower date: " + pk_lower_date.strftime('%Y-%m-%d'))
                    df = df[df['date'] > pk_lower_date]
                    df = df[df['close'] > valley_price]

                    valley_start_date = df.iloc[0]['date']
                    valley_start_price = df.iloc[0]['close']
                    N_days_valley = (date - valley_start_date).days
                    break
                i = i + 1


        return(N_days_valley, valley_start_date, valley_start_price)

    def zigzag_peak_valley(self,df, ma_short, ma_middle, ma_long, dates=[]):
        rtn_dict={}
        # df = self.add_macd(df=df)
        # df = self.add_kdj(df=df)
        # df = self.add_rsi(df=df, middle=14)

        df['date'] = df['date'].apply(lambda _d: datetime.strptime(str(_d), "%Y%m%d"))
        df['close'] = df['close'].apply(lambda _d: round(_d,1))
        df = df.set_index('date')

        X = df['close']
        pivots = zigzag.peak_valley_pivots(X.values, 0.04, -0.04)
        pivots[0]=0
        pivots[-1]=0

        # self.plot_pivots(X.values, pivots)


        # plot
        ts_pivots = pd.Series(X, index=X.index)
        ts_pivots_peak = ts_pivots[pivots == 1].reset_index()
        ts_pivots_valley = ts_pivots[pivots == -1].reset_index()
        ts_pivots = ts_pivots[pivots != 0]

        if ts_pivots_peak.__len__() == 0:
            return(pd.DataFrame())
        if ts_pivots_valley.__len__() == 0:
            return(pd.DataFrame())



        # npeak only support the latest peak date.  Don't loop.
        # for index, row in ts_pivots_valley.iterrows():
        #     date=row['date'] #Timesatmp
        #     N_days_peak, peak_start_date, peak_start_price = self.n_days_peak(df=df.reset_index(), date=date, pivots_peak=ts_pivots_peak)
        peak_price = ts_pivots_peak.iloc[-1]['close']
        valley_price = ts_pivots_valley.iloc[-1]['close']
        peak_date = ts_pivots_peak.iloc[-1]['date']
        N_days_peak, peak_start_date, peak_start_price = self.n_days_peak(df=df.reset_index(),
                                                                          date=ts_pivots_peak.iloc[-1]['date'],
                                                                          pivots_peak=ts_pivots_peak)

        if N_days_peak == -1:
            return(pd.DataFrame())

        pct_to_peak = round(100*(df.iloc[-1]['close'] - peak_price)/peak_price,2)
        days_to_peak = (df.reset_index().iloc[-1]['date'] - ts_pivots_peak.iloc[-1]['date']).days


        N_days_valley, valley_start_date, valley_start_price = self.n_days_valley(df=df.reset_index(),
                                                                          date=ts_pivots_valley.iloc[-1]['date'],
                                                                          pivots_valley=ts_pivots_valley)
        if N_days_valley == -1:
            return(pd.DataFrame())
        
        pct_to_valley = round(100*(df.iloc[-1]['close'] - valley_price) / valley_price, 2)
        days_to_valley = (df.reset_index().iloc[-1]['date'] - ts_pivots_valley.iloc[-1]['date']).days


        rtn_dict={
            'code' : [df.iloc[0]['code']],
            'name' : [df.iloc[0]['name']],
            'date' : [df.index[-1].strftime("%Y-%m-%d")],

            'N_days_peak': [N_days_peak],
            'pct_to_peak': [pct_to_peak],
            'days_to_peak': [days_to_peak],

            'N_days_valley': [N_days_valley],
            'pct_to_valley': [pct_to_valley],
            'days_to_valley': [days_to_valley],

            # 'N_days_peak':N_days_peak,
            'peak_date':[ts_pivots_peak.iloc[-1]['date']],
            'peak_close':[ts_pivots_peak.iloc[-1]['close']],
            'peak_start_date':[peak_start_date],
            'peak_start_price':[peak_start_price],

            # 'N_days_valley':N_days_valley,
            'valley_date': [ts_pivots_valley.iloc[-1]['date']],
            'valley_close': [ts_pivots_valley.iloc[-1]['close']],
            'valley_start_date':[valley_start_date],
            'valley_start_price':[valley_start_price],
        }

        if ts_pivots_peak.iloc[-1]['date'] > ts_pivots_valley.iloc[-1]['date']:
            # N_days_peak, peak_start_date, peak_start_price = self.n_days_peak(df=df.reset_index(), date=peak_date, pivots_peak=ts_pivots_peak)
            print("Most recent is Peak, downning from peak.")
            print("Peak date: "+ peak_date.strftime('%Y-%m-%d'))
            print("Peak price: "+ str(peak_price))
            print("N days Peak: " + str(N_days_peak))
            print("  Dbg, Peak_start_date (down since): " + peak_start_date.strftime('%Y-%m-%d'))
            print("  Dbg, Peak_start_price (down since): " + str(peak_start_price))

        if False:
                if dates.__len__() == 0:
                    dates.append(ts_pivots.index.values[-5])  #date[0] is day B1
                    dates.append(ts_pivots.index.values[-4])  #date[0] is day B1
                    dates.append(ts_pivots.index.values[-3])  #date[0] is day B1
                    dates.append(ts_pivots.index.values[-2])  #date[0] is day B1
                    dates.append(ts_pivots.index.values[-1])  #date[1] is today

                # keep_cols =['code', 'date', 'close', 'dif_main', 'dea_signal', 'macd_histogram', 'kdjk', 'kdjd', 'kdjj', 'rsi_middle_14']
                # df = df[keep_cols]
                # df = df.set_index('date')

                plt.clf()
                # plt.suptitle(code+" "+name+" "+notes_in_title, fontproperties=font)

                ax = plt.subplot(4, 1, 1)
                ax.xaxis.set_visible(True)

                days = (pd.to_datetime(str(dates[1])) - pd.to_datetime(str(dates[0]))).days

                plt.title("close_"+pd.to_datetime(str(dates[0])).strftime("%m%d")+"_"+pd.to_datetime(str(dates[1])).strftime("%m%d")+"_"+str(days)+"_days_ago")

                X.plot()
                ts_pivots.plot(style='g-o')
                if dates.__len__() >=5:
                    plt.annotate(X[X.index==dates[0]].values[0], xy=(dates[0], X[X.index==dates[0]].values[0]))
                    plt.annotate(X[X.index==dates[1]].values[0], xy=(dates[1], X[X.index==dates[1]].values[0]))
                    plt.annotate(X[X.index==dates[2]].values[0], xy=(dates[2], X[X.index==dates[2]].values[0]))
                    plt.annotate(X[X.index==dates[3]].values[0], xy=(dates[3], X[X.index==dates[3]].values[0]))
                    plt.annotate(X[X.index==dates[4]].values[0], xy=(dates[4], X[X.index==dates[4]].values[0]))
                plt.show()

                logging.info("figure saved to "  + "\n")
                print()

        return(pd.DataFrame.from_dict(rtn_dict))

    def zigzag_plot(self,df, code, name, notes_in_title="", dates=[]):

        df = self.add_macd(df=df)
        df = self.add_kdj(df=df)
        df = self.add_rsi(df=df, middle=14)

        df['date'] = df['date'].apply(lambda _d: datetime.strptime(str(_d), "%Y%m%d"))
        df = df.set_index('date')

        X = df['close']
        pivots = zigzag.peak_valley_pivots(X.values, 0.1, -0.1)

        # plot
        ts_pivots = pd.Series(X, index=X.index)
        ts_pivots = ts_pivots[pivots != 0]

        if dates.__len__() == 0:
            dates.append(ts_pivots.index.values[-2])  #date[0] is day B1
            dates.append(ts_pivots.index.values[-1])  #date[1] is today

        # keep_cols =['code', 'date', 'close', 'dif_main', 'dea_signal', 'macd_histogram', 'kdjk', 'kdjd', 'kdjj', 'rsi_middle_14']
        # df = df[keep_cols]
        # df = df.set_index('date')

        plt.clf()
        plt.suptitle(code+" "+name+" "+notes_in_title, fontproperties=font)

        ax = plt.subplot(4, 1, 1)
        ax.xaxis.set_visible(False)

        days = (pd.to_datetime(str(dates[1])) - pd.to_datetime(str(dates[0]))).days

        plt.title("close_"+pd.to_datetime(str(dates[0])).strftime("%m%d")+"_"+pd.to_datetime(str(dates[1])).strftime("%m%d")+"_"+str(days)+"_days_ago")

        X.plot()
        ts_pivots.plot(style='g-o')
        if dates.__len__() >=1:
            plt.annotate(X[X.index==dates[0]].values[0], xy=(dates[0], X[X.index==dates[0]].values[0]))
            plt.annotate(X[X.index==dates[1]].values[0], xy=(dates[1], X[X.index==dates[1]].values[0]))


        ax = plt.subplot(4, 1, 2)
        ax.xaxis.set_visible(False)

        plt.title("MACD dif_main")
        df['dif_main'].plot()
        if dates.__len__() >=1:
            df[df.index.isin(dates)]['dif_main'].plot(style='g-o')
            plt.annotate(df[df.index==dates[0]]['dif_main'].values[0], xy=(dates[0], df[df.index==dates[0]]['dif_main'].values[0]))
            plt.annotate(df[df.index==dates[1]]['dif_main'].values[0], xy=(dates[1], df[df.index==dates[1]]['dif_main'].values[0]))

        ax = plt.subplot(4, 1, 3)
        ax.xaxis.set_visible(False)

        plt.title("KDJJ")
        df['kdjj'].plot()
        if dates.__len__() >=1:
            df[df.index.isin(dates)]['kdjj'].plot(style='g-o')
            plt.annotate(df[df.index==dates[0]]['kdjj'].values[0], xy=(dates[0], df[df.index==dates[0]]['kdjj'].values[0]))
            plt.annotate(df[df.index==dates[1]]['kdjj'].values[0], xy=(dates[1], df[df.index==dates[1]]['kdjj'].values[0]))

        plt.subplot(4, 1, 4)
        plt.title("RSI14")
        df['rsi_middle_14'].plot()

        if dates.__len__() >=1:
            df[df.index.isin(dates)]['rsi_middle_14'].plot(style='g-o')
            plt.annotate(df[df.index==dates[0]]['rsi_middle_14'].values[0], xy=(dates[0], df[df.index==dates[0]]['rsi_middle_14'].values[0]))
            plt.annotate(df[df.index==dates[1]]['rsi_middle_14'].values[0], xy=(dates[1], df[df.index==dates[1]]['rsi_middle_14'].values[0]))

        # plt.show()
        fn ="/home/ryan/DATA/result/zigzag_div/"+code+"_"+name+"_"+notes_in_title+".svg"
        # plt.savefig(fn, bbox_inches='tight')
        plt.savefig(fn)
        logging.info("figure saved to " + fn + "\n")
        print()

    def zigzag_divation(self,df,code,name):
        # if (code=='BJ834765'):
        #     logging.info('debug pause')

        df = df[df['close'] != 0].reset_index().drop('index',axis=1)

        rtn_df_macd_div = pd.DataFrame()
        rtn_df_kdj_div = pd.DataFrame()
        rtn_df_rsi_div = pd.DataFrame()

        bool_plot = False
        notes_in_title = ''

        # two number are close if ( rel_tol or abs_tol)
        rel_tol = 0.05 # cmp_relative_tolerance, a/b < 1%
        abs_tol = 0.5  # cmp_absolute_tolerance, a-b < 0.3

        # ===========
        # df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/DAY_Global/AG_qfq/SH600519.csv")
        # df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)
        # df = df[-200:].reset_index().drop('index', axis=1)

        df_o = copy.deepcopy(df)

        df['date'] = df['date'].apply(lambda _d: datetime.strptime(str(_d), "%Y%m%d"))


        # df = df.set_index('date') # no, we don't need to set index on date, because we will not plot.

        X = df['close']
        pivots = zigzag.peak_valley_pivots(X.values, 0.1, -0.1)

        # Indicator Deviation
        # add indicators
        df = self.add_macd(df=df)
        df = self.add_kdj(df=df)
        df = self.add_rsi(df=df, middle=14)

        # keep_cols =['code', 'date', 'close', 'dif_main', 'dea_signal', 'macd_histogram', 'kdjk', 'kdjd', 'kdjj', 'rsi_middle_14']
        # df = df[keep_cols]

        df['pivots'] = pivots  # add pivots to df, irrelevent with plot.

        df = df[df['pivots'] != 0]
        df = df.reset_index().drop('index',axis=1)  # df is stockstats.StockDataFrame

        df = df.drop(index=0) # drop the 1st point.

        if not (df.__len__() >= 4): # 3 points for [M or W] shape, 1 latest point is the latest day.
            logging.info("no enought pivot, at least 4 pivots required, actual "+str(df.__len__()))
            return(rtn_df_macd_div, rtn_df_kdj_div, rtn_df_rsi_div)


        today_to_last_pivot = (df.iloc[-1]['date'] - df.iloc[-2]['date']).days
        notes_in_title += df.iloc[-2]['date'].strftime('%Y%m%d')+"_"
        notes_in_title += str(today_to_last_pivot)+"_days_ago"

        # PLOT using last two confirmed Peaks or Valleys.
        if False:
            if df.iloc[-2]['pivots'] == -1:  # last confirmed pivot is valley, likely at low level price.
                v_b1 = df.iloc[-2]  # valley -1
                v_b2 = df.iloc[-4]  # valley -2
                trend = 'UP'
                days = (v_b1['date'] - v_b2['date']).days
                date1 = v_b1['date']
                date2 = v_b2['date']

                logging.info(code+" "+ name+" "+ ", trend " + trend + ", valley_b2 " + str(v_b2['date']) + ", valley_b1 " + str(v_b1['date']))

                # suppose at the valley now. so check valley v_b1 and v_b2 for deviation.
                if v_b1['close'] <= v_b2['close']:
                    if v_b1['dif_main'] > v_b2['dif_main']:
                        bool_plot = True
                        notes_in_title += "_dif_main_ "+trend
                        logging.info(code+" "+ name+" "+ ", divation on MACD dif_main, expect to raise up. " + str(v_b2['dif_main']) + " " + str(v_b1['dif_main']))
                        rtn_df_macd_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'dif_main_b2': [v_b2['dif_main']],
                            'dif_main_b1': [v_b1['dif_main']],
                            'days': [days],
                            'strength': [ round((v_b1['dif_main'] - v_b2['dif_main'])/abs(v_b2['dif_main'])/math.log(days, numpy.e),2) ],
                        })
                    if v_b1['kdjj'] > v_b2['kdjj']:
                        bool_plot = True
                        notes_in_title += "_kdjj_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on kdjj, expect to raise up. " + str(v_b2['kdjj']) + " " + str(v_b1['kdjj']))
                        rtn_df_kdj_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'kdjj_b2': [v_b2['kdjj']],
                            'kdjj_b1': [v_b1['kdjj']],
                            'days': [days],
                            'strength': [round(
                                (v_b1['kdjj'] - v_b2['kdjj']) / abs(v_b2['kdjj']) / math.log(days, numpy.e),
                                2)],
                        })
                    if v_b1['rsi_middle_14'] > v_b2['rsi_middle_14']:
                        bool_plot = True
                        notes_in_title += "_rsi_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on rsi, expect to raise up. " + str(v_b2['rsi_middle_14']) + " " + str(v_b1['rsi_middle_14']))
                        rtn_df_rsi_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'rsi_middle_14_b2': [v_b2['rsi_middle_14']],
                            'rsi_middle_14_b1': [v_b1['rsi_middle_14']],
                            'days': [days],
                            'strength': [round(
                                (v_b1['rsi_middle_14'] - v_b2['rsi_middle_14']) / abs(v_b2['rsi_middle_14']) / math.log(days, numpy.e),
                                2)],
                        })

            elif df.iloc[-2]['pivots'] == 1:  # last confirmed pivot is peak, likely at high level price.
                p_b1 = df.iloc[-2]
                p_b2 = df.iloc[-4]
                trend = 'DOWN'

                date1 = p_b1['date']
                date2 = p_b2['date']

                days = (p_b1['date'] - p_b2['date']).days
                logging.info(code+" "+ name+" "+ ", trend " + trend + ", peak_b2 " + str(p_b2['date']) + ", peak_b1 " + str(p_b1['date']))

                # suppose at the peak now. so check valley p_b1 and p_b2 for deviation.
                if p_b1['close'] >= p_b2['close']:
                    if p_b1['dif_main'] < p_b2['dif_main']:
                        bool_plot = True
                        notes_in_title += "_dif_main_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on MACD dif_main, expect to going down. " + str(p_b2['dif_main']) + " " + str(p_b1['dif_main']))
                        rtn_df_macd_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'dif_main_b2': [p_b2['dif_main']],
                            'dif_main_b1': [p_b1['dif_main']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['dif_main'] - p_b2['dif_main']) / abs(p_b2['dif_main']) / math.log(days, numpy.e),
                                2)],

                        })

                    if p_b1['kdjj'] < p_b2['kdjj']:
                        bool_plot = True
                        notes_in_title = "_kdjj_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on kdjj, expect to going down. " + str(p_b2['kdjj']) + " " + str(p_b1['kdjj']))
                        rtn_df_kdj_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'kdjj_b2': [p_b2['kdjj']],
                            'kdjj_b1': [p_b1['kdjj']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['kdjj'] - p_b2['kdjj']) / abs(p_b2['kdjj']) / math.log(days, numpy.e),
                                2)],
                        })
                    if p_b1['rsi_middle_14'] < p_b2['rsi_middle_14']:
                        bool_plot = True
                        notes_in_title = "_rsi_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on rsi, expect to going down. " + str(p_b2['rsi_middle_14']) + " " + str(p_b1['rsi_middle_14']))
                        rtn_df_rsi_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'rsi_middle_14_b2': [p_b2['rsi_middle_14']],
                            'rsi_middle_14_b1': [p_b1['rsi_middle_14']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['rsi_middle_14'] - p_b2['rsi_middle_14']) / abs(p_b2['rsi_middle_14']) / math.log(days, numpy.e),
                                2)],
                        })

        # PLOT using lastest two points
        if True:
            if df.iloc[-2]['pivots'] == -1:  # last confirmed pivot is valley. today_close > -2.close
                v_b1 = df.iloc[-1]  # valley -1
                v_b2 = df.iloc[-2]  # valley -2
                trend = 'DOWN'
                days = (v_b1['date'] - v_b2['date']).days
                date1 = v_b1['date']
                date2 = v_b2['date']

                logging.info(code+" "+ name+" "+ ", trend " + trend + ", b2 " + str(v_b2['date']) + ", b1 " + str(v_b1['date']))

                # if div, then indicators at b2 should > b1.
                if (v_b1['close'] >= v_b2['close']):
                    if v_b1['dif_main'] < v_b2['dif_main'] and not math.isclose(v_b1['dif_main'],v_b2['dif_main'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_dif_main_"+trend
                        logging.info(code+" "+ name+" "+ ", divation on MACD dif_main, expect to going down. " + str(v_b2['dif_main']) + " " + str(v_b1['dif_main']))
                        rtn_df_macd_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'dif_main_b2': [v_b2['dif_main']],
                            'dif_main_b1': [v_b1['dif_main']],
                            'days': [days],
                            'strength': [ round((v_b1['dif_main'] - v_b2['dif_main'])/abs(v_b2['dif_main'])/math.log(days, numpy.e),2) ],
                        })
                    if v_b1['kdjj'] < v_b2['kdjj'] and not math.isclose(v_b1['kdjj'],v_b2['kdjj'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_kdjj_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on kdjj, expect to going down. " + str(v_b2['kdjj']) + " " + str(v_b1['kdjj']))
                        rtn_df_kdj_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'kdjj_b2': [v_b2['kdjj']],
                            'kdjj_b1': [v_b1['kdjj']],
                            'days': [days],
                            'strength': [round(
                                (v_b1['kdjj'] - v_b2['kdjj']) / abs(v_b2['kdjj']) / math.log(days, numpy.e),
                                2)],
                        })
                    if v_b1['rsi_middle_14'] < v_b2['rsi_middle_14'] and not math.isclose(v_b1['rsi_middle_14'],v_b2['rsi_middle_14'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_rsi_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on rsi, expect to going down. " + str(v_b2['rsi_middle_14']) + " " + str(v_b1['rsi_middle_14']))
                        rtn_df_rsi_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [v_b2['date']],
                            'valley_b1': [v_b1['date']],
                            'close_b2': [v_b2['close']],
                            'close_b1': [v_b1['close']],
                            'rsi_middle_14_b2': [v_b2['rsi_middle_14']],
                            'rsi_middle_14_b1': [v_b1['rsi_middle_14']],
                            'days': [days],
                            'strength': [round(
                                (v_b1['rsi_middle_14'] - v_b2['rsi_middle_14']) / abs(v_b2['rsi_middle_14']) / math.log(days, numpy.e),
                                2)],
                        })

            elif df.iloc[-2]['pivots'] == 1:  # last confirmed pivot is peak, likely at high level price.
                p_b1 = df.iloc[-1]
                p_b2 = df.iloc[-2]
                trend = 'UP'

                date1 = p_b1['date']
                date2 = p_b2['date']

                days = (p_b1['date'] - p_b2['date']).days
                logging.info(code+" "+ name+" "+ ", trend " + trend + ", b2 " + str(p_b2['date']) + ", b1 " + str(p_b1['date']))

                #
                if p_b1['close'] <= p_b2['close']:
                    if p_b1['dif_main'] > p_b2['dif_main'] and not math.isclose(p_b1['dif_main'],p_b2['dif_main'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_dif_main_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on MACD dif_main, expect to going up. " + str(p_b2['dif_main']) + " " + str(p_b1['dif_main']))
                        rtn_df_macd_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'dif_main_b2': [p_b2['dif_main']],
                            'dif_main_b1': [p_b1['dif_main']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['dif_main'] - p_b2['dif_main']) / abs(p_b2['dif_main']) / math.log(days, numpy.e),
                                2)],

                        })

                    if p_b1['kdjj'] > p_b2['kdjj'] and not math.isclose(p_b1['kdjj'],p_b2['kdjj'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_kdjj_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on kdjj, expect to going up. " + str(p_b2['kdjj']) + " " + str(p_b1['kdjj']))
                        rtn_df_kdj_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'kdjj_b2': [p_b2['kdjj']],
                            'kdjj_b1': [p_b1['kdjj']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['kdjj'] - p_b2['kdjj']) / abs(p_b2['kdjj']) / math.log(days, numpy.e),
                                2)],
                        })
                    if p_b1['rsi_middle_14'] > p_b2['rsi_middle_14'] and not math.isclose(p_b1['rsi_middle_14'],p_b2['rsi_middle_14'],rel_tol=rel_tol, abs_tol=abs_tol):
                        bool_plot = True
                        notes_in_title += "_rsi_" + trend
                        logging.info(code+" "+ name+" "+ ", divation on rsi, expect to going up. " + str(p_b2['rsi_middle_14']) + " " + str(p_b1['rsi_middle_14']))
                        rtn_df_rsi_div = pd.DataFrame({
                            'code': [code],
                            'name': [name],
                            'trend': [trend],
                            'valley_b2': [p_b2['date']],
                            'valley_b1': [p_b1['date']],
                            'close_b2': [p_b2['close']],
                            'close_b1': [p_b1['close']],
                            'rsi_middle_14_b2': [p_b2['rsi_middle_14']],
                            'rsi_middle_14_b1': [p_b1['rsi_middle_14']],
                            'days': [days],
                            'strength': [round(
                                (p_b1['rsi_middle_14'] - p_b2['rsi_middle_14']) / abs(p_b2['rsi_middle_14']) / math.log(days, numpy.e),
                                2)],
                        })

        if bool_plot:
            df['date'] = df['date'].apply(lambda _d: datetime.strftime(_d, "%Y%m%d"))
            self.zigzag_plot(df=df_o, code=code, name=name,notes_in_title=notes_in_title, dates=[date2,date1])

        # df_valley = df[df['pivots'] == -1].reset_index()
        # df_peak = df[df['pivots'] == 1].reset_index()
        # df_profit = self._calc_jin_cha_si_cha_profit(df_si_cha=df_peak, df_jin_cha=df_valley)
        return (rtn_df_macd_div, rtn_df_kdj_div, rtn_df_rsi_div)



    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def buy_sell_decision_based_on_ma4_ma27_distance_condition(self, df, ma_short, ma_middle,ma_long,period='D'):
        df_rtn=df.iloc[-1:].reset_index().drop('index', axis=1)

        if period=='D':
            reason_b = constant.BUY_MA_DISTANCE
            reason_s = constant.SELL_MA_DISTANCE
            look_back_n_bars = 5
        elif period=='W':
            reason_b = constant.BUY_MA_DISTANCE_WEEKLY
            reason_s = constant.SELL_MA_DISTANCE_WEEKLY
            look_back_n_bars = 2
        elif period=='M':
            reason_b = constant.BUY_MA_DISTANCE_MONTHLY
            reason_s = constant.SELL_MA_DISTANCE_MONTHLY
            look_back_n_bars = 2

        df_rtn['reason']=''
        df_rtn['action']=''

        col_ma_short = 'close_' + str(ma_short) + "_sma"
        col_ma_middle = 'close_' + str(ma_middle) + "_sma"
        col_ma_long = 'close_' + str(ma_long) + "_sma"
        col_ma_short_middle_distance = col_ma_short + "_to_" + col_ma_middle + "_distance"

        df['close_gt_ma_short'] = df['close'] > df[col_ma_short]
        df['close_gt_ma_middle'] = df['close'] > df[col_ma_middle]

        df[col_ma_short_middle_distance] = round(100 * (df[col_ma_short] - df[col_ma_middle]) / df[col_ma_middle], 1)

        # Buy Condition
        if df.iloc[-1]['close_gt_ma_short'] == True and df.iloc[-2]['close_gt_ma_short'] == True:
            logging.info("close above to ma short in last two days in a roll")

            if df.iloc[-1][col_ma_short_middle_distance] > 1:
                logging.info("ma_short above ma_middle 1%, " + str(df.iloc[-1][col_ma_short_middle_distance]))

                for l in range(-1, -1*(look_back_n_bars+1), -1):
                    if df.iloc[l][col_ma_short_middle_distance] < 0:
                        logging.info("ma_short across up ma_middle in latest "+str(look_back_n_bars)+" bars." + str(l + 1))
                        logging.info("all conditions are meet, found the stock expected to Buy. " + str(
                            df.iloc[-1]['code']) + " " + str(df.iloc[-1]['date']))
                        df_rtn['reason']=reason_b + '; '
                        df_rtn['action'] = constant.BUY_CHECK+ '; '
                        break

        # Sell Condition
        if df.iloc[-1]['close_gt_ma_short'] == False and df.iloc[-2]['close_gt_ma_short'] == False:
            if df.iloc[-1]['close_gt_ma_middle'] == False and df.iloc[-2]['close_gt_ma_middle'] == False:
                logging.info("close lower ma short in last two days in a roll")
                logging.info("all conditions are meet, found the stock expected to Sell. " + str(
                    df.iloc[-1]['code']) + " " + str(df.iloc[-1]['date']))
                df_rtn['reason'] = reason_s + '; '
                df_rtn['action'] = constant.SELL_CHECK+ '; '

        df_rtn = df_rtn[['date','code','action','reason']]
        return(df_rtn)

    # point and figure chart
    def point_figure(self, df,code, name, reverse=3, debug=False):
        if df.__len__() < 20:
            return(pd.DataFrame())


        rtn_dict={
            'code':[code],
            'name':[name],
            'now':[str(df.iloc[-1]['date']) + " " +str(df.iloc[-1]['close'])],
        }
        df = stockstats.StockDataFrame.retype(df)
        df[['atr_14']]  # add atr_14 column to df
        df['atr_14'] = df['atr_14'].apply(lambda _d: round(_d, 2))
        # df = df[df['atr_14'] >= 0.01]

        df['box_size'] = df['atr_14'].shift(1)  # exclude current day's variation from caculation of box_size.
        df = df[15:]
        df = df.reset_index()  # .drop('index', axis=1)
        box_size = round(df.atr_14.iloc[-1], 2)  # the latest (current) box_size)
        # df[['atr_14']].reset_index().query("atr_1<60 and atr_1>50")
        # discard the NA, inaccurate atr_14

        x_col = 0

        #last trading date
        last_trade_date_dt = df.iloc[-1]['date_dt']
        last_trade_close = df.iloc[-1]['close']

        #the latest revert date, could be U2D or D2U
        last_rev_u2d_date_dt = None
        last_rev_u2d_close = None

        last_rev_d2u_date_dt = None
        last_rev_d2u_close = None

        #last continue trend date
        last_up_trend_date_dt = None
        last_dn_trend_date_dt = None

        last_up_trend_close = None
        last_dn_trend_close = None



        fg_trend = 'NO_TREND'
        df_figure = pd.DataFrame()

        for index, row in df.iterrows():
            date = row['date']
            date_dt = row['date_dt']
            close = row['close']
            box_size = row['box_size']
            # box_size = 35  # ryan debug, 35 is from the TV, ATR14,35,3
            if debug:
                logging.info(f'{date} {str(close)}')
            # current trend price. It is the latest hitted bound price, not the current price.
            if not 'cur_trend_price' in locals():
                cur_trend_price = df.iloc[0].close

            # up trend contine. if price is hit, draw a new tocken in same row
            new_follow_trend_threshold_up = cur_trend_price + box_size
            # down trend contine. if price is hit, draw a new tocken in same row
            new_follow_trend_threshold_dn = cur_trend_price - box_size

            # trend reversed from up to down. if price is hit, start a new row with a diffent token.
            new_contraray_trend_threshold_up_to_dn = cur_trend_price - reverse * box_size

            # trend reversed from down to up. if price is hit, start a new row with a diffent token.
            new_contraray_trend_threshold_dn_to_up = cur_trend_price + reverse * box_size

            if box_size == 0:
                continue

            # box number
            box_number = int((close - cur_trend_price) / box_size)

            # logical start. None to up continue
            if fg_trend == 'NO_TREND':
                if close >= new_follow_trend_threshold_up:
                    fg_trend = 'UP'
                    rtn_dict['trend']=['UP']
                    y_row = 1
                    y_row_b1 = 0
                    # logging.info(
                    #     f'1st POINT, fgrow {str(x_col)}, add UP box: {str(box_number)}, column={y_row}, price={close} date={date}')
                    # f'X {x_col}{y_row},{date},{close}'  # col, row
                    # cur_trend_price = close
                    cur_trend_price = new_follow_trend_threshold_up
                    continue

            # logical start. None to down continue
            if fg_trend == 'NO_TREND':
                if close <= new_follow_trend_threshold_dn:
                    fg_trend = 'DN'
                    rtn_dict['trend']=['DN']

                    y_row = -1
                    y_row_b1 = 0
                    # logging.info(
                    #     f'1st POINT, fgrow {str(x_col)}, add DN box: {str(box_number)}, column={y_row}, price={close} date={date}')
                    # cur_trend_price = close
                    cur_trend_price = new_follow_trend_threshold_dn
                    continue

            # logical, up trend continue
            if fg_trend == 'UP' and close >= new_follow_trend_threshold_up:
                y_row += box_number
                rtn_dict['y_row'] = y_row

                # logging.info(f"debug: cur_p {cur_trend_price} box_size {box_size}")
                # logging.info(
                #     f"up trend continue, fgrow {str(x_col)}, add UP box {str(box_number)}, column={y_row}, price={close} date={date},")
                cur_trend_price = new_follow_trend_threshold_up

                last_up_trend_date_dt = date_dt
                last_up_trend_close = close
                rtn_dict['trend'] = ['UP']

                if last_rev_d2u_date_dt and last_rev_d2u_close:
                    since_rev_day = (last_trade_date_dt - last_rev_d2u_date_dt).days
                    # since_rev_day = (date_dt - last_rev_d2u_date_dt).days
                    # since_rev_inc = round(100*(close - last_rev_d2u_close)/last_rev_d2u_close,1)
                    since_rev_inc = round(100*(last_trade_close - last_rev_d2u_close)/last_rev_d2u_close,1)
                    rtn_dict['trend_length'] = [f'UP, {y_row}, {date} {close}, {since_rev_day}D_{since_rev_inc}%']


                    rtn_dict['since_rev_day']=[since_rev_day]
                    rtn_dict['since_rev_inc']=[since_rev_inc]
                else:
                    rtn_dict['trend_length']=[f'UP, {y_row}, {date} {close}']

                continue

            # logical, up trend rev to down trend
            if fg_trend == 'UP' and close <= new_contraray_trend_threshold_up_to_dn:
                x_col += 1
                # y_row = y_row - 1
                y_row_b1 = y_row
                y_row = -1
                rtn_dict['y_row'] = y_row

                # logging.info(f"debug: cur_p {cur_trend_price} box_size {box_size}")
                # logging.info(
                #     f"up trend rev to down, fgrow {str(x_col)}, add DN box {str(box_number)}, column={y_row}, price={close} date={date},")
                fg_trend = 'DN'
                rtn_dict['trend'] = [fg_trend]
                cur_trend_price = new_contraray_trend_threshold_up_to_dn

                last_rev_u2d_date_dt = date_dt
                last_rev_u2d_close = close


                rtn_dict['trend_rev_at'] = [f'U2D, {date} {close}']
                rtn_dict['trend_length'] = [f'DN, {y_row}, {date} {close}']
                rtn_dict['y_row_b1'] = y_row_b1

                since_rev_day = None
                since_rev_inc = None

                continue

            # logical, down trend continue
            if fg_trend == 'DN' and close <= new_follow_trend_threshold_dn:
                y_row -= abs(box_number)
                rtn_dict['y_row'] = y_row
                # logging.info(f"debug: cur_p {cur_trend_price} box_size {box_size}")
                # logging.info(
                #     f"down trend continue, fgrow {str(x_col)}, add DN box {str(box_number)}, column={y_row}, price={close} date={date},")
                cur_trend_price = new_follow_trend_threshold_dn

                rtn_dict['trend']=['DN']

                last_dn_trend_date_dt = date_dt
                last_dn_trend_close = close

                if last_rev_u2d_date_dt and last_rev_u2d_close:
                    # since_rev_day = (date_dt - last_rev_u2d_date_dt).days
                    since_rev_day = (last_trade_date_dt - last_rev_u2d_date_dt).days
                    # since_rev_inc = round(100*(close-last_rev_u2d_close)/last_rev_u2d_close,1)
                    since_rev_inc = round(100*(last_trade_close-last_rev_u2d_close)/last_rev_u2d_close,1)
                    rtn_dict['trend_length'] = [f'DN, {y_row}, {date} {close}, {since_rev_day}D_{since_rev_inc}%']
                    rtn_dict['since_rev_day'] = [since_rev_day]
                    rtn_dict['since_rev_inc'] = [since_rev_inc]
                else:
                    rtn_dict['trend_length'] = [f'DN, {y_row}, {date} {close}']

                continue

            # logical, down trend rev to up trend
            if fg_trend == 'DN' and close >= new_contraray_trend_threshold_dn_to_up:
                x_col += 1
                # y_row = y_row + 1
                y_row_b1 = y_row
                y_row = 1
                rtn_dict['y_row'] = y_row

                # logging.info(f"debug: cur_p {cur_trend_price} box_size {box_size}")
                # logging.info(
                #     f"down trend rev to up, fgrow {str(x_col)}, add UP box {str(box_number)}, column={y_row}, price={close} date={date},")

                cur_trend_price = new_contraray_trend_threshold_dn_to_up
                fg_trend = 'UP'
                rtn_dict['trend']=[fg_trend]

                last_rev_d2u_date_dt = date_dt
                last_rev_d2u_close = close

                rtn_dict['trend_rev_at'] = [f'D2U, {date} {close}']
                rtn_dict['trend_length'] = [f'UP, {y_row}, {date} {close}']
                rtn_dict['y_row_b1'] = y_row_b1

                since_rev_day = None
                since_rev_inc = None

                continue

            if debug:
                logging.info('no point print for the day')
            #end of for loop

        #enf of def
        rtn_df = pd.DataFrame.from_dict(rtn_dict)

        if debug:
            logging.info(finlib.Finlib().pprint(rtn_df))
        return(rtn_df)

    def get_pnf(self, type, debug=False):

        ## PnF point and figure
        o_dir = '/home/ryan/DATA/result/point_and_figure'
        if not os.path.isdir(o_dir):
            os.mkdir(o_dir)

        out_csv = f"{o_dir}/pnf_{type}.csv"

        if finlib.Finlib().is_cached(out_csv, day=1):
            logging.info(f"loading from {out_csv}")
            return(pd.read_csv(out_csv))


        if type == 'AG_OPTION_ETF_60M':
            ### PnF Index
            df_all = finlib.Finlib().load_all_ag_option_etf_60m()
            reverse = 2

        if type == 'AG_OPTION_ETF_DAY':
            ### PnF Index
            df_all = finlib.Finlib().load_all_ag_option_etf_day()
            reverse = 2

        if type == 'AG_INDEX':
            ### PnF Index
            df_all = finlib.Finlib().load_all_ag_index_data()
            df_all = finlib.Finlib().add_index_name_to_df(df_all)
            reverse = 2

        if type == 'AG_BK':
            ### PnF bk
            df_all = finlib.Finlib().load_all_bk_qfq_data()
            df_all['name'] = df_all['code']
            reverse = 3

        if type == 'AG':
            ### PnF AG
            df_all = finlib.Finlib().load_all_ag_qfq_data(days=300)
            df_all = finlib.Finlib().add_stock_name_to_df(df=df_all)
            reverse = 3

            if debug:
                df_all = df_all[df_all['code']=='SH600519']

        if 'time_key' in df_all.columns:
            df_all['date_dt'] = df_all['time_key'].apply(lambda _d: datetime.strptime(str(_d), '%Y-%m-%d %H:%M:%S'))
        else:
            df_all['date_dt'] = df_all['date'].apply(lambda _d: datetime.strptime(str(_d), '%Y%m%d'))

        pnf_df = pd.DataFrame()

        for c in df_all['code'].unique():
            # c="移动支付.em"
            df = df_all[df_all['code'] == c]
            name = df.iloc[0]['name']

            # logging.info(f"code {c}", {str})

            rtn_df = self.point_figure(df=df, code=c, name=name, reverse=reverse)
            # logging.info(f'json:')+json.dumps(r)
            # logging.info(rtn_df)
            pnf_df = pd.concat([pnf_df, rtn_df])

        pnf_df = finlib.Finlib().adjust_column(df=pnf_df,col_name_list=['code','name','trend','y_row','y_row_b1'])
        logging.info(f"Point and Figure PnF of {type}:")
        logging.info(finlib.Finlib().pprint(pnf_df.head(20)))
        pnf_df.to_csv(out_csv, encoding='UTF-8', index=False)
        logging.info(f'fg_figure saved to {out_csv}')
        return(pnf_df)

    def stock_price_volatility(self, csv_o):
        if finlib.Finlib().is_cached(csv_o, day=1):
            logging.info("using the result file " + csv_o)
            df_rtn = pd.read_csv(csv_o)

            logging.info("stock vilatility 300D describe ")
            logging.info(df_rtn[["vlt_300D"]].describe())

            logging.info("stock vilatility 300D most active 5 ")
            logging.info(finlib.Finlib().pprint(df_rtn.sort_values(by='vlt_300D', ascending=True).tail(5)))

            logging.info("stock vilatility 300D most stable 10 ")
            logging.info(finlib.Finlib().pprint(df_rtn.sort_values(by='vlt_300D', ascending=True).head(10)))

            return (df_rtn)

        df_rtn = pd.DataFrame()

        csv_index = '/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
        df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index).tail(300).reset_index().drop('index',
                                                                                                         axis=1)
        d = df_index['close'].describe()
        vlt = round(100 * d['std'] / d['mean'], 1)
        logging.info(f"code index SH000001, vlt300 {str(vlt)}")

        _df = pd.DataFrame.from_dict({
            'code': ['SH000001'],
            'name': ['SZZS'],
            'vlt_300D': [vlt],
        })
        df_rtn = pd.concat([df_rtn, _df])

        df = finlib.Finlib().load_all_ag_qfq_data(days=300)

        df = finlib.Finlib().load_all_us_ak_data(days=300)

        df = finlib.Finlib().add_stock_name_to_df(df=df)
        # df = finlib.Finlib().remove_garbage(df=df)

        codes = df['code'].unique()
        codes.sort()
        df_profit_report = pd.DataFrame()

        for c in codes[:5000]:
            logging.info(c)
            if df.__len__() < 300:
                continue

            df_sub = df[df['code'] == c].tail(300).reset_index().drop('index', axis=1)
            if df_sub.__len__() < 300:
                continue
            name = df_sub.iloc[1]['name']

            d = df_sub['close'].describe()

            vlt = round(100 * d['std'] / d['mean'], 1)
            logging.info(f"code {c}, {name}, vlt300 {str(vlt)}")

            _df = pd.DataFrame.from_dict({
                'code': [c],
                'name': [name],
                'vlt_300D': [vlt],
            })
            df_rtn = pd.concat([df_rtn, _df])

            continue

        df_rtn.to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("result saved to " + csv_o)
        return(df_rtn)



    def stock_price_volatility_us(self, csv_o):
        if finlib.Finlib().is_cached(csv_o, day=1):
            logging.info("using the result file " + csv_o)
            df_rtn = pd.read_csv(csv_o)

            logging.info("stock vilatility 300D describe ")
            logging.info(df_rtn[["vlt_300D"]].describe())

            logging.info("stock vilatility 300D most active 5 ")
            logging.info(finlib.Finlib().pprint(df_rtn.sort_values(by='vlt_300D', ascending=True).tail(5)))

            logging.info("stock vilatility 300D most stable 10 ")
            logging.info(finlib.Finlib().pprint(df_rtn.sort_values(by='vlt_300D', ascending=True).head(10)))

            return (df_rtn)

        df_rtn = pd.DataFrame()



        csv_index = '/home/ryan/DATA/DAY_Global/stooq/US_INDEX/NASDAQ100.csv'
        df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index).tail(300).reset_index().drop('index',
                                                                                                         axis=1)
        d = df_index['close'].describe()
        vlt = round(100 * d['std'] / d['mean'], 1)
        logging.info(f"code index NASDAQ100, vlt300 {str(vlt)}")

        _df = pd.DataFrame.from_dict({
            'code': ['NASDAQ100'],
            'name': ['NASDAQ100'],
            'vlt_300D': [vlt],
        })
        df_rtn = pd.concat([df_rtn, _df])

        df = finlib.Finlib().load_all_us_ak_data(days=300, mktcap_n=1000)

        # df = finlib.Finlib().remove_garbage(df=df)

        codes = df['code'].unique()
        codes.sort()
        df_profit_report = pd.DataFrame()

        for c in codes[:5000]:
            # logging.info(c)
            if df.__len__() < 300:
                continue

            df_sub = df[df['code'] == c].tail(300).reset_index().drop('index', axis=1)
            if df_sub.__len__() < 300:
                continue
            name = df_sub.iloc[1]['name']

            d = df_sub['close'].describe()

            if d['mean'] == 0:
                continue


            vlt = round(100 * d['std'] / d['mean'], 1)
            logging.info(f"code {c}, {name}, vlt300 {str(vlt)}")

            _df = pd.DataFrame.from_dict({
                'code': [c],
                'name': [name],
                'vlt_300D': [vlt],
            })
            df_rtn = pd.concat([df_rtn, _df])

            continue

        
        df_rtn = finlib.Finlib().add_name_mktcap_pe_to_df_us(df=df_rtn)
        df_rtn = df_rtn.sort_values(by=['mktcap'], ascending=False, inplace=False)
        df_rtn.to_csv(csv_o, encoding='UTF-8', index=False)
        logging.info("result saved to " + csv_o)
        return(df_rtn)


    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass

    def w_shape_exam(self, df):
        pass
