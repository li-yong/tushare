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

# import matplotlib.pyplot as plt
# from pandas.plotting import register_matplotlib_converters
# register_matplotlib_converters()

import pandas
import mysql.connector
from sqlalchemy import create_engine
import re
import math
from datetime import datetime, timedelta
from scipy import stats
import sys
import traceback
from jaqs.data.dataapi import DataApi
import glob
import stockstats

import finlib
import logging
import yaml
import warnings
import constant

# warnings.filterwarnings("error")
warnings.filterwarnings("default")


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
    def upper_body_lower_shadow(self, df):
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
            junxian_seri = self.sma_jincha_sicha_duotou_koutou(df_tmp, short=5, middle=10, long=20).iloc[-1]

            #yunxian_buy: down trend, down_bar large bar.
            if (junxian_seri['kongtou_pailie']):
                if df.iloc[i-1]['open']>df.iloc[i-1]['close']:
                        if (not df.iloc[i-1]['long_upper_shadow'] ):
                            if (not df.iloc[i-1]['long_lower_shadow'] ):
                                if (not df.iloc[i-1]['small_body'] ):
                                    if df.iloc[i-1]['tr'] > 1.0* df.iloc[i-2]['atr_short_5']:
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
                                    if df.iloc[i-1]['tr'] > 1.0* df.iloc[i-2]['atr_short_5']:
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

        stock = stockstats.StockDataFrame.retype(df)

        df['sma_short_' + str(short)] = stock['close_' + str(short) + '_sma']
        df['sma_middle_' + str(middle)] = stock['close_' + str(middle) + '_sma']
        df['sma_long_' + str(long)] = stock['close_' + str(long) + '_sma']

        df['ema_short_' + str(short)] = stock['close_' + str(short) + '_ema']
        df['ema_middle_' + str(middle)] = stock['close_' + str(middle) + '_ema']
        df['ema_long_' + str(long)] = stock['close_' + str(long) + '_ema']


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
                     constant.DIF_CROSS_OVER_0,
                     constant.DIF_CROSS_DOWN_0,
                     constant.SIG_CROSS_OVER_0,
                     constant.SIG_CROSS_DOWN_0,

                     constant.DIF_CROSS_DOWN_SIG,
                     constant.DIF_CROSS_OVER_SIG,

                     constant.DIF_LT_0,
                     constant.DIF_GT_0,
                     constant.SIG_LT_0,
                     constant.SIG_GT_0,
                     constant.DIF_LT_SIG,
                     constant.DIF_GT_SIG,

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


        else:
            logging.error("Unknow source csv that matching query "+query)
            exit(0)


        df = pd.read_csv(source_csv, encoding="utf-8")

        df_match = df[df[column_name].str.contains(query)].reset_index()

        if top_n > 0:
            df_match = df_match.head(top_n)


        if 'index' in df_match.columns:
            df_match = df_match.drop('index', axis=1)

        perc = round(df_match.__len__()*100/df.__len__(), 2)
        logging.info("Period: "+period+", Query: "+query+", "+str(df_match.__len__())+" of "+str(df.__len__())+", perc "+str(perc))
        finlib.Finlib().pprint(df_match.head(2))

        col = ['code', 'name', 'date', 'close', 'action','strength','reason','operation', ]
        df_match = finlib.Finlib().keep_column(df_match, col)

        return(df_match)





    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass



    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass




