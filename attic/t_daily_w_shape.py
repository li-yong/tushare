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
import finlib
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs

from sklearn.cluster import KMeans
from calendar import monthrange

import stockstats
import tabulate
import os
import logging

from optparse import OptionParser

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)


def _diff(d1, d2):
    return ((d1 - d2)**2 * 100 / (d1 + d2))


def equal(d1, d2, threshold=3):
    if _diff(d1, d2) <= threshold:
        return (True)
    else:
        return (False)


def great(d1, d2, threshold=3):
    if (d1 > d2) and _diff(d1, d2) >= threshold:
        return (True)
    else:
        return (False)


def less(d1, d2, threshold=3):
    if (d1 < d2) and _diff(d1, d2) >= threshold:
        return (True)
    else:
        return (False)


# past < t3 < t2 < t1 < now. dx is the price of tx.
def get_trend(d3, d2, d1):
    trend = 'none'

    if d3 > d2 and d2 > d1:
        trend = 'down'
        return (trend)

    if d3 < d2 and d2 < d1:
        trend = 'up'
        return (trend)

    if equal(d3, d2, threshold=2):
        if equal(d2, d1, threshold=2):
            trend = 'none'
        elif great(d2, d1, threshold=0.5):
            trend = 'down'
        elif less(d2, d1, threshold=0.5):
            trend = 'up'

    elif great(d3, d2, threshold=2):
        if great(d2, d1, threshold=0.5):
            trend = "down"
        elif less(d2, d1, threshold=2):
            trend = "down_up"
    elif less(d3, d2, threshold=2):
        if less(d2, d1, threshold=0.5):
            trend = "up"
        elif great(d2, d1, threshold=2):
            trend = "up_down"

    return (trend)


def w_shape(csv_f, period):
    csv_f = '/home/ryan/DATA/DAY_Global/AG/SH600519.csv'  #ryan debug
    period = 'D'

    if not os.path.exists(csv_f):
        logging.info('file not exist. ' + csv_f)
        return (rtn)

    logging.info(__file__+" "+csv_f + ": ")

    df_rtn = pd.DataFrame()

    df = pd.read_csv(csv_f, converters={'code': str}, header=None, skiprows=1, names=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'tnv'])

    if period == "M":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
    elif period == "W":
        df_period = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
    elif period == "D":
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
        df_period = df

    len = df_period.__len__()
    for i in range(len):
        df_new = df_period[len - i - 100:len - i]
        logging.info(__file__+" "+"Verifying " + str(df_new[-1:].date.values[0]))
        rtn = _w_shape(df_new)
        if rtn['reason'][0] == "NOT_MATCH":
            pass
        elif rtn['strength'][0] <= 0:
            pass
        else:
            print("HIT!!!")
            print(rtn)
            #exit(0)


def _w_shape(df):

    rtn = {
        "reason": [''],
        "strength": [''],
        "action": [''],
        "code": [''],
        "date": [''],
        "p_cur": [''],
        "p_w_head": [''],
    }

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)

    this_code = df.iloc[0]['code']  #'SH603999'
    this_date = df.iloc[-1]['date'].strftime("%Y-%m-%d")  #monthly period, end date of the month.
    this_reason = ''
    this_strength = 0
    this_action = ''

    # logging.info(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #ryan debug
    # logging.info(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    stage = "null"
    d0 = d1 = d2 = d3 = d4 = d5 = d6 = d7 = d8 = "null"
    p0 = p1 = p2 = p3 = p4 = p5 = p6 = p7 = p8 = 0

    df = df.reset_index().drop('index', axis=1)
    stage == 'null'

    for i in reversed(range(df.__len__())):
        if i <= 3:
            break

        r1 = df.loc[i]
        r2 = df.loc[i - 1]
        r3 = df.loc[i - 2]

        trend = get_trend(r3.close, r2.close, r1.close)

        #logging.info(trend+" "+str(r1.date)+str(r1.close)+ ". v_point:"+str(r2.date)+str(r2.close)+" "+str(r3.date)+str(r3.close))

        if trend == 'up':
            if stage == 'null':
                p0 = r1.close
                d0 = r1.date.strftime("%Y-%m-%d")
                logging.info(__file__+" "+"init s0" + ", middle point:" + str(r1.date) + " " + str(r1.close))
                stage = "s0"
                continue

            if stage in ["s0", "s2", "s4", "s6", "s8"]:
                logging.info(__file__+" "+"moving up on " + stage)
                continue

        if trend == 'down':
            if stage == 'null':
                this_reason = "NOT_MATCH"
                logging.info(__file__+" "+"not match")
                break

            if stage in ["s1", "s3", "s5", "s7"]:
                logging.info(__file__+" "+"moving down on " + stage)
                continue

        if trend == 'up_down':
            if stage == 'null':
                logging.info(__file__+" "+"not match")
                this_reason = "NOT_MATCH"
                break

            if stage in ["s1"]:
                logging.info(__file__+" "+"spot p2 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p2 = r2.close
                d2 = r2.date.strftime("%Y-%m-%d")
                stage = "s2"
                continue

            if stage in ["s3"]:
                logging.info(__file__+" "+"spot p4 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p4 = r2.close
                d4 = r2.date.strftime("%Y-%m-%d")
                stage = "s4"
                continue

            if stage in ["s5"]:
                logging.info(__file__+" "+"spot p6 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p6 = r2.close
                d6 = r2.date.strftime("%Y-%m-%d")
                stage = "s6"
                continue

            if stage in ["s7"]:
                logging.info(__file__+" "+"spot p8 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p8 = r2.close
                d8 = r2.date.strftime("%Y-%m-%d")
                stage = "s8"
                continue

        if trend == 'down_up':
            if stage == 'null':
                logging.info(__file__+" "+"not match")
                this_reason = "NOT_MATCH"
                break

            if stage in ["s0"]:
                logging.info(__file__+" "+"spot p1 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                stage = "s1"
                p1 = r2.close
                d1 = r2.date.strftime("%Y-%m-%d")
                continue

            if stage in ["s2"]:
                logging.info(__file__+" "+"spot p3 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p3 = r2.close
                d3 = r2.date.strftime("%Y-%m-%d")
                stage = "s3"

                if (p3 <= p1) and (p2 <= p0):
                    this_strength = 1
                    continue
                else:
                    break

            if stage in ["s4"]:
                logging.info(__file__+" "+"spot p5 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                stage = "s5"
                p5 = r2.close
                d5 = r2.date.strftime("%Y-%m-%d")
                if (p5 < p3) and (p4 < p2):
                    this_strength = 2
                    continue
                else:
                    break

            if stage in ["s6"]:
                logging.info(__file__+" "+"spot p7 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p7 = r2.close
                d7 = r2.date.strftime("%Y-%m-%d")
                stage = "s7"
                if (p7 < p5) and (p8 < p6):
                    this_strength = 3
                    continue
                else:
                    break

    rtn = {
        "reason": [this_reason],
        "strength": [this_strength],
        "action": [this_action],
        "code": [this_code],
        "date": [this_date],
        "date_0": [d0, p0],
        "date_1": [d1, p1],
        "date_2": [d2, p2],
        "date_3": [d3, p3],
        "date_4": [d4, p4],
        "date_5": [d5, p5],
        "date_6": [d6, p6],
        "date_7": [d7, p7],
        "date_8": [d8, p8],
    }

    return (rtn)


def _w_shape_del(df):

    rtn = {
        "reason": [''],
        "strength": [''],
        "action": [''],
        "code": [''],
        "date": [''],
        "p_cur": [''],
        "p_w_head": [''],
    }

    if df.__len__() < 100:
        logging.info('file less than 100 records. ' + csv_f)
        return (rtn)

    this_code = df.iloc[0]['code']  #'SH603999'
    this_date = df.iloc[-1]['date'].strftime("%Y-%m-%d")  #monthly period, end date of the month.
    this_reason = ''
    this_strength = 0
    this_action = ''

    # logging.info(tabulate.tabulate(df[-20:], headers='keys', tablefmt='psql'))  #ryan debug
    # logging.info(tabulate.tabulate(df_monthly_s[-20:], headers='keys', tablefmt='psql'))

    stage = "null"
    d0 = d1 = d2 = d3 = d4 = d5 = d6 = d7 = d8 = "null"
    p0 = p1 = p2 = p3 = p4 = p5 = p6 = p7 = p8 = "null"

    df = df.reset_index().drop('index', axis=1)
    stage == 'null'

    df = df.sort_values(by='close')

    pass
    hd0 = df.iloc[-1]['date'].date().strftime('%Y-%m-%d')
    hd1 = df.iloc[-2]['date'].date().strftime('%Y-%m-%d')
    hd3 = df.iloc[-3]['date'].date().strftime('%Y-%m-%d')
    hd4 = df.iloc[-4]['date'].date().strftime('%Y-%m-%d')

    hp0 = df.iloc[-1]['close']
    hp1 = df.iloc[-2]['close']
    hp3 = df.iloc[-3]['close']
    hp4 = df.iloc[-4]['close']

    l0 = df.iloc[1]['date'].date().strftime('%Y-%m-%d')
    l1 = df.iloc[2]['date'].date().strftime('%Y-%m-%d')
    l3 = df.iloc[3]['date'].date().strftime('%Y-%m-%d')
    l4 = df.iloc[4]['date'].date().strftime('%Y-%m-%d')

    lp0 = df.iloc[-1]['close']
    lp1 = df.iloc[-2]['close']
    lp3 = df.iloc[-3]['close']
    lp4 = df.iloc[-4]['close']

    pass

    for i in reversed(range(df.__len__())):
        if i <= 3:
            break

        r1 = df.loc[i]
        r2 = df.loc[i - 1]
        r3 = df.loc[i - 2]

        trend = get_trend(r3.close, r2.close, r1.close)

        #logging.info(trend+" "+str(r1.date)+str(r1.close)+ ". v_point:"+str(r2.date)+str(r2.close)+" "+str(r3.date)+str(r3.close))

        if trend == 'up':
            if stage == 'null':
                p0 = r2.close
                d0 = r2.date.strftime("%Y-%m-%d")
                logging.info(__file__+" "+"init s0" + ", middle point:" + str(r2.date) + " " + str(r2.close))
                stage = "s0"
                continue

            if stage in ["s0", "s2", "s4"]:
                logging.info(__file__+" "+"moving up on " + stage)
                continue

        if trend == 'down':
            if stage == 'null':
                this_reason = "NOT_MATCH"
                logging.info(__file__+" "+"not match")
                break

            if stage in ["s1", "s3"]:
                logging.info(__file__+" "+"moving down on " + stage)
                continue

        if trend == 'up_down':
            if stage == 'null':
                logging.info(__file__+" "+"not match")
                this_reason = "NOT_MATCH"
                break

            if stage in ["s1"]:
                logging.info(__file__+" "+"spot p2 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p2 = r2.close
                d2 = r2.date.strftime("%Y-%m-%d")
                stage = "s2"
                continue

            if stage in ["s3"]:
                logging.info(__file__+" "+"spot p4 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p4 = r2.close
                d4 = r2.date.strftime("%Y-%m-%d")
                stage = "s4"
                continue

            if stage in ["s5"]:
                logging.info(__file__+" "+"spot p6 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p6 = r2.close
                d6 = r2.date.strftime("%Y-%m-%d")
                stage = "s6"
                continue

            if stage in ["s7"]:
                logging.info(__file__+" "+"spot p8 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p8 = r2.close
                d8 = r2.date.strftime("%Y-%m-%d")
                stage = "s8"
                continue

        if trend == 'down_up':
            if stage == 'null':
                logging.info(__file__+" "+"not match")
                this_reason = "NOT_MATCH"
                break

            if stage in ["s0"]:
                logging.info(__file__+" "+"spot p1 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                stage = "s1"
                p1 = r2.close
                d1 = r2.date.strftime("%Y-%m-%d")
                continue

            if stage in ["s2"]:
                logging.info(__file__+" "+"spot p3 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p3 = r2.close
                d3 = r2.date.strftime("%Y-%m-%d")
                stage = "s3"

                if (p3 <= p1) and (p2 <= p0):
                    this_strength = 1
                    continue
                else:
                    break

            if stage in ["s4"]:
                logging.info(__file__+" "+"spot p5 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                stage = "s5"
                p5 = r2.close
                d5 = r2.date.strftime("%Y-%m-%d")
                if (p5 < p3) and (p4 < p2):
                    this_strength = 2
                    continue
                else:
                    break

            if stage in ["s6"]:
                logging.info(__file__+" "+"spot p7 in " + stage + ", middle point:" + str(r2.date) + " " + str(r2.close))
                p7 = r2.close
                d7 = r2.date.strftime("%Y-%m-%d")
                stage = "s7"
                if (p7 < p5) and (p8 < p6):
                    this_strength = 3
                    continue
                else:
                    break

    rtn = {
        "reason": [this_reason],
        "strength": [this_strength],
        "action": [this_action],
        "code": [this_code],
        "date": [this_date],
        "date_0": [d0, p0],
        "date_1": [d1, p1],
        "date_2": [d2, p2],
        "date_3": [d3, p3],
        "date_4": [d4, p4],
        "date_5": [d5, p5],
        "date_6": [d6, p6],
        "date_7": [d7, p7],
        "date_8": [d8, p8],
    }

    return (rtn)


def main():
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("--indicator", type="str", dest="indicator_f", default=None, help="indicator, one of [KDJ|MACD]")

    parser.add_option("--period", type="str", dest="period_f", default=None, help="period, one of [M|W|D]")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on [MACD|KDJ] M|W|D result.")

    (options, args) = parser.parse_args()

    indicator = options.indicator_f
    period = options.period_f
    analyze_f = options.analyze_f

    w_shape(csv_f="", period='D')


### MAIN ####
if __name__ == '__main__':
    main()
