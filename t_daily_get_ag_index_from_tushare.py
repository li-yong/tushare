# coding: utf-8

import os
import tushare as ts
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import finlib
from optparse import OptionParser
import logging
import sys
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

logging.info(__file__+" "+"\n")
logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


def check_weekly_buysell_strategy(df):
    cap = cap_init = 10000
    stock_num = 0
    x_axis = []
    y_axis = []

    for index, row in df.iterrows():
        d, o, c = row['date'], row['close'], row['open']
        y = d.year

        if y < 2019 or y > 2020:
            continue

        weekDays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

        if d.weekday() == 0:  # Monday is 0 and Sunday is 6.
            print("Buy at " + weekDays[d.weekday()] + " close. " + str(d) + ", " + str(c) + ", cap " + str(cap))
            stock_num = round(cap / c, 5)
            x_axis.append(d)
            y_axis.append(cap)

        if stock_num > 0 and d.weekday() == 3:  # sell on Thursday open

            cap = stock_num * o
            stock_num = 0
            print("sell at " + weekDays[d.weekday()] + " close, " + str(d) + ", " + str(c) + ", cap " + str(cap))
            x_axis.append(d)
            y_axis.append(cap)

    print(str(cap_init) + " --> " + str(cap))
    exit(0)


def fetch_index(myToken, ts_code, csv_file):
    ts.set_token(myToken)
    pro = ts.pro_api()
    df = pro.index_daily(ts_code=ts_code)
    df = df.reindex(index=df.index[::-1])

    if (df.__len__() > 0):
        df.to_csv(csv_file, index=False)
        logging.info(__file__+" "+"Index saved to " + csv_file + " ,len " + str(df.__len__()))
    else:
        logging.info(__file__+" "+"df lengh is 0, did not save to " + csv_file)


def main():
    ########################
    #
    #########################

    parser = OptionParser()

    parser.add_option("-f", "--fetch_index", action="store_true", dest="fetch_index_f", default=False, help="fetch SH000001")

    parser.add_option("-v", "--verify_weekly_buy_sel_strategy", action="store_true", dest="verify_weekly_buy_sel_strategy_f", default=False, help="verify_weekly_buy_sel_strategy, buy on Mon, sell on Thu/Fri")

    (options, args) = parser.parse_args()
    fetch_index_f = options.fetch_index_f
    verify_weekly_buy_sel_strategy_f = options.verify_weekly_buy_sel_strategy_f

    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
    csv_dir = '/home/ryan/DATA/DAY_Global/AG_INDEX'

    if not os.path.isdir(csv_dir):
        os.mkdir(csv_dir)

    if options.fetch_index_f:
        ts_code = '000001.SH'  #上证指数
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '000300.SH'  #沪深300
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '000905.SH'  #中证500
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '399001.SZ'  #深圳成指
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '399005.SZ'  #中小板指
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '399006.SZ'  #创业板指
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '399016.SZ'  #深圳创新
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '399300.SZ'  #沪深300
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

        ts_code = '000688.SH'  #科创50
        fetch_index(myToken, ts_code=ts_code, csv_file=csv_dir + "/" + ts_code + ".csv")

    if options.verify_weekly_buy_sel_strategy_f:

        df = pd.read_csv(csv_file, skiprows=1, header=None, names=['code', 'date', 'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'], converters={'code': str})

        # date int to datetime
        df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y%m%d'))

        check_weekly_buysell_strategy(df)

    exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
