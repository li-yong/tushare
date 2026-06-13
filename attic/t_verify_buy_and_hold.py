# coding: utf-8

import finlib
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas
import math
import re
from scipy import stats
import datetime
import traceback
import sys
import tushare.util.conns as ts_cs
import finlib
from optparse import OptionParser

trade_cost = 0.005  #buy lost and sell lost, paid to the exchange each way.


def calc_daily_incease(input_csv, bv_csv):

    df = pd.read_csv(input_csv, skiprows=1, converters={'code': str}, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])

    df = df[df['c'] > 0]

    df = df.reset_index().drop('index', axis=1)

    if df.__len__() <= 10:
        print('ignore new stock which have no sufficient data ' + input_csv)
        return

    #calc daily increase
    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['increase'])
    df = new_value_df.join(df)  # the inserted colun on the head

    for i in range(1, df.__len__()):  #0: header, 1: frist data row
        last_row = df.iloc[i - 1]
        this_row = df.iloc[i]
        increase = 100 * (this_row['c'] - last_row['c']) / last_row['c']
        increase = round(increase, 8)
        df.iloc[i, df.columns.get_loc('increase')] = increase
        print("increase of the day " + str(this_row['date']) + " " + str(increase))
        pass

    df.to_csv(bv_csv, encoding='UTF-8', index=False)
    print("saved to " + bv_csv)

    return (df)


def verify(bv_csv, op, start_date=None, end_date=None, top_N=5, days_after_signal_to_buy=1, buy_and_hold_days=10):
    # days_after_signal_to_buy and buy_and_hold_days is used with catch*, not miss*.
    # e.g. with op CATCH_MAX_DECREASE, Buy after day of a big drop, then hold two week and sell.

    #bv_csv: buy and verify output csv, output of calc_daily_increase
    balance_init = 100000.0
    balance = balance_init
    profit = 0.0
    stock_count = 0.0

    if os.path.isfile(bv_csv):
        df = pd.read_csv(bv_csv, skiprows=1, converters={'c': float, 'date': str}, header=None, names=['increase', 'code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amt', 'tnv'])
    else:
        print('skip the verify of a stock, file not exist ' + bv_csv)
        return (None, None, None, None, None)

    if df.__len__() < 10:
        print("skip as the file has no sufficient date " + bv_csv)
        return (None, None, None, None, None)

    if start_date is None:
        start_date = df.iloc[0]['date']
    else:
        df = df[df['date'] >= start_date]

    if end_date is None:
        end_date = df.iloc[-1]['date']
    else:
        df = df[df['date'] <= end_date]

    df = df.reset_index().drop('index', axis=1)

    #buy at the 1st day
    code = df.iloc[0]['code']

    print("Buy at " + str(df.iloc[0]['date']) + " " + str(df.iloc[0]['c']))
    balance = balance * (1 - trade_cost)
    stock_count = balance / df.iloc[0]['c']

    #sort
    df_sorted = df.sort_values('increase', ascending=False, inplace=False)
    df_sorted = df_sorted[df_sorted['c'] > 0]

    df_top_n_increase = df_sorted[:top_N]
    df_top_n_increase = df_top_n_increase.sort_values('date', ascending=True, inplace=False)

    df_top_n_decrease = df_sorted[0 - top_N:]
    df_top_n_decrease = df_top_n_decrease.sort_values('date', ascending=True, inplace=False)

    df_top_n_indecrease = df_top_n_increase.append(df_top_n_decrease)
    df_top_n_indecrease = df_top_n_indecrease.sort_values('date', ascending=True, inplace=False)

    ######################
    # MISS Max Increase
    ######################
    if op == "MISS_MAX_INCREASE":
        print("\n===MISS Max Increase ===")

        for i in range(df_top_n_increase.__len__()):
            index_in_df = df_top_n_increase.iloc[i].name
            the_df = df.iloc[index_in_df]

            the_df_pre = df.iloc[index_in_df - 1]

            if index_in_df + 1 < df.__len__():  #not the latest row.
                the_df_aft = df.iloc[index_in_df + 1]

            sys.stdout.write(the_df['code'] + " " + the_df['date'] + " " + str(round(the_df['c'], 2)) + " IndayIncrease: " + str(round(the_df['increase'], 2)) + ".  ")

            #sell at the_df_pre
            if stock_count > 0:
                balance = stock_count * the_df_pre['c']  #using o or c?
                balance = balance * (1 - trade_cost)
                stock_count = 0.0
                profit = (balance - balance_init) / balance_init
                profit = round(profit, 2)
                logging.info(__file__+" "+"sell at " + str(the_df_pre['date']) + " " + str(the_df_pre['c']) + ". profit " + str(profit) + ". ")

            if balance > 0 and index_in_df + 1 < df.__len__():  #not the latest row.
                #buy at the_df_aft
                balance = balance * (1 - trade_cost)
                stock_count = balance / the_df_aft['c']
                balance = 0.0
                print("Buy at " + the_df_aft['date'] + " " + str(the_df_aft['c']))

    elif op == "MISS_MAX_DECREASE":
        ######################
        # MISS Max decrease
        ######################
        print("\n===MISS Max Decrease ===")

        for i in range(df_top_n_decrease.__len__()):
            index_in_df = df_top_n_decrease.iloc[i].name
            the_df = df.iloc[index_in_df]

            the_df_pre = df.iloc[index_in_df - 1]

            if index_in_df + 1 < df.__len__():  #not the latest row.
                the_df_aft = df.iloc[index_in_df + 1]

            sys.stdout.write(the_df['code'] + " " + the_df['date'] + " " + str(round(the_df['c'], 2)) + " IndayIncrease: " + str(round(the_df['increase'], 2)) + ".  ")

            #sell at the_df_pre
            if stock_count > 0:
                balance = stock_count * the_df_pre['c']  #using o or c?
                balance = balance * (1 - trade_cost)
                stock_count = 0.0
                profit = (balance - balance_init) / balance_init
                profit = round(profit, 2)
                logging.info(__file__+" "+"sell at " + str(the_df_pre['date']) + " " + str(the_df_pre['c']) + ". profit " + str(profit) + ". ")

            if balance > 0 and index_in_df + 1 < df.__len__():  #not the latest row.
                #buy at the_df_aft
                balance = balance * (1 - trade_cost)
                stock_count = balance / the_df_aft['c']
                balance = 0.0
                print("Buy at " + the_df_aft['date'] + " " + str(the_df_aft['c']))

    elif op == "MISS_MAX_INDECREASE":
        ######################
        # MISS Max decrease
        ######################
        print("\n===MISS Max Increase ad Decrease ===")

        for i in range(df_top_n_indecrease.__len__()):
            index_in_df = df_top_n_indecrease.iloc[i].name
            the_df = df.iloc[index_in_df]

            the_df_pre = df.iloc[index_in_df - 1]

            if index_in_df + 1 < df.__len__():  #not the latest row.
                the_df_aft = df.iloc[index_in_df + 1]

            sys.stdout.write(the_df['code'] + " " + the_df['date'] + " " + str(round(the_df['c'], 2)) + " IndayIncrease: " + str(round(the_df['increase'], 2)) + ".  ")

            #sell at the_df_pre
            if stock_count > 0:
                balance = stock_count * the_df_pre['c']  #using o or c?
                balance = balance * (1 - trade_cost)
                stock_count = 0.0
                profit = (balance - balance_init) / balance_init
                profit = round(profit, 2)
                logging.info(__file__+" "+"sell at " + str(the_df_pre['date']) + " " + str(the_df_pre['c']) + ". profit " + str(profit) + ". ")

            if balance > 0 and index_in_df + 1 < df.__len__():  #not the latest row.
                #buy at the_df_aft
                balance = balance * (1 - trade_cost)
                stock_count = balance / the_df_aft['c']
                balance = 0.0
                print("Buy at " + the_df_aft['date'] + " " + str(the_df_aft['c']))

    elif op == "CATCH_MAX_INCREASE":
        ######################
        # Catch MAX increase
        ######################
        print("\n===Catch Max Increase ===")
        balance = balance_init

        for i in range(df_top_n_increase.__len__()):
            index_in_df = df_top_n_increase.iloc[i].name
            the_df = df.iloc[index_in_df]

            #the_df_pre = df.iloc[index_in_df - 1 ]
            the_df_pre = df.iloc[index_in_df + days_after_signal_to_buy]

            if index_in_df + 1 < df.__len__():  #not the latest row.
                #the_df_aft = df.iloc[index_in_df + 1 ]
                the_df_aft = df.iloc[index_in_df + buy_and_hold_days]

            sys.stdout.write(the_df['code'] + " " + the_df['date'] + " " + str(round(the_df['c'], 2)) + " IndayIncrease: " + str(round(the_df['increase'], 2)) + ".  ")

            #Buy at the_df_pre
            if balance > 0:
                logging.info(__file__+" "+"buy at " + str(the_df_pre['date']) + " " + str(the_df_pre['c']) + ". ")
                balance = balance * (1 - trade_cost)
                stock_count = balance / the_df_pre['c']
                balance = 0

            #Sell at the df_aft
            if stock_count > 0 and index_in_df + 1 < df.__len__():  #not the latest row.
                balance = stock_count * the_df_aft['c']  # using o or c?
                balance = balance * (1 - trade_cost)
                stock_count = 0.0
                profit = (balance - balance_init) / balance_init
                profit = round(profit, 2)

                logging.info(__file__+" "+"sell at " + str(the_df_aft['date']) + " " + str(the_df_aft['c']) + ". profit " + str(profit) + ".\n")

    elif op == "CATCH_MAX_DECREASE":
        ######################
        # Catch MAX increase
        ######################
        print("\n===Catch Max Decrease ===")
        balance = balance_init

        for i in range(df_top_n_decrease.__len__()):
            index_in_df = df_top_n_decrease.iloc[i].name
            the_df = df.iloc[index_in_df]

            #the_df_pre = df.iloc[index_in_df - 1]
            the_df_pre = df.iloc[index_in_df + days_after_signal_to_buy]  #DEBUG

            if index_in_df + 1 < df.__len__():  # not the latest row.
                #the_df_aft = df.iloc[index_in_df + 1]
                the_df_aft = df.iloc[index_in_df + buy_and_hold_days]  #DEBUG

            sys.stdout.write(the_df['code'] + " " + the_df['date'] + " " + str(round(the_df['c'], 2)) + " IndayIncrease: " + str(round(the_df['increase'], 2)) + ".  ")

            # Buy at the_df_pre
            if balance > 0:
                logging.info(__file__+" "+"buy at " + str(the_df_pre['date']) + " " + str(the_df_pre['c']) + ". ")
                balance = balance * (1 - trade_cost)
                stock_count = balance / the_df_pre['c']
                balance = 0

            # Sell at the df_aft
            if stock_count > 0 and index_in_df + 1 < df.__len__():  # not the latest row.
                balance = stock_count * the_df_aft['c']  # using o or c?
                balance = balance * (1 - trade_cost)
                stock_count = 0.0
                profit = (balance - balance_init) / balance_init
                profit = round(profit, 2)

                logging.info(__file__+" "+"sell at " + str(the_df_aft['date']) + " " + str(the_df_aft['c']) + ". profit " + str(profit) + ".\n")

    if op in ["MISS_MAX_INCREASE", "MISS_MAX_DECREASE "]:
        #sell at the last day
        #print("sell at the latest day")
        balance = stock_count * df[-1:]['c'].values[0]
        balance = balance * (1 - trade_cost)

        stock_count = 0.0
        profit = (balance - balance_init) / balance_init
        profit = round(profit, 2)
        sys.stdout.write(code + " Result: sell at " + str(df[-1:]['date'].values[0]) + " " + str(df[-1:]['c'].values[0]) + ". profit " + str(profit) + ". ")

    #profit alway hold
    stock_count = (balance_init * (1 - trade_cost)) / df[:1]['c'].values[0]
    balance = stock_count * df[-1:]['c'].values[0]
    balance = balance * (1 - trade_cost)
    bhf_profit = (balance - balance_init) / balance_init
    bhf_profit = round(bhf_profit, 2)
    print("While Buy and Hold profit " + str(df[:1]['date'].values[0]) + " to " + str(df[-1:]['date'].values[0]) + " " + str(bhf_profit))

    return (code, start_date, end_date, profit, bhf_profit)


'''
    print("\n=== Max Decrease ===")
    df_top_n_decrease = df_sorted[0-top_N:]
    df_top_n_decrease = df_top_n_decrease.sort_values('date', ascending=True, inplace=False)


    for i in range(df_top_n_decrease.__len__()):
        index_in_df = df_top_n_decrease.iloc[i].name
        the_df_pre = df.iloc[index_in_df - 1 ]
        the_df_aft = df.iloc[index_in_df +   1 ]
        the_df = df.iloc[index_in_df]

        print(the_df['code'] + " " + the_df['date'] + " " + str(the_df['o']) + " " + str(the_df['increase']))

    pass
'''


def main():
    parser = OptionParser()

    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="debug mode")

    parser.add_option("-t", "--top_N", dest="top_n", default=5, type="int", help="N of most in/decrease of days")

    parser.add_option("-b", "--base_dir", dest="base_dir", type="str", help="dir contains daily bar csvs to be parsed")

    parser.add_option("-s", "--start_date", dest="start_date", type="str", help="Optional. Verify start date. In format YYYY-MM-DD")

    parser.add_option("-e", "--end_date", dest="end_date", type="str", help="Optional. Verify end date")

    parser.add_option("-p", "--days_after_signal_to_buy", dest="days_after_signal_to_buy", type="int", default=1, help="Optional. Using with CATCH operation.")

    parser.add_option("-q", "--buy_and_hold_days", dest="buy_and_hold_days", type="int", default=10, help="Optional. Using with CATCH operation.")

    parser.add_option("-o", "--operation", dest="operation", type="str", help="MISS_MAX_INCREASE|MISS_MAX_DECREASE|MISS_MAX_INDECREASE|CATCH_MAX_INCREASE|CATCH_MAX_DECREASE")

    (options, args) = parser.parse_args()
    debug = options.debug
    top_n_f = options.top_n
    base_dir_f = options.base_dir
    operation_f = options.operation
    start_date_f = options.start_date
    end_date_f = options.end_date

    days_after_signal_to_buy_f = options.days_after_signal_to_buy
    buy_and_hold_days_f = options.buy_and_hold_days

    print("debug: " + str(debug))
    print("top_n_f: " + str(top_n_f))
    print("base_dir: " + str(base_dir_f))
    print("operation: " + str(operation_f))
    print("start_date: " + str(start_date_f))
    print("end_date: " + str(end_date_f))
    print("days_after_signal_to_buy: " + str(days_after_signal_to_buy_f))
    print("buy_and_hold_days: " + str(buy_and_hold_days_f))

    files_path = os.listdir(base_dir_f)

    df_result = pd.DataFrame(columns=['code', 'strategy', 'start', 'end', 'prof', 'B_H_p'])
    i = 0

    for f in files_path:
        print("\n======" + f)

        if not re.match(".*\.csv$", f):
            print("ignore non csv " + f)
            continue

        bv_csv = "/home/ryan/DATA/tmp/verify_buy_and_hold/" + f

        files_abs_path = base_dir_f + "/" + f

        if not os.path.isfile(bv_csv):
            df = calc_daily_incease(input_csv=files_abs_path, bv_csv=bv_csv)

        (code, start_date, end_date, profit, bhf_profit) = verify(bv_csv=bv_csv, op=operation_f, start_date=start_date_f, end_date=end_date_f, days_after_signal_to_buy=days_after_signal_to_buy_f, buy_and_hold_days=buy_and_hold_days_f, top_N=top_n_f)

        if code is not None:
            df_result.loc[i] = [code, operation_f, start_date, end_date, profit, bhf_profit]
            i += 1

    result_csv = "~/DATA/result/b_a_h_verify_" + operation_f + "_" + str(top_n_f) + ".csv"
    df_result.to_csv(result_csv, encoding='UTF-8', index=False)
    print("result saved to " + result_csv)
    print('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
