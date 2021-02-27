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
import finlib_indicator

# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates

import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

from sklearn.cluster import KMeans
from optparse import OptionParser

global force_run_global


def my_round(i):
    if i < 0:
        return i

    if i < 10:
        i = round(i, 2)  # 0.005/1 == 0.5%  #14.444 --> 14.44, 14.446 --> 14.45,

    elif i < 100:
        i = round(i, 1)  # 0.05/10 == 0.5%  #14.44 --> 14.4,

    elif i < 1000:
        i = round(i, 0)  # 0.5/100  == 0.5%. #132.4 --> 132.0, 132.6 -->133.0

    elif i < 10000:
        i = round(i, -1)  # 5/1000 == 0.5%.  #3014.14 --> 3010.0, 3015.14 --> 3020.0
    else:
        i = round(i, -2)

    return (i)


def extract_key_point(df):
    # save all info to a tmp df
    df = pd.DataFrame([np.nan] * df.__len__(), columns=['close_round']).join(df)
    df = pd.DataFrame([np.nan] * df.__len__(), columns=['close_cnt']).join(df)
    df = pd.DataFrame(['unk'] * df.__len__(), columns=['high_or_low']).join(df)

    for i in range(df.__len__()):
        df.iloc[i, df.columns.get_loc('close_round')] = my_round(df.iloc[i]['close'])

    for i in range(df.__len__()):
        this_close = df.iloc[i]['close_round']
        cnt = df[df['close_round'] == this_close].__len__()
        df.iloc[i, df.columns.get_loc('close_cnt')] = cnt

    if True:  # Calc HL method 1
        check_range = 30
        half_check_range = int(check_range / 2)

        start = 0
        while True:
            end = start + check_range + 1

            if end >= df.__len__():
                break

            maxidx = df[start:end]['close_round'].idxmax()  # index of the max value
            minidx = df[start:end]['close_round'].idxmin()  # index of the min value

            # if (end - maxidx) > 0.2*(end -start) and (maxidx - start) > 0.2*(end -start): #not at the end border, hope it in the middle of the range.
            if (maxidx <= (end - 1)) and (maxidx >= start + 1):
                df.iloc[maxidx, df.columns.get_loc('high_or_low')] = 'H'

            if (minidx <= (end - 1)) and (minidx >= start + 1):
                df.iloc[minidx, df.columns.get_loc('high_or_low')] = 'L'

            start = end - 1

    if False:  # Calc HL method 2.
        check_range = 90
        half_check_range = int(check_range / 2)

        for i in range(check_range, df.__len__() - 1):
            max = df[i - check_range:i]['close_round'].max()
            min = df[i - check_range:i]['close_round'].min()

            # print("checking day "+df.iloc[i-half_check_range].date+" in range "+df.iloc[i-check_range].date+" ~ "+df.iloc[i].date)
            '''
            checking day 2018-11-02 in range 2018-08-21 ~ 2019-01-08
            checking day 2018-11-05 in range 2018-08-22 ~ 2019-01-09
            checking day 2018-11-06 in range 2018-08-23 ~ 2019-01-10
            '''

            if (df.iloc[i - half_check_range].close_round == max):
                df.iloc[i - half_check_range, df.columns.get_loc('high_or_low')] = 'H'
                i += half_check_range

            if (df.iloc[i - half_check_range].close_round == min):
                df.iloc[i - half_check_range, df.columns.get_loc('high_or_low')] = 'L'
                i += half_check_range

    high_df = df[df['high_or_low'] == 'H'].reset_index()[['code', 'name', 'date', 'close_round', "close_cnt", 'high_or_low']]
    low_df = df[df['high_or_low'] == 'L'].reset_index()[['code', 'name', 'date', 'close_round', "close_cnt", 'high_or_low']]

    df_hl = pd.DataFrame()

    for a in low_df['close_round'].unique():
        last_record = low_df[low_df['close_round'] == a].tail(1)
        df_hl = df_hl.append(last_record)

    for a in high_df['close_round'].unique():
        last_record = high_df[high_df['close_round'] == a].tail(1)
        df_hl = df_hl.append(last_record)

    return (df_hl)


def check_last_day(df_hl, df):
    df_hl = df_hl.sort_values(by='close_round').reset_index().drop('index', axis=1)
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['perc_to_up']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['perc_to_down']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['up_p']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['down_p']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['up_date']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['down_date']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['last_is_h_or_l']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['day_to_up']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['day_to_down']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['current_close']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['current_date']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['up_cnt']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['down_cnt']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['long_enter']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['long_quit']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['long_expect_ear_perct']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['delta_to_long_enter']))

    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['short_enter']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['short_quit']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['short_expect_ear_perct']))
    df_hl = df_hl.join(pd.DataFrame([np.nan] * df_hl.__len__(), columns=['delta_to_short_enter']))

    rtn_cols = ['code', 'name', 'current_date', 'current_close', 'delta_to_long_enter', 'long_enter', 'long_quit', 'long_expect_ear_perct', 'up_p', 'down_p', 'up_cnt', 'down_cnt', 'day_to_up', 'day_to_down', 'up_date', 'down_date', 'perc_to_up', 'perc_to_down', 'last_is_h_or_l', 'short_enter', 'short_quit', 'short_expect_ear_perct', 'delta_to_short_enter']

    if df_hl.__len__() < 2:
        df_hl = df_hl[rtn_cols]
        return (df_hl)

    df_hl.iloc[0, df_hl.columns.get_loc('current_date')] = df.iloc[-1].date
    df_hl.iloc[0, df_hl.columns.get_loc('current_close')] = round(df.iloc[-1].close, 2)

    for i in range(df_hl.__len__() - 1):
        last_close = df.iloc[-1].close
        last_date = df.iloc[-1].date

        if df_hl.iloc[i].close_round < last_close and last_close < df_hl.iloc[i + 1].close_round:
            current_bound_low = df_hl.iloc[i].close_round
            current_bound_high = df_hl.iloc[i + 1].close_round

            df_hl.iloc[i, df_hl.columns.get_loc('up_p')] = current_bound_high
            df_hl.iloc[i, df_hl.columns.get_loc('down_p')] = current_bound_low
            df_hl.iloc[i, df_hl.columns.get_loc('up_date')] = df_hl.iloc[i + 1].date
            df_hl.iloc[i, df_hl.columns.get_loc('down_date')] = df_hl.iloc[i].date

            df_hl.iloc[i, df_hl.columns.get_loc('up_cnt')] = df_hl.iloc[i + 1].close_cnt
            df_hl.iloc[i, df_hl.columns.get_loc('down_cnt')] = df_hl.iloc[i].close_cnt

            df_hl.iloc[i, df_hl.columns.get_loc('last_is_h_or_l')] = df_hl.sort_values(by='date').iloc[-1].high_or_low

            perc_to_up = round((current_bound_high - last_close) / last_close, 3) * 100
            perc_to_down = round((last_close - current_bound_low) / current_bound_low, 3) * 100

            df_hl.iloc[i, df_hl.columns.get_loc('current_close')] = round(last_close, 2)
            df_hl.iloc[i, df_hl.columns.get_loc('current_date')] = last_date

            df_hl.iloc[i, df_hl.columns.get_loc('perc_to_up')] = perc_to_up
            df_hl.iloc[i, df_hl.columns.get_loc('perc_to_down')] = perc_to_down

            up_date = df_hl.iloc[i + 1].date

            down_date = df_hl.iloc[i].date

            last_date = datetime.datetime.strptime(last_date, '%Y%m%d').date()
            up_date = datetime.datetime.strptime(str(up_date), '%Y%m%d').date()
            down_date = datetime.datetime.strptime(str(down_date), '%Y%m%d').date()

            df_hl.iloc[i, df_hl.columns.get_loc('day_to_up')] = (last_date - up_date).days
            df_hl.iloc[i, df_hl.columns.get_loc('day_to_down')] = (last_date - down_date).days

            break
    # df_hl = df_hl[df_hl['day_to_up']!='na'].reset_index().drop('index', axis=1)
    df_hl = df_hl[df_hl.day_to_up.notnull()].reset_index().drop('index', axis=1)

    for i in range(df_hl.__len__()):
        if df_hl.iloc[i].last_is_h_or_l == 'L':

            current_close = df_hl.iloc[i].current_close

            long_enter = round(df_hl.iloc[i].down_p * 1.06, 2)
            long_quit = round(df_hl.iloc[i].up_p * 0.97, 2)
            long_expect_ear_perct = 0

            if current_close > long_enter:
                long_expect_ear_perct = round(100 * (long_quit - current_close) / current_close)
            else:
                long_expect_ear_perct = round(100 * (long_quit - long_enter) / current_close)

            if long_expect_ear_perct <= 3:
                # the down_p and up_p quite near.
                return (pd.DataFrame())

            delta_to_long_enter = round(100 * (long_enter - current_close) / current_close, 1)

            print("long enter: " + str(long_enter) + " ,current " + str(current_close) + " ,long quit " + str(long_quit) + " ,long expect earn " + str(long_expect_ear_perct) + " ,delta " + str(delta_to_long_enter) + " .")
            df_hl.iloc[i, df_hl.columns.get_loc('long_enter')] = long_enter
            df_hl.iloc[i, df_hl.columns.get_loc('long_quit')] = long_quit
            df_hl.iloc[i, df_hl.columns.get_loc('long_expect_ear_perct')] = long_expect_ear_perct
            df_hl.iloc[i, df_hl.columns.get_loc('delta_to_long_enter')] = delta_to_long_enter
            pass

        if df_hl.iloc[i].last_is_h_or_l == 'H':
            current_close = df_hl.iloc[i].current_close

            short_enter = round(df_hl.iloc[i].up_p * 0.94, 2)
            short_quit = round(df_hl.iloc[i].down_p * 1.03, 2)
            short_expect_ear_perct = 0

            if current_close < short_enter:
                short_expect_ear_perct = round(100 * (current_close - short_quit) / current_close)
            else:
                short_expect_ear_perct = round(100 * (short_enter - short_quit) / current_close)

            if short_expect_ear_perct <= 3:
                # the down_p and up_p quite near.
                return (pd.DataFrame())

            delta_to_short_enter = round(100 * (current_close - short_enter) / current_close, 1)

            print("short enter: " + str(short_enter) + " ,current " + str(current_close) + " ,short quit " + str(short_quit) + " ,short expect earn " + str(short_expect_ear_perct) + " ,delta " + str(delta_to_short_enter) + " .")
            df_hl.iloc[i, df_hl.columns.get_loc('short_enter')] = short_enter
            df_hl.iloc[i, df_hl.columns.get_loc('short_quit')] = short_quit
            df_hl.iloc[i, df_hl.columns.get_loc('short_expect_ear_perct')] = short_expect_ear_perct
            df_hl.iloc[i, df_hl.columns.get_loc('delta_to_short_enter')] = delta_to_short_enter
            pass

    # df_hl = df_hl[['code', 'name', 'current_date','current_close','up_cnt','down_cnt','perc_to_up','perc_to_down','up_p','down_p','up_date','down_date','last_is_h_or_l','day_to_up','day_to_down','long_enter','long_quit','long_expect_ear_perct','delta_to_long_enter','short_enter','short_quit','short_expect_ear_perct','delta_to_short_enter']]
    # df_hl = df_hl[['code', 'name', 'current_date','current_close','delta_to_long_enter','long_enter','long_quit','long_expect_ear_perct','up_p','down_p','up_cnt','down_cnt','day_to_up','day_to_down','up_date','down_date','perc_to_up','perc_to_down','last_is_h_or_l','short_enter','short_quit','short_expect_ear_perct','delta_to_short_enter']]
    df_hl = df_hl[rtn_cols]

    return (df_hl)


def today_selection(inputF):
    if not finlib.Finlib().is_cached(file_path=inputF, day=30):
        logging.fatal("file not exist. or more than 30 days no changes. " + inputF)
        exit(0)

    df = pd.read_csv(inputF, converters={'code': str})
    df = df.fillna(0)

    df_long = df[df.long_expect_ear_perct.notnull()].reset_index().drop('index', axis=1)
    df_long = df_long[df_long['long_expect_ear_perct'] > 9].reset_index().drop('index', axis=1)
    # df_long = df_long[(df_long['long_expect_ear_perct'] +df_long['delta_to_long_enter']) > 9].reset_index().drop('index', axis=1)
    df_long = df_long[df_long['down_cnt'] > 1].reset_index().drop('index', axis=1)
    finlib.Finlib().pprint(df_long)

    df_short = df[df.short_expect_ear_perct.notnull()].reset_index().drop('index', axis=1)
    df_short = df_short[df_short['short_expect_ear_perct'] > 9].reset_index().drop('index', axis=1)
    # df_short = df_short[(df_short['short_expect_ear_perct'] + df_short['delta_to_short_enter']) > 9].reset_index().drop('index', axis=1)
    df_short = df_short[df_short['up_cnt'] > 1].reset_index().drop('index', axis=1)

    # df_rtn = df_long.append(df_short)  #df_short no meaning for A stock. long only.
    df_rtn = df_long
    df_rtn = df_rtn.sort_values(by=['down_cnt', 'up_cnt'], ascending=[False, False]).reset_index().drop('index', axis=1)

    # df_rtn.iloc[0]
    return (df_rtn)


def main():
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(A G)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("-b", "--calc_base", action="store_true", dest="calc_base", default=False, help="Step1. generate base High Low point for each stock, time consuming.")

    parser.add_option("-t", "--calc_today", action="store_true", dest="calc_today", default=False, help="Step2. find current price vs HL.")

    parser.add_option("-s", "--today_selection", action="store_true", dest="today_selection", default=False, help="Step3. selection.")

    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="enable debug, use in development purpose")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")
    parser.add_option("--print_selected_keyprice_based_on_price_volume", action="store_true", dest="print_selected_keyprice_based_on_price_volume", default=False, help="calculate then print key price based on last N days price and volume.")

    (options, args) = parser.parse_args()

    stock_global = options.stock_global
    selected = options.selected

    calc_base = options.calc_base
    calc_today = options.calc_today
    today_selection_f = options.today_selection
    debug = options.debug
    force_run_f = options.force_run_f
    print_selected_keyprice_based_on_price_volume = options.print_selected_keyprice_based_on_price_volume

    print("stock_global: " + str(stock_global))
    print("calc_base: " + str(calc_base))
    print("calc_today: " + str(calc_today))
    print("today_selection_f: " + str(today_selection_f))
    print("debug: " + str(debug))
    print("force_run_f: " + str(force_run_f))
    print("print_selected_keyprice_based_on_price_volume: " + str(print_selected_keyprice_based_on_price_volume))

    global force_run_global
    force_run_global = False
    if force_run_f:
        force_run_global = True

    if stock_global is None:
        print("-x --stock_global is None, check help for available options, program exit")
        exit(0)

    if (not calc_base) and (not calc_today) and (not today_selection):
        print("have to specify at least one action, calc_base or calc_today, program exit")
        exit(0)

    if (calc_base) and (calc_today):
        print("only one action, calc_base or calc_today, not the both, program exit")
        exit(0)

    if selected:
        output_dir_root = "/home/ryan/DATA/result/selected"
    else:
        output_dir_root = "/home/ryan/DATA/result"

    outputF = output_dir_root + "/key_points_" + stock_global.lower() + ".csv"
    outputF_today = output_dir_root + "/key_points_" + stock_global.lower() + "_today.csv"
    outputF_today_s = output_dir_root + "/key_points_" + stock_global.lower() + "_today_selected.csv"

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


    if print_selected_keyprice_based_on_price_volume:
        for index, row in stock_list.iterrows():
            data_csv = csv_dir + '/' + str(row['code']).upper() + '.csv'
            rtn = finlib_indicator.Finlib_indicator().print_support_price_by_price_volume(data_csv=data_csv)
        exit(0)

    if debug:
        files = ['SH000001.csv']
        # files = ['SZ300492.csv']
        files = ['SZ000029.csv']

    if calc_today:
        df_hl = pd.read_csv(outputF)

    if today_selection_f:
        df_rtn = today_selection(outputF_today)
        df_rtn.to_csv(outputF_today_s, encoding='UTF-8', index=False)
        finlib.Finlib().pprint(df_rtn)
        logging.info(__file__+" "+"output saved to " + outputF_today_s + "\n")
        exit(0)

    if calc_base or calc_today:

        if calc_base and (not force_run_global) and finlib.Finlib().is_cached(outputF, 7):
            print("file already update in 7 days, not calculate again. " + outputF)
            exit(0)

        j = 1
        looplen = str(stock_list.__len__())

        tmp_dir = "/home/ryan/DATA/tmp/support_resistent"

        if not os.path.isdir(tmp_dir):
            os.mkdir(tmp_dir)

        for code in stock_list['code']:
            inputF = root_dir + "/" + code + ".csv"

            logging.info(__file__+"  "+str(j) + " of " + looplen + ". " + inputF + " . ")
            j += 1

            if not os.path.exists(inputF):
                print("file not exist, " + inputF)
                continue

            df = finlib.Finlib().regular_read_csv_to_stdard_df(inputF)

            if df.__len__() < 100:
                continue

            # adding name column
            df = pd.merge(df, stock_list, on='code', how='inner', suffixes=('', '_x'))

            code = df.iloc[0].code

            if calc_base:
                tmp_base_f = tmp_dir + "/" + str(code) + ".csv"

                if (not force_run_global) and finlib.Finlib().is_cached(tmp_base_f, 1):
                    df_tmp = pd.read_csv(tmp_base_f)
                else:
                    df = df[df['close'] > 0]
                    # df = df.tail(1000).reset_index().drop('index', axis=1)
                    df = df.reset_index().drop('index', axis=1)
                    df_tmp = extract_key_point(df)

                    if df_tmp.__len__() > 2:
                        df_tmp.to_csv(tmp_base_f, encoding='UTF-8', index=False)

                df_rtn = df_rtn.append(df_tmp).reset_index().drop('index', axis=1)
                # df_rtn.to_csv(outputF, encoding='UTF-8', index=False)
                # logging.info(__file__+" "+"output saved to "+outputF+"\n")

            if calc_today:
                df_tmp = check_last_day(df_hl[df_hl['code'] == code], df)

                if df_tmp.__len__() > 0:
                    df_rtn = df_rtn.append(df_tmp).reset_index().drop('index', axis=1)

                # df_rtn.to_csv(outputF_today, encoding='UTF-8', index=False)
                # logging.info(__file__+" "+"output saved to "+outputF_today+"\n")

        if calc_base:
            df_rtn.to_csv(outputF, encoding='UTF-8', index=False)
            logging.info(__file__+" "+"output saved to " + outputF + "\n")

        if calc_today:
            # L then H,  down_cnt more then less, long_expect_ear more than less.
            if df_rtn.__len__() == 0:
                logging.info(__file__+" "+"empty df for calc_today, stop.")
                exit(0)

            df_rtn = df_rtn.sort_values(by=['last_is_h_or_l', 'down_cnt', 'long_expect_ear_perct'], ascending=[False, False, False])

            df_rtn.to_csv(outputF_today, encoding='UTF-8', index=False)
            finlib.Finlib().pprint(df_rtn)
            logging.info(__file__+" "+"output saved to " + outputF_today + "\n")

    # print df.head(1)


### MAIN ####
if __name__ == '__main__':
    main()

    print('script completed')
