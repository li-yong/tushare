# coding: utf-8
import finlib
import pdb
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
import datetime
import traceback
import sys

from optparse import OptionParser
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

import os
import shutil
import tabulate

global debug_global
global force_run_global
global myToken


def set_global(debug=False, force_run=False):
    global debug_global
    global force_run_global
    global myToken

    ### Global Variables ####
    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'

    debug_global = False
    force_run_global = False

    if force_run:
        force_run_global = True

    if debug:
        debug_global = True


def fetch_hsgt_top_10():
    # 获取沪股通、深股通每日前十大成交详细数据
    dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow"
    input_output_csv =dir+ "/"+"hsgt_top10.csv"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    ts.set_token(myToken)
    pro = ts.pro_api()

    if os.path.exists(input_output_csv):
        df = pd.read_csv(input_output_csv, converters={'trade_date': str})
    else:
        df = pd.DataFrame(columns=['trade_date'])

    fetching_days = 30

    for i in range(fetching_days, -1, -1):  # i from 30, 29, 28... 0

        trade_date = datetime.datetime.today() - datetime.timedelta(i)
        trade_date_s = (datetime.datetime.today() - datetime.timedelta(i)).strftime('%Y%m%d')

        if trade_date.isoweekday() in [6, 7] and (not force_run_global):
            print("skip, weekend " + trade_date_s)
            continue

        if (not df[df['trade_date'] == trade_date_s].empty) and (not force_run_global):
            print("skip, already have records on " + trade_date_s)
            continue

        logging.info(__file__+" "+"fetching hsgt_top 10 " + trade_date_s)

        df_h = pro.hsgt_top10(trade_date=trade_date_s, market_type='1')
        logging.info(__file__+" "+", get len " + str(df_h.__len__()) + "\n")
        df = df.append(df_h)

    cols = ['trade_date', 'ts_code', 'name', 'close', 'net_amount', 'amount', 'buy', 'change', 'market_type', 'rank', 'sell']

    df = df[cols]
    df = df.sort_values(['trade_date', 'net_amount', 'rank'], ascending=[True, False, True], inplace=False)

    df.to_csv(input_output_csv, encoding='UTF-8', index=False)
    print("hsgt_top_10 saved to " + input_output_csv)


def analyze_hsgt_top_10():
    input_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv"
    output_csv = "/home/ryan/DATA/result/hsgt_top_10_selected.csv"

    if os.path.exists(input_csv):
        df = pd.read_csv(input_csv, converters={'trade_date': str, 'ts_code': str})
        #df = pd.read_csv(input_csv)
    else:
        logging.info(__file__+" "+"abort, no such file " + input_csv)
        return ()

    #recent 10 days most source in money stocks

    period_days = 10  #two weeks
    #period_days = 5   #two weeks
    period_days = 3  #two weeks

    df = df.tail(n=period_days * 10)  #assume 10 records per day
    #print(df.tail(40))
    #print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))

    df = df.groupby(['name', 'ts_code'])['net_amount'].sum().sort_values().reset_index()
    df = df.sort_values(by=['net_amount'], ascending=[False])
    df = df[df['net_amount'] > 0].reset_index().drop('index', axis=1)
    df = finlib.Finlib().ts_code_to_code(df)

    df.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info(finlib.Finlib().pprint(df))
    logging.info("Based on recent " + str(period_days) + " days selected hsgt_top_10 was saved to " + output_csv)


def fetch_moneyflow(fetch_whole=False, fetch_today=True, force_run=False,debug=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # SH600519

    if debug:
        stock_list = stock_list[stock_list['code']=='SH600519']


    i = 0
    csv_dir = "/home/ryan/DATA/DAY_Global/AG_MoneyFlow"
    if not os.path.isdir(csv_dir):
        os.mkdir(csv_dir)

    if fetch_today:
        df_today = pro.moneyflow(trade_date=finlib.Finlib().get_last_trading_day())
        df_today = finlib.Finlib().ts_code_to_code(df_today)

    for index, row in stock_list.iterrows():
        i += 1
        #print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        logging.info(__file__ + " " + str(i) + " of " + str(stock_list.__len__()))
        name, code = row['name'], row['code']

        # SH600519 --> 600519.SH
        ts_code = finlib.Finlib().add_market_to_code(pd.DataFrame([code], columns=['code']), dot_f=True, tspro_format=True).iloc[0]['code']

        csv_f = csv_dir + "/" + code + ".csv"
        if fetch_today:
            df_tmp = df_today[df_today['code'] == code]

            if os.path.exists(csv_f):
                df_old = pd.read_csv(csv_f, converters={'trade_date': str})
                df_new = pd.concat([df_old, df_tmp], sort=False).drop_duplicates().reset_index().drop('index', axis=1)

            else:
                df_new = df_tmp
            df_new.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info(__file__ + " " + "saved to " + csv_f + " len " + str(df_new.__len__()))

        elif fetch_whole :
            if finlib.Finlib().is_cached(csv_f, 3) and not force_run:
                logging.info(__file__+" "+"file has been updated in 3 days. "+csv_f)
                continue
            df = pro.moneyflow(ts_code=ts_code)
            time.sleep(0.5)
            #df = pro.moneyflow(ts_code=code, start_date = '20200101', end_date = trade_day)
            #ts_code = '002149.SZ', start_date = '20190115', end_date = '20190315
            df = finlib.Finlib().ts_code_to_code(df)
            df = df.reindex(index=df.index[::-1])
            df.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info(__file__ + " " + "len " + str(df.__len__()) + " " + csv_f)

    #df_4 = pro.moneyflow(trade_date=trade_day)
    #df_4 = df_4.sort_values('net_mf_amount', ascending=False, inplace=False)
    #print(tabulate.tabulate(df_4, headers='keys', tablefmt='psql'))

#mf_ana_date: moneyflow analyze date
def describe_std(code, name, df_sub, col_name, mf_ana_date, force_print=False):
    td = df_sub.iloc[-1:]['trade_date'].values[0]
    df_description = df_sub[col_name].describe()
    std = df_description['std']
    mean = df_description['mean']
    current_v = df_sub.iloc[-1:][col_name].values[0]
    sig = round((current_v - mean) / std, 1)

    rst = {
        'col_name': col_name,
        'des': df_description,
        'sig': sig,
        'date': td,
        'col_v': current_v,
        'hit': False,
        'hit_today': False,
    }

    threshold = 3  # 3 sigma, 99.8% chance
    if sig > threshold or (-1 * sig) > threshold or force_print:
        rst['hit'] = True
        #print(
        #    code + " " + name + " " + str(td) + " sig " + str(sig) + " " + col_name + " " + str(current_v))
        #print("last 10 day " +col_name+" " + str(df_sub[col_name][-10:].describe()['std']))
        #print("-30:-10 day " +col_name+" " + str(df_sub[col_name][-30:-10].describe()['std']))

        if str(mf_ana_date) == str(td):
            rst['hit_today'] = True
        #    print("TODAY HITTED. "+ code + " " + name + " " + str(td) + " sig " + str(sig) + " " + col_name + " " + str(current_v))
        pass

    return (rst)


def analyze_moneyflow(mf_ana_date, mf_ana_pre_days=3, mf_ana_test_hold_days=5, prime_stock_list=True, selected=True, debug=False):
    ts.set_token(myToken)

    if selected:
        rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global='AG')
        stock_list = rst['stock_list']

    elif prime_stock_list:
        stock_list = finlib.Finlib().prime_stock_list()
    else:  #all stocks
        stock_list = finlib.Finlib().get_A_stock_instrment()
        stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # SH600519
        stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')

    if debug:
        stock_list = finlib.Finlib().get_stock_configuration(selected=selected, stock_global='AG')['stock_list']
        stock_list = stock_list[stock_list['code']=='SH600519']


    i = 0
    csv_in_dir = "/home/ryan/DATA/DAY_Global/AG_MoneyFlow"
    csv_out_dir = "/home/ryan/DATA/tmp/moneyflow_ana"

    csv_out_today = "/home/ryan/DATA/result/today/money_flow.csv"
    csv_out_history = csv_out_dir + "/history_moneyflow_hit.csv"

    # all stocks comparation of today. To find out max(bv1/bv0) stock etc.
    csv_out_today_snap = csv_out_dir + "/" + "mf_today_snap.csv"
    if selected:
        csv_out_today_snap =  csv_out_dir + "/" + "mf_today_snap_selected.csv"
        csv_out_today_selected =  "/home/ryan/DATA/result/selected/mf_today_top5_large_amount.csv"
    else:
        csv_out_today_snap = csv_out_dir + "/" + "mf_today_snap.csv"
        csv_out_today_selected =  "/home/ryan/DATA/result/today/mf_today_top5_large_amount.csv"

    if os.path.exists(csv_out_today):
        os.remove(csv_out_today)

    if os.path.exists(csv_out_history):
        os.remove(csv_out_history)

    if os.path.isdir(csv_out_dir):
        shutil.rmtree(csv_out_dir)

    os.mkdir(csv_out_dir)

    df_history = pd.DataFrame(columns=['code', 'name', 'date', 'operation', 'strength', 'reason'])
    df_today = pd.DataFrame(columns=['code', 'name', 'date', 'operation', 'strength', 'reason'])
    #csv_out_today = csv_out_dir + "/today_moneyflow_hit.csv"

    df_today_snap = pd.DataFrame()

    for index, row in stock_list.iterrows():
        i += 1
        logging.info(__file__ + " " + str(i) + " of " + str(stock_list.__len__()))
        name, code = row['name'], row['code']

        csv_in = csv_in_dir + "/" + code + ".csv"



        if os.path.exists(csv_in):
            df = pd.read_csv(csv_in)
            #analysis start
            # b: buy, s:sell, v:volume, m:amount
            # 0: all, 1:extr large, 2:large, 3:middle, 4:small

            if df.__len__() <= 120:
                logging.info("insufficient days' data, skip. "+str(df.__len__())+ " "+csv_in)
                continue

            df = pd.DataFrame([name] * df.__len__(), columns=['name']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['bv0']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['sv0']).join(df)  # the inserted column on the head
            df['bv0'] = df['buy_elg_vol'] + df['buy_lg_vol'] + df['buy_md_vol'] + df['buy_sm_vol']
            df['sv0'] = df['sell_elg_vol'] + df['sell_lg_vol'] + df['sell_md_vol'] + df['sell_sm_vol']

            df = pd.DataFrame([0] * df.__len__(), columns=['bm0']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['sm0']).join(df)  # the inserted column on the head
            df['bm0'] = df['buy_elg_amount'] + df['buy_lg_amount'] + df['buy_md_amount'] + df['buy_sm_amount']
            df['sm0'] = df['sell_elg_amount'] + df['sell_lg_amount'] + df['sell_md_amount'] + df['sell_sm_amount']

            df = pd.DataFrame([0] * df.__len__(), columns=['bv1/bv0']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['sv1/sv0']).join(df)  # the inserted column on the head
            df['bv1/bv0'] = df['buy_elg_vol'] / df['bv0']
            df['sv1/sv0'] = df['sell_elg_vol'] / df['sv0']

            df = pd.DataFrame([0] * df.__len__(), columns=['(bv1-sv1)/bv0']).join(df)  # the inserted column on the head
            df['(bv1-sv1)/bv0'] = (df['buy_elg_vol'] - df['sell_elg_vol']) / df['bv0']

            df = pd.DataFrame([0] * df.__len__(), columns=['(bm1-sm1)/bm0']).join(df)  # the inserted column on the head
            df['(bm1-sm1)/bm0'] = (df['buy_elg_amount'] - df['sell_elg_amount']) / df['bm0']

            df = pd.DataFrame([0] * df.__len__(), columns=['(bv1+bv2)/bv0']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['(sv1+sv2)/sv0']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['(bv1+bv2+sv1+sv2)/(bv0+sv0)']).join(df)  # the inserted column on the head
            df = pd.DataFrame([0] * df.__len__(), columns=['(bm1+bm2+sm1+sm2)/(bm0+sm0)']).join(df)  # the inserted column on the head
            df['(bv1+bv2)/bv0'] = (df['buy_elg_vol'] + df['buy_lg_vol']) / df['bv0']
            df['(sv1+sv2)/sv0'] = (df['sell_elg_vol'] + df['sell_lg_vol']) / df['sv0']
            df['(bv1+bv2+sv1+sv2)/(bv0+sv0)'] = (df['buy_elg_vol'] + df['buy_lg_vol']+df['sell_elg_vol'] + df['sell_lg_vol']) / ( df['bv0']+df['sv0'])
            df['(bm1+bm2+sm1+sm2)/(bm0+sm0)'] = (df['buy_elg_amount'] + df['buy_lg_amount']+df['sell_elg_amount'] + df['sell_lg_amount']) / ( df['bm0']+df['sm0'])

            df_today_snap = df_today_snap.append(df.iloc[-1:].reset_index().drop('index', axis=1))

            pre_days = mf_ana_pre_days  #analysis last 100 rows
            for i2 in range(df.__len__() - pre_days, df.__len__()):
                df_sub = df[i2 - 120:i2]  #compare with previous one year data for each row

                target = '(bv1+bv2)/bv0'
                rst = describe_std(code, name, df_sub, col_name=target, mf_ana_date=mf_ana_date, force_print=False)

                if rst['hit']:
                    reason = rst['col_name'] + " outstanding std"
                    in_date = datetime.datetime.strptime(str(rst['date']), '%Y%m%d')
                    out_date = in_date + datetime.timedelta(mf_ana_test_hold_days)
                    today = datetime.datetime.today()

                    if today < out_date:
                        out_date = today

                    p_in = finlib.Finlib().get_price(code_m=code, date=in_date.strftime('%Y-%m-%d'))
                    p_out = finlib.Finlib().get_price(code_m=code, date=out_date.strftime('%Y-%m-%d'))

                    profit = round((p_out - p_in) / p_in * 100, 1)

                    if rst['sig'] > 0:
                        logging.info(__file__ + " " + "buy, hold short " + code + " " + name + " " + str(rst['date']) + " sig " + str(rst['sig']))
                        operation = "buy, hold short"

                    elif rst['sig'] < 0:
                        logging.info(__file__ + " " + "buy, hold long  " + code + " " + name + " " + str(rst['date']) + " sig " + str(rst['sig']))
                        operation = "buy, hold long"

                    df_history = df_history.append({
                        'code': code,
                        'name': name,
                        'date': str(rst['date']),
                        'operation': operation,
                        'strength': rst['sig'],
                        'reason': reason,
                        'date_in': in_date.strftime('%Y-%m-%d'),
                        'date_out': out_date.strftime('%Y-%m-%d'),
                        'p_in': p_in,
                        'p_out': p_out,
                        'profit': profit,
                    }, ignore_index=True)

                    df_history.to_csv(csv_out_history, encoding='UTF-8', index=False)
                    logging.info(__file__ + " " + "hit saved to " + csv_out_history)
                    #pdb.set_trace()

                    rst = describe_std(row['code'], row['name'], df_sub, col_name='(sv1+sv2)/sv0', mf_ana_date=mf_ana_date, force_print=True)
                    rst = describe_std(row['code'], row['name'], df_sub, col_name='bv1/bv0', mf_ana_date=mf_ana_date, force_print=True)
                    rst = describe_std(row['code'], row['name'], df_sub, col_name='(bv1-sv1)/bv0', mf_ana_date=mf_ana_date, force_print=True)
                    rst = describe_std(row['code'], row['name'], df_sub, col_name='(bm1-sm1)/bm0', mf_ana_date=mf_ana_date, force_print=True)

                    if rst['hit_today']:
                        logging.info(__file__ + " " + "today is hit")
                        df_today = df_today.append({'code': code, 'name': name, 'date': str(rst['date']), 'operation': operation, 'strength': rst['sig'], 'reason': reason}, ignore_index=True)
                        df_today.to_csv(csv_out_today, encoding='UTF-8', index=False)
                        logging.info(__file__ + " " + "Today hit saved to " + csv_out_today)

        else:
            logging.warning(__file__ + " " + "no such file " + csv_in)
            continue

    df_today_snap = df_today_snap.sort_values('(bm1+bm2+sm1+sm2)/(bm0+sm0)', ascending=False, inplace=False)
    df_today_snap = finlib.Finlib().adjust_column(df = df_today_snap, col_name_list=['code','name','trade_date'])
    df_today_snap.to_csv(csv_out_today_snap)
    logging.info(__file__ + " " + "saved today_snap to " + csv_out_today_snap + " Len " + str(df_today_snap.__len__()))

    df_today_snap = df_today_snap.head(5)
    df_today_snap = df_today_snap.rename(columns={"trade_date": "date"}, inplace=False)

    df_today_snap.to_csv(csv_out_today_selected)
    logging.info(__file__+" "+"saved today_max_amount to "+csv_out_today_selected+" Len "+str(df_today_snap.__len__()))

    if 'profit' in df_history.columns:
        logging.info(__file__ + " " + "history profit describe:")
        logging.info(__file__ + " " + str(df_history['profit'].describe()))

    logging.info(__file__ + " " + "today hit account, len " + str(df_today.__len__()) + " " + csv_out_today)


### MAIN ####
if __name__ == '__main__':

    logging.info(__file__+" "+"\n")
    logging.info(__file__ + " " + "SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-u", "--debug", action="store_true", dest="debug_f", default=False, help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--fetch_moneyflow_all", action="store_true", dest="fetch_moneyflow_all_f", default=False, help="fetch moneyflow full history for stocks, by pro.moneyflow(ts_code) ")

    parser.add_option("--fetch_hsgt_top_10", action="store_true", dest="fetch_hsgt_top_10_f", default=False, help="fetch hsgt ")

    parser.add_option("--fetch_moneyflow_daily", action="store_true", dest="fetch_moneyflow_daily_f", default=False, help="fetch today moneyflow and update to all stocks, by pro.moneyflow(trade_date) )")

    parser.add_option("--analyze_hsgt", action="store_true", dest="analyze_hsgt_f", default=False, help="analyze hsgt ")

    parser.add_option("--analyze_moneyflow", action="store_true", dest="analyze_moneyflow_f", default=False, help="analyze moneyflow ")

    parser.add_option("--mf_ana_date", type="str", help="moneyflow analyze date. The date (YYYYMMDD) to be analyzed moneyflow ")

    parser.add_option("--mf_ana_pre_days", type="int", default=3, dest="mf_ana_pre_days", help="analyze moneyflow, check from how many days before today")

    parser.add_option("--mf_ana_test_hold_days", type="int", default=5, dest="mf_ana_test_hold_days", help="analyze moneyflow, test profit of holding n days. 5 had a good result in few tests.")

    parser.add_option("--mf_ana_prime_stock", action="store_true", default=False, dest="mf_ana_prime_stock", help="analyze moneyflow, analyze prime stocks only.")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

    (options, args) = parser.parse_args()
    fetch_hsgt_top_10_f = options.fetch_hsgt_top_10_f

    fetch_moneyflow_all_f = options.fetch_moneyflow_all_f
    fetch_moneyflow_daily_f = options.fetch_moneyflow_daily_f
    analyze_moneyflow_f = options.analyze_moneyflow_f
    analyze_hsgt_f = options.analyze_hsgt_f
    force_run_f = options.force_run_f
    debug_f = options.debug_f
    mf_ana_pre_days = options.mf_ana_pre_days
    mf_ana_test_hold_days = options.mf_ana_test_hold_days
    mf_ana_prime_stock = options.mf_ana_prime_stock
    mf_ana_date = options.mf_ana_date
    selected = options.selected

    if mf_ana_date == None:
        mf_ana_date = finlib.Finlib().get_last_trading_day()

    logging.info(__file__+" "+"fetch_moneyflow_all_f: " + str(fetch_moneyflow_all_f))
    logging.info(__file__+" "+"fetch_hsgt_top_10_f: " + str(fetch_hsgt_top_10_f))
    logging.info(__file__+" "+"fetch_moneyflow_daily_f: " + str(fetch_moneyflow_daily_f))
    logging.info(__file__+" "+"analyze_moneyflow_f: " + str(analyze_moneyflow_f))
    logging.info(__file__+" "+"analyze_hsgt_f: " + str(analyze_hsgt_f))
    logging.info(__file__+" "+"mf_ana_pre_days: " + str(mf_ana_pre_days))
    logging.info(__file__+" "+"mf_ana_test_hold_days: " + str(mf_ana_test_hold_days))
    logging.info(__file__+" "+"mf_ana_prime_stock: " + str(mf_ana_prime_stock))
    logging.info(__file__+" "+"mf_ana_date: " + str(mf_ana_date))
    logging.info(__file__+" "+"debug_f: " + str(debug_f))
    logging.info(__file__+" "+"selected: " + str(selected))

    set_global(debug=debug_f, force_run=force_run_f)

    if fetch_moneyflow_all_f:
        fetch_moneyflow(fetch_whole=True, fetch_today=False, force_run=force_run_f, debug=debug_f)
    elif fetch_hsgt_top_10_f:
        fetch_hsgt_top_10()
    elif fetch_moneyflow_daily_f:
        fetch_moneyflow(fetch_whole=False, fetch_today=True, force_run=force_run_f, debug=debug_f)
    elif analyze_hsgt_f:
        analyze_hsgt_top_10()
    elif analyze_moneyflow_f:
        analyze_moneyflow(
            mf_ana_date=mf_ana_date,
            mf_ana_pre_days=mf_ana_pre_days,
            mf_ana_test_hold_days=mf_ana_test_hold_days,
            prime_stock_list=mf_ana_prime_stock,
            selected = selected,
            debug = debug_f,
        )
