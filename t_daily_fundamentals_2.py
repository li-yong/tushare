# coding: utf-8

import sys, traceback, threading
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np

# import matplotlib.pyplot as plt
import pandas
import math
import re
from scipy import stats
import finlib
import datetime
from optparse import OptionParser
import sys
import os
import logging
import signal

logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m_%d %H:%M:%S", level=logging.DEBUG)

# from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
USD_DIV_CNY = 7

global debug_global
global force_run_global
global myToken
global fund_base
global fund_base_source
global fund_base_merged
global fund_base_report
global fund_base_tmp

global csv_income
global csv_balancesheet
global csv_cashflow
global csv_forecast
global csv_dividend
global csv_express
global csv_fina_indicator
global csv_fina_audit

global csv_fina_mainbz_p
global csv_fina_mainbz_d

global csv_fina_mainbz_sum
global csv_disclosure_date
global csv_basic

global fund_base_latest

global csv_income_latest
global csv_balancesheet_latest
global csv_cashflow_latest
global csv_forecast_latest
global csv_express_latest
global csv_dividend_latest
global csv_fina_indicator_latest
global csv_fina_audit_latest

global csv_fina_mainbz_p_latest
global csv_fina_mainbz_d_latest
global csv_fina_mainbz_sum_latest
global csv_disclosure_date_latest
global csv_disclosure_date_latest_notify
global csv_fina_mainbz_latest_percent

global col_list_income

global col_list_balancesheet

global col_list_cashflow
global col_list_forecast
global col_list_dividend
global col_list_express

global col_list_fina_indicator
global col_list_fina_audit
global col_list_fina_mainbz
global col_list_disclosure_date

global query_fields_income
global query_fields_balancesheet
global query_fields_cashflow
global query_fields_fina_indicator
global query_fields_forecast
global query_fields_dividend
global query_fields_express
global query_fields_fina_audit
global query_fields_fina_mainbz
global query_fields_disclosure_date

# pd.set_option('display.height', 1000)
# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)

global df_all_ts_pro
global df_all_jaqs
global big_memory_global


def set_global(debug=False, big_memory=False, force_run=False):
    global debug_global
    global force_run_global
    global myToken
    global fund_base
    global fund_base_source
    global fund_base_merged
    global fund_base_report
    global fund_base_tmp

    global csv_income
    global csv_balancesheet
    global csv_cashflow
    global csv_forecast
    global csv_dividend
    global csv_express
    global csv_fina_indicator
    global csv_fina_audit
    global csv_fina_mainbz_p
    global csv_fina_mainbz_d

    global csv_fina_mainbz_sum
    global csv_disclosure_date
    global csv_basic

    global fund_base_latest

    global csv_income_latest
    global csv_balancesheet_latest
    global csv_cashflow_latest
    global csv_forecast_latest
    global csv_express_latest
    global csv_dividend_latest
    global csv_fina_indicator_latest
    global csv_fina_audit_latest

    global csv_fina_mainbz_d_latest
    global csv_fina_mainbz_p_latest
    global csv_fina_mainbz_sum_latest
    global csv_fina_mainbz_latest_percent
    global csv_disclosure_date_latest
    global csv_disclosure_date_latest_notify

    global col_list_income

    global col_list_balancesheet

    global col_list_cashflow
    global col_list_forecast
    global col_list_dividend
    global col_list_express

    global col_list_fina_indicator
    global col_list_fina_audit
    global col_list_fina_mainbz
    global col_list_disclosure_date
    global df_all_ts_pro
    global df_all_jaqs
    global big_memory_global

    ### Global Variables ####

    myToken = "4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b"
    fund_base = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"

    fund_base_source = fund_base + "/source"
    fund_base_merged = fund_base + "/merged"
    fund_base_report = fund_base + "/report"
    fund_base_tmp = fund_base + "/tmp"
    fund_base_tmp = fund_base + "/tmp"

    debug_global = False
    force_run_global = False
    big_memory_global = False

    if force_run:
        force_run_global = True

    if debug:
        debug_global = True
        fund_base_source = fund_base + "/source.dev"
        fund_base_merged = fund_base + "/merged.dev"  # modify global fund_base_merged to dev
        fund_base_report = fund_base + "/report.dev"

    if big_memory:
        big_memory_global = True
        df_all_jaqs = finlib.Finlib().load_all_jaqs()
        df_all_ts_pro = finlib.Finlib().load_all_ts_pro(debug=debug, overwrite=True)
    else:
        df_all_jaqs = None
        df_all_ts_pro = None

    csv_income = fund_base_source + "/income.csv"
    csv_balancesheet = fund_base_source + "/balancesheet.csv"
    csv_cashflow = fund_base_source + "/cashflow.csv"
    csv_forecast = fund_base_source + "/forecast.csv"
    csv_dividend = fund_base_source + "/dividend.csv"
    csv_express = fund_base_source + "/express.csv"
    csv_fina_indicator = fund_base_source + "/fina_indicator.csv"
    csv_fina_audit = fund_base_source + "/fina_audit.csv"
    csv_fina_mainbz_p = fund_base_source + "/fina_mainbz_p.csv"
    csv_fina_mainbz_d = fund_base_source + "/fina_mainbz_d.csv"
    csv_fina_mainbz_sum = fund_base_source + "/fina_mainbz_sum.csv"
    csv_disclosure_date = fund_base_source + "/disclosure_date.csv"
    csv_basic = fund_base_source + "/basic.csv"

    fund_base_latest = fund_base_source + "/latest"

    csv_income_latest = fund_base_latest + "/income.csv"
    csv_balancesheet_latest = fund_base_latest + "/balancesheet.csv"
    csv_cashflow_latest = fund_base_latest + "/cashflow.csv"
    csv_forecast_latest = fund_base_latest + "/forecast.csv"
    csv_express_latest = fund_base_latest + "/express.csv"
    csv_dividend_latest = fund_base_latest + "/dividend.csv"
    csv_fina_indicator_latest = fund_base_latest + "/fina_indicator.csv"
    csv_fina_audit_latest = fund_base_latest + "/fina_audit.csv"
    csv_fina_mainbz_p_latest = fund_base_latest + "/fina_mainbz_p.csv"
    csv_fina_mainbz_d_latest = fund_base_latest + "/fina_mainbz_d.csv"
    csv_fina_mainbz_sum_latest = fund_base_latest + "/fina_mainbz_sum.csv"
    csv_fina_mainbz_latest_percent = fund_base_latest + "/fina_mainbz_percent.csv"
    csv_disclosure_date_latest = fund_base_latest + "/disclosure_date.csv"
    csv_disclosure_date_latest_notify = fund_base_latest + "/disclosure_date_notify.csv"

    col_list_income = ["ts_code", "name", "end_date", "basic_eps", "total_revenue", "revenue", "oth_b_income", "n_income_attr_p", "distable_profit"]
    col_list_balancesheet = ["ts_code", "name", "end_date", "total_assets", "total_liab", "money_cap", "undistr_porfit", "invest_real_estate", "fa_avail_for_sale", "lt_borr", "st_borr", "cb_borr"]
    col_list_cashflow = ["ts_code", "name", "end_date", "net_profit"]
    col_list_forecast = [
        "ts_code",
        "name",
        "end_date",
    ]
    col_list_dividend = [
        "ts_code",
        "name",
        "end_date",
    ]
    col_list_express = [
        "ts_code",
        "name",
        "end_date",
    ]
    col_list_disclosure_date = ["ts_code", "name", "ann_date", "end_date", "pre_date", "actual_date", "modify_date"]

    # rd_exp is not in the actual output
    col_list_fina_indicator = [
        "ts_code",
        "name",
        "end_date",
        "eps",
        "roe",
        "debt_to_assets",
        "rd_exp",
        "total_revenue_ps",
        "netprofit_margin",
    ]
    col_list_fina_audit = [
        "ts_code",
        "name",
        "end_date",
    ]
    col_list_fina_mainbz = [
        "ts_code",
        "name",
        "end_date",
    ]


def set_global_pro_fetch_field():
    global query_fields_income
    global query_fields_balancesheet
    global query_fields_cashflow
    global query_fields_fina_indicator
    global query_fields_forecast
    global query_fields_dividend
    global query_fields_express
    global query_fields_fina_audit
    global query_fields_fina_mainbz
    global query_fields_disclosure_date

    query_fields_income = finlib.Finlib().get_tspro_query_fields("income")
    query_fields_balancesheet = finlib.Finlib().get_tspro_query_fields("balancesheet")
    query_fields_cashflow = finlib.Finlib().get_tspro_query_fields("cashflow")
    query_fields_fina_indicator = finlib.Finlib().get_tspro_query_fields("fina_indicator")
    query_fields_forecast = finlib.Finlib().get_tspro_query_fields("forecast")
    query_fields_dividend = finlib.Finlib().get_tspro_query_fields("dividend")
    query_fields_express = finlib.Finlib().get_tspro_query_fields("express")
    query_fields_fina_audit = finlib.Finlib().get_tspro_query_fields("fina_audit")
    query_fields_fina_mainbz = finlib.Finlib().get_tspro_query_fields("fina_mainbz")
    query_fields_disclosure_date = finlib.Finlib().get_tspro_query_fields("disclosure_date")


def get_a_specify_stock(ts_code, end_date):  # (ts_code='600519.SH', end_date='20180630')
    odir = fund_base_tmp + "/" + ts_code + "_" + end_date

    if not os.path.exists(fund_base_tmp):
        os.makedirs(fund_base_tmp)

    csv_income_tmp = odir + "/income.csv"
    csv_balancesheet_tmp = odir + "/balancesheet.csv"
    csv_cashflow_tmp = odir + "/cashflow.csv"
    csv_forecast_tmp = odir + "/forecast.csv"
    csv_express_tmp = odir + "/express.csv"
    csv_dividend_tmp = odir + "/dividend.csv"
    csv_fina_indicator_tmp = odir + "/fina_indicator.csv"
    csv_fina_audit_tmp = odir + "/fina_audit.csv"
    # csv_fina_mainbz_tmp = odir + "/fina_mainbz.csv"
    csv_fina_mainbz_sum_tmp = odir + "/fina_mainbz_sum.csv"
    csv_disclosure_date_tmp = odir + "/disclosure_date.csv"

    _extract_latest(csv_input=csv_income, csv_output=csv_income_tmp, feature="income", col_name_list=col_list_income, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_balancesheet, csv_output=csv_balancesheet_tmp, feature="balancesheet", col_name_list=col_list_balancesheet, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_cashflow, csv_output=csv_cashflow_tmp, feature="cashflow", col_name_list=col_list_cashflow, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_forecast, csv_output=csv_forecast_tmp, feature="forecast", col_name_list=col_list_forecast, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_dividend, csv_output=csv_dividend_tmp, feature="dividend", col_name_list=col_list_dividend, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_express, csv_output=csv_express_tmp, feature="express", col_name_list=col_list_express, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_indicator, csv_output=csv_fina_indicator_tmp, feature="fina_indicator", col_name_list=col_list_fina_indicator, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_audit, csv_output=csv_fina_audit_tmp, feature="fina_audit", col_name_list=col_list_fina_audit, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_mainbz_sum, csv_output=csv_fina_mainbz_sum_tmp, feature="fina_mainbz", col_name_list=col_list_fina_mainbz, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_disclosure_date, csv_output=csv_disclosure_date_tmp, feature="disclosure_date", col_name_list=col_list_fina_mainbz, ts_code=ts_code, end_date=end_date)


def remove_dup_record(df_input, csv_name):

    df = pd.DataFrame(columns=list(df_input.columns))

    lst = list(df_input["ts_code"].unique())
    lst.sort()

    for code in lst:
        df_tmp = df_input[df_input["ts_code"] == code]
        df_append = pd.DataFrame()

        len = df_tmp.__len__()

        if len <= 1:
            df = df.append(df_tmp)
            continue

        # now df_tmp have multiple records (dup), check if any record has update_flag set.
        if "update_flag" in df_tmp.columns:
            df_tmp_updated = df_tmp[df_tmp["update_flag"] == "1"]  # <<< it is string 1.
            if df_tmp_updated.__len__() > 1:
                logging.info(__file__ + " " + csv_name + " has multi update_flag records, len " + str(df_tmp_updated.__len__()) + ".")
                sys.stdout.flush()
                logging.info(__file__ + " " + "\t" + df_tmp_updated["ts_code"] + " " + df_tmp_updated["end_date"])
                df_append = df_tmp_updated.iloc[0]  # choose the 1st updated records if have multiple updated records
                df = df.append(df_append)
                continue
            elif df_tmp_updated.__len__() == 1:
                logging.info(__file__ + " " + csv_name + " has one update_flag record\n")
                df_append = df_tmp_updated.iloc[0]
                df = df.append(df_append)
                continue
            else:
                logging.info(__file__ + " " + csv_name + " has multi records, len " + str(len) + " and zero update_flag records.")
                sys.stdout.flush()
                df_append = df_tmp.iloc[0]
                logging.info(__file__ + " " + "\t" + df_append["ts_code"] + " " + df_append["end_date"])
                df = df.append(df_append)
                continue
        else:
            # now df_tmp have multiple dup records, and no update_flag in columns.
            logging.info(__file__ + " " + csv_name + " has multi records, len " + str(len) + " and no update_flag in column.")
            sys.stdout.flush()
            df_append = df_tmp.iloc[0]
            logging.info(__file__ + " " + "\t" + df_append["ts_code"] + " " + df_append["end_date"])
            df = df.append(df_append)
            continue

    len = df.__len__()
    logging.info(__file__ + " " + "len of " + csv_name + " after remove dup records " + str(len))
    return df


def load_fund_result(mini_score=80):
    stable_rpt_date = finlib.Finlib().get_year_month_quarter()["stable_report_perid"]

    f_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_" + stable_rpt_date + ".csv"

    logging.info(__file__ + " " + "loading df_fund_2, " + f_fund_2)
    if (os.path.isfile(f_fund_2)) and os.stat(f_fund_2).st_size >= 10:  # > 10 bytes
        df_fund_2 = pd.read_csv(f_fund_2)
        df_fund_2 = df_fund_2[df_fund_2["sos"] > mini_score]

        # df_fund_2 = finlib.Finlib().ts_code_to_code(df_fund_2)
        df_fund_2 = df_fund_2[["ts_code", "name"]]
        # df_fund_2 = df_fund_2.rename(columns={"ts_code": "code"}, inplace=False)

        df_fund_2 = df_fund_2.drop_duplicates()
        df_fund_2 = df_fund_2.reset_index().drop("index", axis=1)
    else:
        logging.info(__file__ + " " + "no such file " + f_fund_2)
        logging.info(__file__ + " " + "stop and exit")
        exit(0)
    return df_fund_2


def fetch_pro_fund(fast_fetch=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    if not os.path.exists(fund_base):
        os.makedirs(fund_base)

    if not os.path.exists(fund_base_source + "/individual"):
        os.makedirs(fund_base_source + "/individual")

    time_series = finlib.Finlib().get_year_month_quarter()

    fetch_period_list = time_series["fetch_most_recent_report_perid"]
    fetch_period_list = list(set(fetch_period_list))  # remove duplicate in list
    fetch_period_list.sort(reverse=True)  # 20181231 -> 20171231 -> 20161231

    if fast_fetch:
        fetch_period_list = fetch_period_list[0:1]

        # high_score_stock_only
        if force_run_global:  # ryan debug
            stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
            stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)
        else:
            stock_list = load_fund_result(mini_score=70)
            # stock_list = finlib.Finlib().ts_code_to_code(df=stock_list)
            # stock_list = finlib.Finlib().add_ts_code_to_column(df=stock_list)
            stock_list = finlib.Finlib().remove_garbage(df=stock_list) #code: SH600519
            # print(stock_list.__len__())

    else:
        stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
        stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # code:SH603999
        fetch_period_list = time_series["full_period_list"][0:3] + time_series["full_period_list_yearly"]
        fetch_period_list = list(set(fetch_period_list))  # remove duplicate in list
        fetch_period_list.sort(reverse=True)  # 20181231 -> 20171231 -> 20161231


    if debug_global:  # ryan debug start of fetching
        stock_list = stock_list[stock_list["code"] == "SH600519"]
        fetch_period_list = ['20201231']
        # stock_list = stock_list[stock_list["code"] == "SH601995"]
        # stock_list=stock_list[stock_list['code']=='300319.SZ'] #income 20181231 is empty

    # select = datetime.datetime.today().day%2  # avoid too many requests a day

    # if (select == 0)  or fast_fetch or force_run_global: #'save_only' runs on the HK VPS.
    stock_list = finlib.Finlib().add_ts_code_to_column(df=stock_list,code_col='code')

    # not fetching/calculating fundermental data at month 6,9, 11, 12
    if not finlib.Finlib().get_report_publish_status()["process_fund_or_not"]:
        logging.info(__file__ + " " + "not processing fundermental data at this month. ")
        return ()
    else:
        #return the valid stock_list which has data.
        stock_list = _ts_pro_fetch(pro, stock_list, fast_fetch, "income", query_fields_income, fetch_period_list)  # 利润表
        _ts_pro_fetch(pro, stock_list, fast_fetch, "balancesheet", query_fields_balancesheet, fetch_period_list)  # 资产负债表
        _ts_pro_fetch(pro, stock_list, fast_fetch, "cashflow", query_fields_cashflow, fetch_period_list)  # 现金流量表
        _ts_pro_fetch(pro, stock_list, fast_fetch, "fina_indicator", query_fields_fina_indicator, fetch_period_list)  # 财务指标数据
        _ts_pro_fetch(pro, stock_list, fast_fetch, "fina_audit", query_fields_fina_audit, fetch_period_list)  # 财务审计意见

# check following as stock_list is very short after previous filter.
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'dividend', query_fields_dividend, fetch_period_list)  #分红送股
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'fina_mainbz_p', query_fields_fina_mainbz, fetch_period_list)  # 主营业务构成,Product
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'fina_mainbz_d', query_fields_fina_mainbz, fetch_period_list)  # 主营业务构成, Division
        #
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'forecast', query_fields_forecast, fetch_period_list)  #业绩预告
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'express', query_fields_express, fetch_period_list)  #业绩快报
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'disclosure_date', query_fields_disclosure_date, fetch_period_list)  #财报披露计划日期


def handler(signum, frame):
    logging.info(__file__ + " " + "timeout when fetching!")
    raise Exception("end of time")


# save only == True
def _ts_pro_fetch(pro_con, stock_list, fast_fetch, query, query_fields, fetch_period_list):
    # save_only == generate 6 source/*.csv, e.g income.csv, balance_sheet.csv
    fetch_period_list_ori = fetch_period_list
    df_stock_list_rtn = stock_list

    basic_df = get_pro_basic()
    fetch_most_recent_report_perid = finlib.Finlib().get_year_month_quarter()["fetch_most_recent_report_perid"][-1]

    if not os.path.exists(fund_base_source):
        os.makedirs(fund_base_source)

    total = str(stock_list.__len__())

    stock_cnt = 0
    for ts_code in stock_list["ts_code"]:
        stock_cnt += 1
        fetch_period_list = fetch_period_list_ori  # fetch_period_list will be empty after fetch a stock, so need restore it at the begining of new stock fetching.

        fetch_period_list = list(set(fetch_period_list))  # remove duplicate in list
        # fetch_period_list.sort(reverse=False) #20161231 -> 20171231 -> 20181231.
        fetch_period_list.sort(reverse=True)  # 20181231 -> 20171231 -> 20161231
        all_per_cnt = fetch_period_list.__len__()
        fetch_period_flag = False  # the 1st time, fetch use no period, so get a bunch of records.
        already_fetch_p = []

        p_cnt = 0

        # fetch_period_list = fetch_period_list[0:4] #this line result in only fetch default period in tushare. (no period in parameter when fetch)
        # fetch_period_list = [finlib.Finlib().get_year_month_quarter()['ann_date_1y_before']] #this line result in only fetch default period in tushare. (no period in parameter when fetch)

        for period in fetch_period_list:
            p_cnt += 1

            # logging.info(__file__+" "+"p_cnt "+str(p_cnt)+" stock_cnd "+str(stock_cnt) + " total " + total+" query "+query )
            # continue

            if period in already_fetch_p:
                logging.info(__file__ + " " + "skip period " + period + ", it has been fetched before")
                continue

            dir = fund_base_source + "/individual/" + period
            if not os.path.isdir(dir):
                os.mkdir(dir)

            ind_csv = dir + "/" + ts_code + "_" + query + ".csv"

            # print(ind_csv)

            # > 100 bytes
            # removed force_fetch checking. If want start fresh over, deleted the latest period folder. fund_base_source + "/individual/" + period
            if fast_fetch \
                    and (os.path.exists(ind_csv)) \
                    and (os.stat(ind_csv).st_size > 100) \
                    and finlib.Finlib().is_cached(ind_csv, day=14):
                logging.info(__file__ + ": " + "file already have content and update in 14 days, skip fetching, kick off from return df "+ind_csv)
                df_stock_list_rtn = df_stock_list_rtn[df_stock_list_rtn['ts_code'] != ts_code]
                continue


            if not os.path.exists(ind_csv):
                open(ind_csv, "a").close()  # create empty
            else:  # exist but ctime is two days before
                now = datetime.datetime.now()
                modTime = time.mktime(now.timetuple())
                os.utime(ind_csv, (modTime, modTime))

            if (not force_run_global) and (period < fetch_most_recent_report_perid):
                # logging.info(__file__ + " " + "not fetch stable period on " + ind_csv)
                continue

            if os.path.exists(ind_csv) and os.stat(ind_csv).st_size > 0 and period < fetch_most_recent_report_perid:
                already_fetch_p.append(period)
                logging.info(__file__ + " " + "not fetch as file already exists " + ind_csv + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
                continue

            # weekday = datetime.datetime.today().weekday()
            # #on Friday, the most recent Q fund data will be updated.
            # if (not force_run_global) \
            #         and os.path.exists(ind_csv) \
            #         and os.stat(ind_csv).st_size > 0:
            #     #and weekday != 5:
            #     #and os.stat(ind_csv).st_size > 0 \
            #     #and period < fetch_most_recent_report_perid:
            #     already_fetch_p.append(period)
            #     logging.info(__file__ + " " + "file have data already, not fetch out of FRIDAY. " + ind_csv + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
            #     continue

            if not finlib.Finlib().is_on_market(ts_code, period, basic_df):
                # logging.info()
                logging.info(__file__ + " " + "not fetch as stock is not on market. " + ts_code + " " + period + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
                continue

            # signal.signal(signal.SIGALRM, handler)

            file_csv = fund_base_source + "/individual/" + period + "/" + ts_code + "_" + query + ".csv"

            logging.info(__file__ + " " + "handling " + file_csv)

            # if (not force_run_global)  and finlib.Finlib().is_cached(file_csv, day=6):
            if finlib.Finlib().is_cached(file_csv, day=6):
                logging.info(__file__ + " " + "file has been updated in 6 days, not fetch again " + file_csv)
                continue

            # try:
            logging.info(__file__ + " " + "fetching period " + str(p_cnt) + " of " + str(all_per_cnt) + " , stock " + str(stock_cnt) + " of " + total + ", Getting " + query + " " + ts_code + " " + period)

            time.sleep(60.0 / 45)

            # signal.alarm(5)
            df_tmp = pd.DataFrame()
            if query in ["income", "balancesheet", "cashflow", "fina_indicator", "fina_audit", "fina_mainbz_p", "fina_mainbz_d", "disclosure_date", "express"]:
                if fast_fetch:
                    if query in ["fina_audit", "fina_mainbz_p", "fina_mainbz_d", "disclosure_date", "express"] and (not str(period).__contains__("1231")) and fetch_period_flag:
                        continue
                    else:
                        try:
                            if fetch_period_flag:
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                                if query == 'fina_mainbz_d':
                                    df_tmp = pro_con.query('fina_mainbz', ts_code=ts_code, fields=query_fields, period=period, type='D')
                                elif query == 'fina_mainbz_p':
                                    df_tmp = pro_con.query('fina_mainbz', ts_code=ts_code, fields=query_fields, period=period, type='P')
                                else:
                                    df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)

                            else:  # the 1st fetch
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                                if query == 'fina_mainbz_d':
                                    df_tmp = pro_con.query('fina_mainbz', ts_code=ts_code, fields=query_fields, type='D')
                                elif query == 'fina_mainbz_p':
                                    df_tmp = pro_con.query('fina_mainbz', ts_code=ts_code, fields=query_fields, type='P')
                                else:
                                    df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)

                        except Exception as e:
                            logging.exception("Exception occurred")
                # else:
                #     if query in ["fina_audit", "fina_mainbz", "disclosure_date", "express"] and (not str(period).__contains__("1231")):
                #         continue
                #     else:
                #         try:
                #             if fetch_period_flag:
                #                 logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                #                 df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                #
                #             else:  # the 1st fetch
                #                 logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                #                 df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields) ## Income,balance, most useful api
                #
                #         except Exception as e:
                #             logging.exception("Exception occurred")
            elif query in ["forecast"]:
                if fast_fetch:
                    if str(period).__contains__("1231"):
                        try:
                            if fetch_period_flag:
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                            else:  # the 1st fetch
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                        except Exception as e:
                            logging.exception("Exception occurred")
                    else:
                        continue
                else:
                    try:
                        logging.info("query " + str(query) + " tscode " + str(ts_code))
                        if fetch_period_flag:
                            logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                            df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                        else:  # the 1st fetch
                            logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                            df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                    except Exception as e:
                        logging.exception("Exception occurred")
            elif query in ["dividend"]:
                if fast_fetch:
                    if str(period).__contains__("1231"):
                        try:
                            logging.info("query " + str(query) + " tscode " + str(ts_code))
                            if fetch_period_flag:
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                            else:  # the 1st fetch
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                        except Exception as e:
                            logging.exception("Exception occurred")
                    else:
                        continue
                else:
                    try:
                        if fetch_period_flag:

                            logging.info("query " + str(query) + " tscode " + str(ts_code))
                            if fetch_period_flag:
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                            else:  # the 1st fetch
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)

                        else:  # the 1st fetch

                            logging.info("query " + str(query) + " tscode " + str(ts_code))
                            if fetch_period_flag:
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period " + str(period))
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                            else:  # the 1st fetch
                                logging.info("query " + str(query) + " tscode " + str(ts_code) + " period is None")
                                df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                    except Exception as e:
                        logging.exception("Exception occurred")

            logging.info(__file__ + " " + str(period) + " " + str(query) + ". received len " + str(df_tmp.__len__()))
            logging.info(finlib.Finlib().pprint(df_tmp.head(1)))

            # signal.alarm(0)
            # df_tmp = df_tmp.astype(str)
            # logging.info(df_tmp)

            field = ""
            if "end_date" in df_tmp.columns:
                field = "end_date"
            elif "period" in df_tmp.columns:
                field = "period"
            elif "ann_date" in df_tmp.columns:
                field = "ann_date"
            else:
                continue  # when getting df_tmp failed and df_tmp is None.

            if (not force_run_global) and fast_fetch:
                df_tmp = df_tmp[df_tmp[field] == fetch_most_recent_report_perid]

                if df_tmp.empty:
                    logging.info(__file__ + ": " + "no content for "+ts_code +" "+query+" "+period+", kick off from return df")
                    df_stock_list_rtn = df_stock_list_rtn[df_stock_list_rtn['ts_code'] != ts_code]
                    continue
                else:
                    print(finlib.Finlib().pprint(df=df_tmp))

            name = stock_list[stock_list["ts_code"] == ts_code]["name"].values[0]
            df_tmp = pd.DataFrame([name] * df_tmp.__len__(), columns=["name"]).join(df_tmp)
            df_tmp = df_tmp.drop_duplicates().reset_index().drop("index", axis=1)

            # df_tmp contains multiple end_date
            end_date_lst = list(df_tmp[field].unique())

            # create an empty csv file if return df is empty.
            # if (not os.path.exists(ind_csv)) and (period not in end_date_lst or df_tmp.__len__()== 0):
            # if (not os.path.exists(ind_csv)):
            #    open(ind_csv, 'a').close()
            #    logging.info(__file__+" "+"created empty file "+ind_csv)

            for ed in end_date_lst:
                ed = str(ed)
                if ed == "nan":
                    continue
                logging.info(__file__ + " " + "end date is " + ed)

                if ed in fetch_period_list:
                    fetch_period_list.remove(ed)
                    fetch_period_flag = True  # next fetching will use period
                    logging.info("removed " + str(ed) + " from fetch_period_list")

                if ed in already_fetch_p:
                    # print("already fetched " + ed)
                    continue

                df_tmp_sub = df_tmp[df_tmp[field] == ed]

                # if df_tmp_sub.__len__() > 1 and "update_flag" in df_tmp_sub.columns and (query not in ['fina_mainbz','dividend']):
                if df_tmp_sub.__len__() > 1 and "update_flag" in df_tmp_sub.columns and (df_tmp_sub[df_tmp_sub["update_flag"] == "1"].__len__()>0):
                    df_tmp_sub = df_tmp_sub[df_tmp_sub["update_flag"] == "1"]
                    # if df_tmp_sub.__len__() > 1:
                    #    df_tmp_sub = df_tmp_sub.iloc[-1]
                df_tmp_sub = df_tmp_sub.reset_index().drop("index", axis=1)

                dir_sub = fund_base_source + "/individual/" + ed
                if not os.path.isdir(dir_sub):
                    os.mkdir(dir_sub)

                ind_csv_sub = dir_sub + "/" + ts_code + "_" + query + ".csv"

                # if (not os.path.exists(ind_csv_sub)) or (force_run_global) or (os.stat(ind_csv).st_size < 2000):
                if df_tmp_sub.__len__() > 0:
                    df_tmp_sub.to_csv(ind_csv_sub, encoding="UTF-8", index=False)
                    logging.info(__file__ + ": " + "saved " + ind_csv_sub + " . len " + str(df_tmp_sub.__len__()))
                else:
                    logging.info(__file__+" "+"empty df, skip saving "+ind_csv_sub)

                if not ed in already_fetch_p:
                    already_fetch_p.append(ed)
                    # logging.info(__file__+" "+"append "+ed +" to already_fetch_p")
    return(df_stock_list_rtn)


# jasq stop work, get PE, PB from tushare. 20190302
def fetch_basic_quarterly():
    ts.set_token(myToken)
    pro = ts.pro_api()

    a = finlib.Finlib().get_year_month_quarter()
    b = a["full_period_list"]

    fields = "ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm,pb, ps, ps_ttm,"
    fields += "dv_ratio,dv_ttm,total_share,float_share,free_share, total_mv,circ_mv "

    dir_q = fund_base_source + "/basic_quarterly"

    if not os.path.isdir(dir_q):
        os.mkdir(dir_q)

    ### get quarterly

    for i in b:
        output_csv = dir_q + "/basic_" + i + ".csv"  # the date in filename is i but not the actual date of the data.
        if finlib.Finlib().is_cached(output_csv, day=90) and (not force_run_global):
            logging.info(__file__ + " " + "file exist and have content, not fetch again " + output_csv)
            continue

        d = finlib.Finlib().get_last_trading_day(date=i)

        reg = re.match("(\d{4})(\d{2})(\d{2})", d)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)

        trade_date = yyyy + mm + dd

        df = pro.daily_basic(ts_code="", trade_date=trade_date, fields=fields)
        time.sleep(1)
        df.to_csv(output_csv, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved basic of all stocks to " + output_csv + " len " + str(df.__len__()))


def fetch_basic_daily(fast_fetch=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    ##### get daily basic
    calendar_f = "/home/ryan/DATA/pickle/trading_day_" + str(datetime.datetime.today().year) + ".csv"
    if not os.path.isfile(calendar_f):
        logging.error("no such file " + calendar_f)
        exit()

    trade_days = pandas.read_csv(calendar_f)
    todayS = datetime.datetime.today().strftime("%Y%m%d")

    trading_days = trade_days[(trade_days.cal_date <= int(todayS)) & (trade_days.is_open == 1)]
    # trading_days = trading_days.sort_values("cal_date", ascending=False, inplace=False)  #don't sort, we need create the latest_ symbol link.

    if fast_fetch:  # run on daily, fetch the most recent 5 day only.
        trading_days = trading_days[-5:]

    # the file should keep same between t_daily_update_csv_from_tushare.py and t_daily_fundamentals_2.py
    fields = "ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm,pb, ps, ps_ttm,"
    fields += "dv_ratio,dv_ttm,total_share,float_share,free_share, total_mv,circ_mv "

    dir_d = fund_base_source + "/basic_daily"

    if not os.path.isdir(dir_d):
        os.mkdir(dir_d)

    for i in trading_days["cal_date"]:
        reg = re.match(r"(\d{4})(\d{2})(\d{2})", str(i))
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)

        trade_date = yyyy + mm + dd

        # trade_date="20191224" #ryan debug

        output_csv = dir_d + "/basic_" + trade_date + ".csv"

        if finlib.Finlib().is_cached(output_csv, day=90) and (not force_run_global):
            # logging.info(__file__ + " " + "file exist and have content, not fetch again " + output_csv)
            continue

        logging.info(__file__ + " " + "fetch daily_basic on date " + str(trade_date))
        df = pro.daily_basic(ts_code="", trade_date=trade_date, fields=fields)
        time.sleep(1)

        df.to_csv(output_csv, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved daily basic of all stocks to " + output_csv + " len " + str(df.__len__()))

    pass


def fetch_info_daily(fast_fetch=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    ##### get daily basic
    calendar_f = "/home/ryan/DATA/pickle/trading_day_" + str(datetime.datetime.today().year) + ".csv"
    if not os.path.isfile(calendar_f):
        logging.error("no such file " + calendar_f)
        exit()

    trade_days = pandas.read_csv(calendar_f)
    todayS = datetime.datetime.today().strftime("%Y%m%d")

    trading_days = trade_days[(trade_days.cal_date <= int(todayS)) & (trade_days.is_open == 1)]
    trading_days = trading_days.sort_values("cal_date", ascending=False, inplace=False)

    if fast_fetch:  # run on daily, fetch the most recent 5 day only.
        trading_days = trading_days[:5]

    # the file should keep same between t_daily_update_csv_from_tushare.py and t_daily_fundamentals_2.py
    # trade_date	                         #	交易日期
    # ts_code	                         #	市场代码
    # ts_name	                         #	市场名称
    # com_count	                         #	挂牌数
    # total_share	                         #	总股本（亿股）
    # float_share	                         #	流通股本（亿股）
    # total_mv	                         #	总市值（亿元）
    # float_mv	                         #	流通市值（亿元）
    # amount	                         #	交易金额（亿元）
    # vol	                         #	成交量（亿股）
    # trans_count	                         #	成交笔数（万笔）
    # pe	                         #	平均市盈率
    # tr	                         #	换手率（％），注：深交所暂无此列
    # exchange	                         #	交易所（SH上交所 SZ深交所）

    fields = "trade_date,ts_code,ts_name,com_count, total_share, float_share, total_mv, float_mv, amount, vol, trans_count,pe, tr, exchange"

    dir_d = fund_base_source + "/info_daily"

    if not os.path.isdir(dir_d):
        os.mkdir(dir_d)

    for i in trading_days["cal_date"]:
        reg = re.match(r"(\d{4})(\d{2})(\d{2})", str(i))
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)

        trade_date = yyyy + mm + dd

        # trade_date="20191224" #ryan debug

        output_csv = dir_d + "/info_" + trade_date + ".csv"

        if finlib.Finlib().is_cached(output_csv, day=90) and (not force_run_global):
            logging.info(__file__ + " " + "file exist and have content, not fetch again " + output_csv)
            continue

        logging.info(__file__ + " " + "fetch daily_info on date " + str(trade_date))
        df = pro.daily_info(trade_date=trade_date, exchange="SZ,SH", fields=fields)
        time.sleep(1)

        df.to_csv(output_csv, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "saved daily info of all stocks to " + output_csv + " len " + str(df.__len__()))

    pass



def fetch_industry_l123():
    pro = ts.pro_api(token=myToken)

    # 获取申万一级行业列表
    df1 = pro.index_classify(level='L1', src='SW2021')
    # 获取申万二级行业列表
    df2 = pro.index_classify(level='L2', src='SW2021')
    # 获取申万三级级行业列表
    df3 = pro.index_classify(level='L3', src='SW2021')

    df_index = df1.append(df2).append(df3).reset_index().drop('index', axis=1)

    out_csv = "/home/ryan/DATA/pickle/ag_industry.csv"
    df_index.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("industry index saved to " + out_csv)


    df1n = pd.DataFrame()
    for index, row in df1.iterrows():
        logging.info(
            str(index) + " of " + str(df1.__len__()) + ", fetching index_code " + row['index_code'] + " ," + row[
                'industry_name'] + " ," + row['level'])
        df1n = df1n.append(pro.index_member(index_code=row['index_code']))
        time.sleep(1) #max visiting frequency 150/min
    df1n = df1n.reset_index().drop('index', axis=1)
    df1n = pd.merge(df1n, df_index, on='index_code', how='inner')

    df2n = pd.DataFrame()
    for index, row in df2.iterrows():
        logging.info(
            str(index) + " of " + str(df2.__len__()) + ", fetching index_code " + row['index_code'] + " ," + row[
                'industry_name'] + " ," + row['level'])
        df2n = df2n.append(pro.index_member(index_code=row['index_code']))
        time.sleep(1)
    df2n = df2n.reset_index().drop('index', axis=1)
    df2n = pd.merge(df2n, df_index, on='index_code', how='inner')

    df3n = pd.DataFrame()
    for index, row in df3.iterrows():
        logging.info(
            str(index) + " of " + str(df3.__len__()) + ", fetching index_code " + row['index_code'] + " ," + row[
                'industry_name'] + " ," + row['level'])
        df3n = df3n.append(pro.index_member(index_code=row['index_code']))
        time.sleep(1)
    df3n = df3n.reset_index().drop('index', axis=1)
    df3n = pd.merge(df3n, df_index, on='index_code', how='inner')

    dfnn = pd.merge(left=df1n, right=df2n, on='con_code', how="inner", suffixes=("_1n", "_2n"))
    dfnn = pd.merge(left=dfnn, right=df3n, on='con_code', how="inner", suffixes=("", "_3n"))
    dfnn['industry_name_L1_L2_L3'] = dfnn['industry_name_1n'] + "_" + dfnn['industry_name_2n'] + "_" + dfnn[
        'industry_name']
    dfnn['index_code_L1_L2_L3'] = dfnn['index_code_1n'] + "_" + dfnn['index_code_2n'] + dfnn['index_code']
    dfnn = dfnn[['con_code', 'industry_name_L1_L2_L3', 'index_code_L1_L2_L3']]
    #
    # dfnn = df1n.append(df2n).append(df3n).reset_index().drop('index', axis=1)

    dfnn = dfnn.rename(columns={"con_code": "ts_code"}, inplace=False)
    dfnn = finlib.Finlib().ts_code_to_code(dfnn)
    dfnn = finlib.Finlib().add_stock_name_to_df(dfnn)

    dfnn[dfnn['code'] == 'SH600519']

    out_csv = "/home/ryan/DATA/pickle/ag_stock_industry.csv"
    dfnn.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("stock industry mapping saved to "+out_csv)

    return(dfnn)



def fetch_new_share():
    ts.set_token(myToken)
    pro = ts.pro_api()
    df = pro.new_share()
    # df = df[df['issue_date']>='20200801']
    df = df.sort_values(by=["ipo_date", "issue_date"], ascending=False).reset_index().drop("index", axis=1)
    f = fund_base_source + "/new_share.csv"
    df.to_csv(f, encoding="UTF-8", index=False)
    logging.info("new share saved to " + f)


def _fetch_change_name():
    ts.set_token(myToken)
    pro = ts.pro_api()
    df = pro.namechange()
    f = fund_base_source + "/changed_name_stocks.csv"
    df.to_csv(f, encoding="UTF-8", index=False)
    logging.info("all stocks changed name saved to " + f)


# input: source/*.csv
# output: source/individual_per_stock/stockid_feature.csv , which include all history. (not the 50 records entry)


def merge_individual():
    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list = stock_list["code"]

    list = ["income", "balancesheet", "cashflow", "fina_mainbz", "dividend", "fina_indicator", "fina_audit", "forecast", "express", "disclosure_date"]

    if debug_global:
        stock_list = ["600519.SH"]
        stock_list = ["000333.SZ"]

    for ts_code in stock_list:
        for feature in list:
            _merge_individual_bash(ts_code, feature)


# input: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
# output: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/a_stock_code_feature.csv
def _merge_individual_bash(ts_code, feature):
    logging.info(__file__ + " " + "processing " + ts_code + " " + feature)
    fetch_most_recent_report_perid = finlib.Finlib().get_year_month_quarter()["fetch_most_recent_report_perid"][0]

    input_file = fund_base_source + "/" + feature + ".csv"

    output_dir = fund_base_source + "/individual_per_stock"
    output_csv = output_dir + "/" + ts_code + "_" + feature + ".csv"

    if finlib.Finlib().is_cached(output_csv, day=6) and (not force_run_global):
        logging.info(__file__ + " " + "file updated in 6 days, not processing. " + output_csv)
        return ()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # f_header = "~/tmp/header.txt"
    # f_content = "~/tmp/content.txt"

    # os.system("rm -f " + f_header + "; ")
    # os.system("rm -f " + f_content + "; ")

    os.system("head -1 " + input_file + " > " + output_csv)

    cmd = "grep " + ts_code + " " + input_file + " >> " + output_csv
    logging.info(cmd)
    os.system(cmd)


# input: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
# output: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/a_stock_code_basic.csv
def merge_individual_bash_basic(fast_fetch=False):
    os.system("mkdir -p /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock")

    if fast_fetch:

        last_trade_date = finlib.Finlib().get_last_trading_day()
        reg = re.match("(\d{4})(\d{2})(\d{2})", last_trade_date)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)
        last_trade_date = yyyy + mm + dd

        input_csv = fund_base_source + "/basic_daily/basic_" + last_trade_date + ".csv"

        logging.info(__file__ + " " + "DAILY UPDATE, update " + input_csv + " to source/individual_per_stock/*code*_basic.csv")

        if not os.path.exists(input_csv):
            logging.error("no such file " + input_csv + " , cannot continue")
            exit(0)

        logging.info(__file__ + " " + "read csv " + input_csv)
        df = pd.read_csv(input_csv, converters={i: str for i in range(20)}, names=["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "total_share", "float_share", "total_mv", "circ_mv"])

        totals = df.__len__()
        for cnt in range(totals - 1):
            entry = df.iloc[cnt + 1]
            ts_code = entry["ts_code"]
            this_date = entry["trade_date"]
            logging.info(str(cnt + 1) + " of " + str(totals - 1) + " " + ts_code)

            output_csv = fund_base_source + "/individual_per_stock/" + ts_code + "_basic.csv"

            if os.path.exists(output_csv):
                df_exist = pd.read_csv(output_csv, converters={i: str for i in range(20)})  # @todo: need to specify the column name. Dangerous
                df_test = df_exist[df_exist.trade_date == this_date]
                if df_test.__len__() == 1:
                    logging.info(__file__ + " " + "file already updated. " + output_csv + " to date " + this_date)
                    continue
                elif df_test.__len__() > 1:
                    logging.info(__file__ + " " + "ERROR, duplicate records in . " + output_csv + " for day " + this_date)
                    continue
                else:
                    df_exist = df_exist.append(entry).reset_index().drop("index", axis=1)
                    df_exist.to_csv(output_csv, encoding="UTF-8", index=False)
                    logging.info(__file__ + " " + "updated day " + this_date + " to " + output_csv + " len " + str(df_exist.__len__()))
            else:
                logging.info(__file__ + " " + "new stock " + ts_code + " no such file, " + output_csv)
                entry.to_csv(output_csv, encoding="UTF-8", index=False)
                logging.info(__file__ + " " + "file saved, len " + str(entry.__len__()))

    if not fast_fetch:
        logging.info(__file__ + " " + "FULL UPDATE, overwrite exists. processing basic, split source/basic.csv to source/individual_per_stock/ts_code_basic.csv")

        check_csv = fund_base_source + "/individual_per_stock/600519.SH_basic.csv"

        if (not force_run_global) and finlib.Finlib().is_cached(check_csv, day=6):
            logging.info(__file__ + " " + "*_basic.csv are updated in 5 days, not process. result checked by " + check_csv)

        tmp_dir = "~/tmp/pro_basic"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        # create a 2G ramdisk /mnt/ramdisk. Files inside will gone after reboot.
        # sudo mkdir /mnt/ramdisk
        # sudo vim /etc/fstab, adding
        # tmpfs  /mnt/ramdisk  tmpfs  rw,size=2G  0   0

        cmd = "cd " + fund_base_source + ";"
        cmd += "cp basic.csv /mnt/ramdisk/basic.csv.tmp;"
        cmd += "mkdir -p  ~/tmp/pro_basic;"

        cmd += "for i in `awk -F',' '{print $1}' /mnt/ramdisk/basic.csv.tmp  |sort| uniq|grep -v ts_code` ; do "
        cmd += "echo ${i}_basic.csv;  head -1 /mnt/ramdisk/basic.csv.tmp > ~/tmp/pro_basic/${i}_basic.csv;"
        cmd += 'grep -E "^$i" /mnt/ramdisk/basic.csv.tmp >> ~/tmp/pro_basic/${i}_basic.csv; '
        cmd += "mv ~/tmp/pro_basic/${i}_basic.csv " + fund_base_source + "/individual_per_stock/ ;"
        #        cmd += "grep -v \"^$i\" basic.csv.tmp > tmp; mv tmp basic.csv.tmp;" #let filesystem do caching. Not read the file again.
        cmd += "done"

        logging.info(cmd)
        os.system(cmd)
        os.system("rm -f basic.csv.tmp")


#########################
# input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/*.csv
# output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv [ fina_mainbz_sum.csv,balancesheet.csv,dividend.csv,fina_audit.csv,fina_mainbz_p.csv,fina_mainbz_d.csv,forecast.csv]
#
#########################
def merge_local_bash(debug=False):
    features = ["income", "balancesheet", "cashflow", "fina_mainbz_p", "fina_mainbz_d", "dividend", "fina_indicator", "fina_audit", "forecast", "express", "disclosure_date"]

    input_dir = fund_base_source + "/individual"

    if not os.path.exists(fund_base_tmp):
        os.makedirs(fund_base_tmp)

    for f in features:
        tmp_f = fund_base_tmp + "/" + f + ".txt"
        cmd="rm -f " + tmp_f
        logging.info(cmd)
        os.system(cmd)

        output_csv = fund_base_source + "/" + f + ".csv"

        if (not force_run_global) and finlib.Finlib().is_cached(output_csv, day=6):
            logging.info(__file__ + " " + "file updated in 6 days, not process. " + output_csv)
            continue
        if debug:
            #fundamentals_2/source/individual/20181231/600519.SH_balancesheet.csv
            cmd = "find -L " + input_dir + " -size +0 -name *600519.SH_" + f + ".csv  -exec cat {} >> " + tmp_f + " \;"
        else:
            cmd = "find -L " + input_dir + " -size +0 -name *_" + f + ".csv  -exec cat {} >> " + tmp_f + " \;"
            # cmd = "find -L " + input_dir + " -size +0 -name *600519.SH_" + f + ".csv  -exec cat {} >> " + tmp_f + " \;"

        logging.info(cmd)
        start_time = time.time()
        os.system(cmd)
        logging.info(__file__ + " " + "--- %s seconds ---" % (time.time() - start_time))

        # sort and uniq, inplace

        cmd="head -1 " + tmp_f + " > " + output_csv
        logging.info(cmd)
        os.system(cmd)  # make header

        cmd="sort -u -o " + tmp_f + " " + tmp_f
        logging.info(cmd)
        os.system(cmd)

        cmd='grep -vE "ts_code.*name|name.*ts_code" ' + tmp_f + " >> " + output_csv
        logging.info(cmd)
        os.system(cmd)  # append body

        cmd="rm -f " + tmp_f
        logging.info(cmd)
        os.system(cmd)

        # os.system("mv "+tmp_f +  " "+output_csv)
        logging.info(__file__ + " " + "merged all " + f + " to " + output_csv+"\n\n")


###############################
# input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/*.csv
# output:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
###############################
def merge_local_bash_basic(output_csv, fast=False):
    logging.info(__file__ + " " + "DAILY merge local basic")
    logging.info(output_csv)
    if (not fast) and (not force_run_global) and finlib.Finlib().is_cached(output_csv, 6) and (os.stat(output_csv).st_size >= 10):
        logging.info(__file__ + " " + "file is updated in 5 days, not merge again. " + output_csv)
        return ()

    f_header = "~/tmp/header.txt"
    f_content = "~/tmp/content.txt"

    if fast:  # run daily, merge daily to the basic.csv
        last_trade_date = finlib.Finlib().get_last_trading_day()
        reg = re.match("(\d{4})(\d{2})(\d{2})", last_trade_date)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)
        last_trade_date = yyyy + mm + dd

        input_csv = fund_base_source + "/basic_daily/basic_" + last_trade_date + ".csv"

        if not os.path.exists(input_csv):
            logging.error("no such file " + input_csv + " , cannot continue")
            exit(0)

        if not os.path.exists(output_csv):
            logging.error("no such file " + output_csv + " , cannot continue")
            exit(0)

        logging.info(__file__ + " " + "read csv " + output_csv)
        df = pd.read_csv(output_csv, skiprows=9390000, converters={i: str for i in range(20)}, names=["ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "total_share", "float_share", "total_mv", "circ_mv"])
        if not df.empty:
            df = df[df.trade_date == last_trade_date]
            if df.__len__() > 1000:  # should have more than 3000+ records
                logging.info(str(df.__len__()) + " records were found, date " + last_trade_date + " should have already updated to " + output_csv)
                return ()

        cmd_content = "sed 1d " + input_csv + " > " + f_content
        logging.info(cmd_content)
        os.system(cmd_content)

        cmd_content = "cat " + f_content + " >> " + output_csv
        logging.info(cmd_content)
        os.system(cmd_content)

        cmd_content = "rm -f " + f_content
        logging.info(cmd_content)
        os.system(cmd_content)

        logging.info(__file__ + " " + "merged latest trading date " + last_trade_date + " to " + output_csv)
        return ()

    if not fast:
        logging.info(__file__ + " " + "FULLY merge local basic")
        cmd_header = "for i in `ls " + fund_base_source + "/basic_daily/basic_*.csv`; do sed 1q $i > " + f_header + "; break; done;"

        cmd_content = "rm -f " + f_content + "; "
        cmd_content += "for i in `ls " + fund_base_source + "/basic_daily/basic_2019*.csv`; do sed 1d $i >> " + f_content + "; done;"
        cmd_content += "for i in `ls " + fund_base_source + "/basic_daily/basic_2020*.csv`; do sed 1d $i >> " + f_content + "; done;"
        cmd_content += "for i in `ls " + fund_base_source + "/basic_daily/basic_2021*.csv`; do sed 1d $i >> " + f_content + "; done;"

        # cmd_exist_content_remove_header_overwrite = "sed -i 1d "+output_csv

        cmd_uniq = "sort -u -o " + f_content + " " + f_content

        logging.info(cmd_header)
        os.system(cmd_header)

        logging.info(cmd_content)
        os.system(cmd_content)

        logging.info(cmd_uniq)
        os.system(cmd_uniq)

        cmd = "cat " + f_content + " >> " + f_header
        logging.info(cmd)
        os.system(cmd)

        cmd = "mv " + f_header + " " + output_csv
        logging.info(cmd)
        os.system(cmd)

        cmd = "rm -f " + f_content
        logging.info(cmd)
        os.system(cmd)
        return
    """ ### pandas operation being killed by linux. csv file 1G+
    df = pd.read_csv(output_csv, converters={i: str for i in range(20)})
    df = df.fillna(0)

    if not df.empty:
        df = df.reset_index().drop('index', axis=1)

        df.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + output_csv+" . len "+str(df.__len__()))
    else:
        logging.info(__file__+" "+"df is empty")
    """


###############################
# input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/*.csv
# output:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly.csv
###############################
def merge_local_bash_basic_quarterly():
    output_csv = fund_base_source + "/basic_quarterly.csv"

    logging.info(__file__ + " " + "DAILY merge local basic quartly")
    logging.info(output_csv)
    if (not force_run_global) and finlib.Finlib().is_cached(output_csv, 6) and (os.stat(output_csv).st_size >= 10):
        logging.info(__file__ + " " + "file is updated in 5 days, not merge again. " + output_csv)
        return ()

    f_header = "~/tmp/header.txt"
    f_content = "~/tmp/content.txt"

    logging.info(__file__ + " " + "merge local basic quarterly starts")
    cmd_header = "for i in `ls " + fund_base_source + "/basic_quarterly/basic_*.csv`; do sed 1q $i > " + f_header + "; break; done;"

    cmd_content = "rm -f " + f_content + "; "
    cmd_content += "for i in `ls " + fund_base_source + "/basic_quarterly/basic_*.csv`; do sed 1d $i >> " + f_content + "; done;"

    # cmd_exist_content_remove_header_overwrite = "sed -i 1d "+output_csv

    cmd_uniq = "sort -u -o " + f_content + " " + f_content

    logging.info(cmd_header)
    os.system(cmd_header)

    logging.info(cmd_content)
    os.system(cmd_content)

    logging.info(cmd_uniq)
    os.system(cmd_uniq)

    cmd = "cat " + f_content + " >> " + f_header
    logging.info(cmd)
    os.system(cmd)

    cmd = "mv " + f_header + " " + output_csv
    logging.info(cmd)
    os.system(cmd)

    cmd = "rm -f " + f_content
    logging.info(cmd)
    os.system(cmd)
    logging.info(__file__ + " " + "merge local basic quarterly completed , saved to " + output_csv)
    return


def sum_fina_mainbz():

    if (not force_run_global) and finlib.Finlib().is_cached(csv_fina_mainbz_sum, day=6):
        logging.info(__file__ + " " + "skip file, it been updated in 6 day. " + csv_fina_mainbz_sum)
        return

    df = pd.read_csv(csv_fina_mainbz_p, converters={"end_date": str})

    df = df.fillna(0)

    df_result = pd.DataFrame(columns=list(df.columns))
    df_result = df_result.drop("bz_item", axis=1)

    lst = list(df["ts_code"].unique())
    lst.sort()

    i = 0

    for code in lst:
        df_tmp = df[df["ts_code"] == code]

        ed = list(df_tmp["end_date"].unique())

        for e in ed:
            logging.info(__file__ + ": mainbz " + code + " end date " + e + ". ")

            df_code_date = df_tmp[df_tmp["end_date"] == e]

            df_code_date_cny = df_code_date[df_code_date["curr_type"] == "CNY"]
            df_code_date_usd = df_code_date[df_code_date["curr_type"] == "USD"]

            ts_code = code
            end_date = e
            name = df_code_date.iloc[0]["name"]
            # USD_DIV_CNY=6

            bz_sales = df_code_date_cny.bz_sales.sum() + df_code_date_usd.bz_sales.sum() * USD_DIV_CNY
            bz_profit = df_code_date_cny.bz_profit.sum() + df_code_date_usd.bz_profit.sum() * USD_DIV_CNY
            bz_cost = df_code_date_cny.bz_cost.sum() + df_code_date_usd.bz_cost.sum() * USD_DIV_CNY

            df_result.loc[i] = pd.Series({"ts_code": ts_code, "name": name, "end_date": end_date, "bz_sales": bz_sales, "bz_profit": bz_profit, "bz_cost": bz_cost})

            i += 1

    df_result.to_csv(csv_fina_mainbz_sum, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "sum of fina_mainbz saved to " + csv_fina_mainbz_sum + " , len " + str(df_result.__len__()))
    return df


def percent_fina_mainbz():

    if (not force_run_global) and finlib.Finlib().is_cached(csv_fina_mainbz_latest_percent, day=6):
        logging.info(__file__ + " " + "skip file, it been updated in 6 day. " + csv_fina_mainbz_latest_percent)
        return


    # df = pd.read_csv(csv_fina_mainbz_p_latest, converters={"end_date": str})
    df = pd.read_csv(csv_fina_mainbz_p, converters={"end_date": str})

    df = df[~df['bz_item'].str.contains("\\(行业\\)")]

    check_date = finlib.Finlib().get_report_publish_status()['completed_year_rpt_date']
    # check_date = finlib.Finlib().get_report_publish_status()['completed_half_year_rpt_date']
    df = df[df['end_date']==check_date].reset_index().drop('index', axis=1)

    df = df.fillna(0)

    df_result = pd.DataFrame()

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=["perc_sales"])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=["perc_profit"])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=["perc_cost"])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=["bz_cnt"])
    df = new_value_df.join(df)

    lst = list(df["ts_code"].unique())
    lst.sort()

    i = 0

    for code in lst:
        i += 1
        logging.info(__file__ + " percent_fina_mainbz " + code + " " + str(i) + " of " + str(lst.__len__()))

        df_tmp = df[df["ts_code"] == code].reset_index().drop("index", axis=1)

        df_code_date_cny = df_tmp[df_tmp["curr_type"] == "CNY"]
        df_code_date_usd = df_tmp[df_tmp["curr_type"] == "USD"]

        cost_sum = df_code_date_cny["bz_cost"].sum() + df_code_date_usd["bz_cost"].sum() * USD_DIV_CNY
        profit_sum = df_code_date_cny["bz_profit"].sum() + df_code_date_usd["bz_profit"].sum() * USD_DIV_CNY
        sales_sum = df_code_date_cny["bz_sales"].sum() + df_code_date_usd["bz_sales"].sum() * USD_DIV_CNY

        for j in range(df_tmp.__len__()):
            df_tmp.iloc[j, df_tmp.columns.get_loc("bz_cnt")] = df_tmp.__len__()

            if cost_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_cost")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_cost")] / cost_sum, 0)

            if profit_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_profit")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_profit")] / profit_sum, 0)

            if sales_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_sales")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_sales")] / sales_sum, 0)

            if cost_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_cost")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_cost")] * USD_DIV_CNY / cost_sum, 0)

            if profit_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_profit")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_profit")] * USD_DIV_CNY / profit_sum, 0)

            if sales_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc("curr_type")]:
                df_tmp.iloc[j, df_tmp.columns.get_loc("perc_sales")] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc("bz_sales")] * USD_DIV_CNY / sales_sum, 0)

        df_result = df_result.append(df_tmp)

    df_result = df_result.reset_index().drop("index", axis=1)
    df_result = df_result[["ts_code", "name", "end_date", "bz_cnt", "perc_cost", "perc_profit", "perc_sales", "bz_cost", "bz_item", "bz_profit", "bz_sales", "curr_type"]]

    df_result.to_csv(csv_fina_mainbz_latest_percent, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "percent of fina_mainbz saved to " + csv_fina_mainbz_latest_percent + "  , len " + str(df_result.__len__()))
    return df



#''' COMMENT THIS FUC TO SEE IF ANY OTHER USES IT
#########################
# merge all 9 tables to one table, "merged_all_"+end_date+".csv"
# input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
# output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_*.csv
######################
def merge_quarterly(fast=False):
    # requires lots memory. >10 GB estimated.

    # df_income = pd.read_csv(csv_income, converters={'end_date': str})
    df_income = pd.read_csv(csv_income, converters={i: str for i in range(1000)})

    lst = []

    if not fast:
        lst = list(df_income["end_date"].unique())
        lst.sort()
    else:
        # lst = finlib.Finlib().get_report_publish_status()['period_to_be_checked_lst']  # period to be checked at this time point (based on month)
        lst = finlib.Finlib().get_year_month_quarter()["fetch_most_recent_report_perid"][0]  # period to be checked at this time point (based on month)

    # df_balancesheet = pd.read_csv(csv_balancesheet, converters={'end_date': str})
    df_balancesheet = pd.read_csv(csv_balancesheet, converters={i: str for i in range(1000)})  # convert all columns as string
    # df_cashflow = pd.read_csv(csv_cashflow, converters={'end_date': str})
    df_cashflow = pd.read_csv(csv_cashflow, converters={i: str for i in range(1000)})
    # df_fina_indicator = pd.read_csv(csv_fina_indicator, converters={'end_date': str})
    df_fina_indicator = pd.read_csv(csv_fina_indicator, converters={i: str for i in range(1000)})
    # df_fina_audit = pd.read_csv(csv_fina_audit, converters={'end_date': str})
    df_fina_audit = pd.read_csv(csv_fina_audit, converters={i: str for i in range(1000)})
    # df_fina_mainbz = pd.read_csv(csv_fina_mainbz_sum, converters={'end_date': str})
    df_fina_mainbz = pd.read_csv(csv_fina_mainbz_sum, converters={i: str for i in range(1000)})

    # do not need this df.
    # df_disclosure_date = pd.read_csv(csv_disclosure_date, converters={'end_date': str})
    # df_disclosure_date = pd.read_csv(csv_disclosure_date, converters={i: str for i in range(1000)})

    for i in lst:
        _merge_quarterly(i, df_income, df_balancesheet, df_cashflow, df_fina_indicator, df_fina_audit, df_fina_mainbz)


#'''


#''' COMMENT THIS FUC TO SEE IF ANY OTHER USES IT
def _merge_quarterly(end_date, df_income, df_balancesheet, df_cashflow, df_fina_indicator, df_fina_audit, df_fina_mainbz):
    if not os.path.exists(fund_base_merged):
        os.makedirs(fund_base_merged)

    output_csv = fund_base_merged + "/merged_all_" + end_date + ".csv"

    if (not force_run_global) and finlib.Finlib().is_cached(output_csv, day=6):
        logging.info(__file__ + " " + "file has been updated in 2 days, will not calculate. " + output_csv)
        return

    i = 0
    logging.info(__file__ + " " + "\n==== " + end_date + " ====")

    logging.info(__file__ + " " + "\tdf_income, ")
    sys.stdout.flush()

    df_income = df_income[df_income["end_date"] == end_date]
    df_income = df_income.drop_duplicates()
    df_income = remove_dup_record(df_input=df_income, csv_name="df_income")

    df_result_d = df_income

    cols = str(df_result_d.columns.__len__())
    lens = str(df_result_d.__len__())
    logging.info(__file__ + " " + "cols " + cols + ", lens " + lens)

    # logging.info(df_result_d[df_result_d['ts_code']=='000001.SZ'].__len__())

    #@ryan todo: check if df_fina_mainbz should be here
    # for df_name in ["df_balancesheet", "df_cashflow", "df_fina_indicator", "df_fina_audit", "df_fina_mainbz"]:
    for df_name in ["df_balancesheet", "df_cashflow", "df_fina_indicator", "df_fina_audit"]:
        # i += 1
        # suffix = "_x"+str(i)
        suffix = "_" + df_name
        logging.info(__file__ + " " + "\t" + df_name + ", ")
        sys.stdout.flush()

        df = eval(df_name)

        df = df[df["end_date"] == end_date]
        df = df.drop("name", axis=1)
        # if 'ann_date' in list(df.columns):
        #    df = df.drop('ann_date', axis=1)
        df = df.drop_duplicates()
        df = remove_dup_record(df_input=df, csv_name=df_name)

        df_result_d = pd.merge(df_result_d, df, how="outer", on=["ts_code"], suffixes=("", suffix))
        cols = str(df_result_d.columns.__len__())
        lens = str(df_result_d.__len__())

        logging.info(__file__ + " " + "cols " + cols + ", lens " + lens)

    df_result_d = df_result_d.drop_duplicates()
    df_result_d.replace("", 0, inplace=True)  # replace '' value to 0, otherwise will cause score to NaN in later analyse step1.
    df_result_d = df_result_d.fillna(0)
    df_result_d.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "saved " + output_csv + " . len " + str(df_result_d.__len__()))


#'''


#########################
#
# input:source/*.csv
# output: source/latest/*.csv
######################
def extract_latest():
    if not os.path.exists(fund_base_latest):
        os.makedirs(fund_base_latest)

    _extract_latest(csv_input=csv_income, csv_output=csv_income_latest, feature="income", col_name_list=col_list_income)
    _extract_latest(csv_input=csv_balancesheet, csv_output=csv_balancesheet_latest, feature="balancesheet", col_name_list=col_list_balancesheet)
    _extract_latest(csv_input=csv_cashflow, csv_output=csv_cashflow_latest, feature="cashflow", col_name_list=col_list_cashflow)
    _extract_latest(csv_input=csv_forecast, csv_output=csv_forecast_latest, feature="forecast", col_name_list=col_list_forecast)
    _extract_latest(csv_input=csv_dividend, csv_output=csv_dividend_latest, feature="dividend", col_name_list=col_list_dividend)
    _extract_latest(csv_input=csv_express, csv_output=csv_express_latest, feature="express", col_name_list=col_list_express)
    _extract_latest(csv_input=csv_fina_indicator, csv_output=csv_fina_indicator_latest, feature="fina_indicator", col_name_list=col_list_fina_indicator)
    _extract_latest(csv_input=csv_fina_audit, csv_output=csv_fina_audit_latest, feature="fina_audit", col_name_list=col_list_fina_audit)
    _extract_latest(csv_input=csv_fina_mainbz_p, csv_output=csv_fina_mainbz_p_latest, feature="fina_mainbz_p", col_name_list=col_list_fina_mainbz)
    _extract_latest(csv_input=csv_fina_mainbz_sum, csv_output=csv_fina_mainbz_sum_latest, feature="fina_mainbz_sum", col_name_list=col_list_fina_mainbz)
    _extract_latest(csv_input=csv_disclosure_date, csv_output=csv_disclosure_date_latest, feature="disclosure_date", col_name_list=col_list_disclosure_date)


def _extract_latest(csv_input, csv_output, feature, col_name_list, ts_code=None, end_date=None):
    if not os.path.exists(csv_input):
        logging.info(__file__ + " " + "skip, input csv doesn't exist " + csv_input)
        return

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info(__file__ + " " + "skip file, it been updated in 1 day. " + csv_output)
        return

    if os.stat(csv_input).st_size == 0:
        logging.info(__file__ + " " + "skip, empty input file " + csv_input)
        return

    df_result = pd.DataFrame()
    df = pd.read_csv(csv_input, converters={i: str for i in range(200)})  # convert all columns as string

    if ts_code is not None:
        df = df[df["ts_code"] == ts_code]

    if end_date is not None:
        df = df[df["end_date"] == end_date]  # in format 20180630

    i = 1
    total = str(df["ts_code"].unique().__len__())

    for ts_code in df["ts_code"].unique():
        logging.info(str(i) + " of " + total + ", extracting latest " + feature + " of " + ts_code)
        i += 1
        df_tmp = df[df["ts_code"] == ts_code]
        max_date = df_tmp["end_date"].max()
        df_tmp = df_tmp[df_tmp["end_date"] == max_date]
        finlib.Finlib().pprint(df_tmp)
        df_result = df_result.append(df_tmp)
        pass

        # df_tmp = df_tmp.sort_values(by='end_date', ascending=False).reset_index(drop=True).head(1)
        # df_result = df_result.append(df_tmp.iloc[0])

    cols = df_result.columns.tolist()
    name_list = list(reversed(col_name_list))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info(__file__ + " " + "warning, no column named " + i + " in cols")

    df_result = df_result[cols]
    df_result.fillna(0, inplace=True)

    if df_result.__len__() > 0:
        logging.info(__file__ + " " + "\n=== DataFrame " + feature + " ===")
        logging.info(df_result.iloc[0].astype(str))
        logging.info(__file__ + " " + "\n")

    df_result.to_csv(csv_output, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "saved to " + csv_output + " . len " + str(df_result.__len__()))


def _analyze_step_1(end_date):

    # logging.info(__file__ + " " + "=== analyze step 1, "+end_date+" ===")
    # end_date in format 20171231
    output_dir = fund_base_report + "/step1"
    csv_output = fund_base_report + "/step1/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
        return

    df_on_market_date = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/instrument_A.csv")
    df_on_market_date = finlib.Finlib().add_market_to_code(df=df_on_market_date, dot_f=True, tspro_format=True)

    f = fund_base_merged + "/" + "merged_all_" + end_date + ".csv"

    if not os.path.isfile(f):
        logging.info(__file__ + " " + "input file not found " + f)
        return

    # df['net_profit'].describe()
    df = pd.read_csv(f, converters={"ts_code": str, "end_date": str})

    #ryan debug
    # df = finlib.Finlib()._remove_garbage_must(df, b_m_score=2, n_year=1)

    # profit > 1E+8.  Have exception while loading on haha_65
    # df = df[df.net_profit > 1E+6] #1 million
    # df = df[df.bz_profit > 1E+8] #0.1 billion
    # df = df[~df.name.str.contains("ST")] #remove ST

    if debug_global:  # ryan debug
        df = df[df["ts_code"] == "600519.SH"].reset_index().drop("index", axis=1)

    # ryan_debug start
    # df = df.loc[df['ts_code'].isin(['000029.SZ', '600511.SH', '600535.SH', '600406.SH', '600519.SH', '600520.SH', '600518.SH', '600503.SH', '600506.SH'])].reset_index().drop('index', axis=1)
    # df = df.loc[df['ts_code'].isin(['000029.SZ', '600511.SH'])].reset_index().drop('index', axis=1)
    # ryan_debug start

    lst = list(df["ts_code"].unique())
    lst.sort()

    df = pd.DataFrame([0] * df.__len__(), columns=["finExpToGr"]).join(df)  # 财务费用/营业总收入, insert to header
    df = pd.DataFrame([0] * df.__len__(), columns=["optPrftM"]).join(df)  # 营业利润率
    df = pd.DataFrame([0] * df.__len__(), columns=["cashProfitM"]).join(df)  # 长周期来看（10年以上），净利润应该跟经营活动现金流量净额相等或近似相等，即净现比约等于1，当然越大越好。
    df = pd.DataFrame([0] * df.__len__(), columns=["lightAssert"]).join(df)  # 轻资产
    df = pd.DataFrame([0] * df.__len__(), columns=["sum_assert"]).join(df)  # 固定资产+在建工程+工程物资+无形资产里的土地
    df = pd.DataFrame([0] * df.__len__(), columns=["sumRcv"]).join(df)  # sum_应收
    df = pd.DataFrame([0] * df.__len__(), columns=["sumRcvNet"]).join(df)  # sum_应收-应收票据
    df = pd.DataFrame([0] * df.__len__(), columns=["srnAsstM"]).join(df)  # （sum-应收票据）/资产总计
    df = pd.DataFrame([0] * df.__len__(), columns=["bzsAstM"]).join(df)  # bz_sales/total_assets
    df = pd.DataFrame([0] * df.__len__(), columns=["revAbnM"]).join(df)  # （c_fr_sale_sg + sum_应收）/revenue. 靠近0标准，-10~10正常，数字为正且大好，说明交税少
    df = pd.DataFrame([0] * df.__len__(), columns=["boolrevAbn"]).join(df)  # bool_revAbn=1 if <-10 or >10
    df = pd.DataFrame([0] * df.__len__(), columns=["cashPrfAbn"]).join(df)  # 判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=["saleIncAbn"]).join(df)  # 判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=["cashInvAbn"]).join(df)  # 判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=["cashInc"]).join(df)  # 　现金及现金等价物净增加额　＋ 分配股利、利润或偿付利息支付的现金
    df = pd.DataFrame([0] * df.__len__(), columns=["cashLiabM"]).join(df)  # 期末现金及现金等价物余额  / 负债合计 》 1
    df = pd.DataFrame([0] * df.__len__(), columns=["hen"]).join(df)  # 老母鸡型, =0 means not hen.
    df = pd.DataFrame([0] * df.__len__(), columns=["cow"]).join(df)  # 奶牛型, =0 means not cow
    df = pd.DataFrame([0] * df.__len__(), columns=["revCogM"]).join(df)  # 营业总收入/营业总成本
    df = pd.DataFrame([0] * df.__len__(), columns=["curAssliaM"]).join(df)  # 流动资产合计/流动负债合计
    df = pd.DataFrame([""] * df.__len__(), columns=["bonusReason"]).join(df)
    df = pd.DataFrame([""] * df.__len__(), columns=["garbageReason"]).join(df)  # 垃圾股原因, insert to header
    df = pd.DataFrame([""] * df.__len__(), columns=["bonusCnt"]).join(df)
    df = pd.DataFrame([""] * df.__len__(), columns=["garbageCnt"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["stopProcess"]).join(df)  # 垃圾股, insert to header

    # if ('name' in df.columns):  tmp_df = df['name']; df.drop('name', axis=1, inplace=True); df.insert(0, 'name', tmp_df)
    field = "name"
    if field in df.columns:
        tmp_df = df.pop(field)
        df.insert(0, field, tmp_df)

    field = "ts_code"
    if field in df.columns:
        tmp_df = df.pop(field)
        df.insert(0, field, tmp_df)

    basic_df = get_pro_basic()

    df_len = df.__len__()
    MAX_TOLERATED_DAYS_NO_PROFIT = 365 * 2  # 2 years

    for i in range(0, df_len):
        logging.info("\n" + __file__ + " " + "=== analyze step_1, " + str(i + 1) + " of " + str(df_len) + ".  " + end_date + " ===")

        garbageReason = ""
        bonusReason = ""
        bonusCnt = 0
        garbageCnt = 0

        ####xiao xiong start
        ts_code = df.iloc[i]["ts_code"]
        end_date = df.iloc[i]["end_date"]
        name = df.iloc[i]["name"]

        # if end_date == '20171231':
        #    pass  #debug

        if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
            logging.info(__file__ + " " + "stock has been not on market. " + ts_code + " , " + end_date)
            # df = df[df['ts_code'] != ts_code]  #remove the ts_code from df that saved in csv. <<< bug introduced.
            continue

        # ts_code = '002972.SZ'  #ryan debug
        on_market_days = df_on_market_date[df_on_market_date["ts_code"] == ts_code]["list_date_days_before"].values[0]
        logging.info(__file__ + " " + ts_code + " " + name + " , " + end_date + " , on market days " + str(on_market_days))
        sys.stdout.flush()

        # debug
        # logging.info(__file__+" "+"i is " + str(i))
        # continue

        # ryan debug start
        """
        if not re.match("\d{6}", end_date):
            logging.info(__file__+" "+"end date wrong "+end_date)
        continue

        exit()
        #ryan debug end
        """

        # profit_to_gr
        dict = _analyze_profit_to_gr(ts_code=ts_code, end_date=end_date, basic_df=basic_df)
        garbageReason += dict["garbageReason"]
        bonusReason += dict["bonusReason"]
        bonusCnt += dict["bonusCnt"]
        garbageCnt += dict["garbageCnt"]
        if dict["stopProcess"]:
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # # beneish
        # if (not ts_code in list(beneish_df['code'])):
        #     garbageReason += 'No beneish score found. '
        #     garbageCnt += 1
        #     df.iloc[i, df.columns.get_loc('stopProcess')] = 1
        # else:
        #     M_8v = beneish_df[beneish_df['ts_code'] == ts_code]['M_8v'].values[0]
        #     M_5v = beneish_df[beneish_df['ts_code'] == ts_code]['M_5v'].values[0]
        #
        #     if M_8v > 0:
        #         garbageReason += 'beneish M8v > 0.'
        #         garbageCnt += 1
        #         df.iloc[i, df.columns.get_loc('stopProcess')] = 1
        #
        #     if M_8v <= -2:
        #         bonusReason += 'beneish M8v < -2.'
        #         bonusCnt += 1

        dict = _analyze_xiaoxiong_ct(ts_code=ts_code, end_date=end_date, basic_df=basic_df)

        garbageReason += dict["garbageReason"]
        bonusReason += dict["bonusReason"]
        bonusCnt += dict["bonusCnt"]
        garbageCnt += dict["garbageCnt"]

        # white horse
        dict = _analyze_white_horse_ct(ts_code=ts_code, end_date=end_date, basic_df=basic_df)
        garbageReason += dict["garbageReason"]
        bonusReason += dict["bonusReason"]
        bonusCnt += dict["bonusCnt"]
        garbageCnt += dict["garbageCnt"]
        ####xiao xiong end

        # audit_result
        # 标准无保留意见|带强调事项段的无保留意见
        audit_result = df.iloc[i]["audit_result"]
        audit_result = str(audit_result)

        if not (audit_result == "标准无保留意见" or audit_result == "0" or audit_result == "0.0"):
            garbageReason += "audit_result:" + audit_result + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        n_income_attr_p = df.iloc[i]["n_income_attr_p"]  # 净利润(不含少数股东损益)
        net_profit = df.iloc[i]["net_profit"]  # 净利润

        # Ryan todo: adding checking condition that on-market > 5 years. in order to keep the stock in high speed development.
        if on_market_days > MAX_TOLERATED_DAYS_NO_PROFIT and net_profit < 0:
            garbageReason += "net profit < 0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if net_profit > 0 and n_income_attr_p / net_profit < 0.5:
            garbageReason += "minor stock holder shares major(>50%) profit" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # 流动资产合计 > 流动负债合计
        total_cur_assets = df.iloc[i]["total_cur_assets"]  # 流动资产合计
        total_cur_liab = df.iloc[i]["total_cur_liab"]  # 流动负债合计
        total_nca = df.iloc[i]["total_nca"]  # 非流动资产合计
        total_ncl = df.iloc[i]["total_ncl"]  # 非流动负债合计
        total_revenue = df.iloc[i]["total_revenue"]  # 营业总收入
        total_cogs = df.iloc[i]["total_cogs"]  # 营业总成本
        n_income = df.iloc[i]["n_income"]  # 净利润(含少数股东损益)
        n_income_attr_p = df.iloc[i]["n_income_attr_p"]  # 归属于母公司所有者的净利润

        if on_market_days > MAX_TOLERATED_DAYS_NO_PROFIT and n_income < 0:
            garbageReason += "n_income 净利润(含少数股东损益) <  0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if on_market_days > MAX_TOLERATED_DAYS_NO_PROFIT and n_income_attr_p < 0:
            garbageReason += "n_income_attr_p 归属于母公司所有者的净利润 <  0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if on_market_days > MAX_TOLERATED_DAYS_NO_PROFIT and total_cur_assets < total_cur_liab:
            garbageReason += "total_cur_assets <  total_cur_liab" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if total_cur_assets < total_nca:
            garbageReason += "total_cur_assets <  total_nca" + ". "
            garbageCnt += 1

        if total_cogs < 0:
            garbageReason += "total_cogs <  0" + ". "
            garbageCnt += 1

        if total_revenue < total_cogs:
            garbageReason += "total_revenue <  total_cogs" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        revCogM = finlib.Finlib().measureValue(total_revenue, total_cogs)  # 营业总收入/营业总成本
        curAssliaM = finlib.Finlib().measureValue(total_cur_assets, total_cur_liab)  # 流动资产合计/流动负债合计

        df.iloc[i, df.columns.get_loc("revCogM")] = round(revCogM, 1)
        df.iloc[i, df.columns.get_loc("curAssliaM")] = round(curAssliaM, 1)

        # finExpToGr

        fin_exp = df.iloc[i]["fin_exp"]
        total_revenue = df.iloc[i]["total_revenue"]
        if total_revenue != 0.0:
            finExpToGr = fin_exp * 100 / total_revenue
            df.iloc[i, df.columns.get_loc("finExpToGr")] = round(finExpToGr, 1)

        if total_revenue != 0.0 and total_revenue < 1000000:
            garbageReason += "total_revenue less than 1M:" + str(total_revenue) + ". "
            df.iloc[i, df.columns.get_loc("garbageReason")] += garbageReason + ". "
            garbageCnt += 1

        # optPrftM
        operate_profit = df.iloc[i]["operate_profit"]
        revenue = df.iloc[i]["revenue"]
        if revenue != 0.0:
            optPrftM = operate_profit * 100 / revenue
            df.iloc[i, df.columns.get_loc("optPrftM")] = round(optPrftM, 1)

        if operate_profit != 0.0 and operate_profit < 1000000:
            garbageReason += "operate_profit less than 1M:" + str(operate_profit) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if revenue != 0.0 and revenue < 1000000:
            garbageReason += "revenue less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # cashProfitM
        n_cashflow_act = df.iloc[i]["n_cashflow_act"]
        net_profit = df.iloc[i]["net_profit"]
        if n_cashflow_act != 0.0 and net_profit != 0.0:
            cashProfitM = n_cashflow_act * 100 / net_profit
            df.iloc[i, df.columns.get_loc("cashProfitM")] = round(cashProfitM, 1)

        if n_cashflow_act != 0.0 and n_cashflow_act < 1000000:
            garbageReason += "n_cashflow_act less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if net_profit != 0.0 and net_profit < 1000000:
            garbageReason += "net_profit less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # bzsAstM
        c_fr_sale_sg = df.iloc[i]["c_fr_sale_sg"]
        total_assets = df.iloc[i]["total_assets"]

        if c_fr_sale_sg != 0.0 and total_assets != 0.0:
            bzsAstM = c_fr_sale_sg * 100 / total_assets
            df.iloc[i, df.columns.get_loc("bzsAstM")] = round(bzsAstM, 1)

        if c_fr_sale_sg != 0.0 and c_fr_sale_sg < 1000000:
            garbageReason += "c_fr_sale_sg less than 1M:" + str(c_fr_sale_sg) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        if total_assets != 0.0 and total_assets < 1000000:
            garbageReason += "total_assets less than 1M:" + str(total_assets) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # cashLiabM
        c_cash_equ_end_period = df.iloc[i]["c_cash_equ_end_period"]
        total_liab = df.iloc[i]["total_liab"]
        if c_cash_equ_end_period != 0.0 and total_liab != 0.0:
            cashLiabM = finlib.Finlib().measureValue(c_cash_equ_end_period, total_liab)
            df.iloc[i, df.columns.get_loc("cashLiabM")] = round(cashLiabM, 1)

            if cashLiabM < 1:
                garbageReason += "cashLiabM less than 1:" + str(cashLiabM) + ". "
                garbageCnt += 1

        # cashInc
        n_incr_cash_cash_equ = df.iloc[i]["n_incr_cash_cash_equ"]
        c_pay_dist_dpcp_int_exp = df.iloc[i]["c_pay_dist_dpcp_int_exp"]
        cashInc = n_incr_cash_cash_equ + c_pay_dist_dpcp_int_exp
        df.iloc[i, df.columns.get_loc("cashInc")] = round(cashInc, 0)

        if cashInc < 0:
            garbageReason += "cashInc less than 0:" + str(cashInc) + ". "
            garbageCnt += 1

        # saleIncAbn
        c_fr_sale_sg = df.iloc[i]["c_fr_sale_sg"]
        revenue = df.iloc[i]["revenue"]

        saleIncAbn = c_fr_sale_sg - revenue
        df.iloc[i, df.columns.get_loc("saleIncAbn")] = round(saleIncAbn, 0)

        if saleIncAbn < 0:
            garbageReason += "saleIncAbn less than 0:" + str(saleIncAbn) + ". "
            garbageCnt += 1

        # hen/cow
        n_cashflow_act = df.iloc[i]["n_cashflow_act"]
        n_cashflow_inv_act = df.iloc[i]["n_cashflow_inv_act"]
        n_cash_flows_fnc_act = df.iloc[i]["n_cash_flows_fnc_act"]

        val = abs(n_cashflow_act) + abs(n_cashflow_inv_act) + abs(n_cash_flows_fnc_act)
        val = round(val, 0)

        if n_cashflow_act > 0 and n_cashflow_inv_act >= 0 and n_cash_flows_fnc_act <= 0:
            df.iloc[i, df.columns.get_loc("hen")] = val
        elif n_cashflow_act > 0 and n_cashflow_inv_act <= 0 and n_cash_flows_fnc_act <= 0:
            df.iloc[i, df.columns.get_loc("cow")] = val

        if n_cashflow_act < 0:
            garbageReason += "n_cashflow_act less than 0:" + str(n_cashflow_act) + ". "
            garbageCnt += 1

        # lightAssert
        total_profit = df.iloc[i]["total_profit"]
        fix_assets = df.iloc[i]["fix_assets"]
        cip = df.iloc[i]["cip"]
        const_materials = df.iloc[i]["const_materials"]
        intan_assets = df.iloc[i]["intan_assets"]

        sum_assert = fix_assets + cip + const_materials + intan_assets
        df.iloc[i, df.columns.get_loc("sum_assert")] = round(sum_assert, 1)

        if sum_assert != 0.0:
            lightAssert = total_profit * 100 / sum_assert
            df.iloc[i, df.columns.get_loc("lightAssert")] = round(lightAssert, 1)

            if lightAssert < 12:  # 6%*2, none risky return
                garbageReason += "lightAssert less than 12:" + str(lightAssert) + ". "
                garbageCnt += 1

        # sumRcv, sumRcvNet, srnAsstM
        accounts_receiv = df.iloc[i]["accounts_receiv"]
        oth_receiv = df.iloc[i]["oth_receiv"]
        div_receiv = df.iloc[i]["div_receiv"]
        int_receiv = df.iloc[i]["int_receiv"]
        premium_receiv = df.iloc[i]["premium_receiv"]
        reinsur_receiv = df.iloc[i]["reinsur_receiv"]
        reinsur_res_receiv = df.iloc[i]["reinsur_res_receiv"]
        lt_rec = df.iloc[i]["lt_rec"]
        rr_reins_une_prem = df.iloc[i]["rr_reins_une_prem"]
        rr_reins_outstd_cla = df.iloc[i]["rr_reins_outstd_cla"]
        rr_reins_lins_liab = df.iloc[i]["rr_reins_lins_liab"]
        rr_reins_lthins_liab = df.iloc[i]["rr_reins_lthins_liab"]
        invest_as_receiv = df.iloc[i]["invest_as_receiv"]
        acc_receivable = df.iloc[i]["acc_receivable"]

        notes_receiv = df.iloc[i]["notes_receiv"]

        total_assets = df.iloc[i]["total_assets"]

        sumRcv = accounts_receiv + oth_receiv + div_receiv + int_receiv + premium_receiv + reinsur_receiv + reinsur_res_receiv + lt_rec + rr_reins_une_prem + rr_reins_outstd_cla + rr_reins_lins_liab + rr_reins_lthins_liab + invest_as_receiv + acc_receivable

        sumRcvNet = sumRcv - notes_receiv

        df.iloc[i, df.columns.get_loc("sumRcv")] = round(sumRcv, 1)
        df.iloc[i, df.columns.get_loc("sumRcvNet")] = round(sumRcvNet, 1)

        if total_assets != 0.0:
            srnAsstM = sumRcvNet * 100 / total_assets
            df.iloc[i, df.columns.get_loc("srnAsstM")] = round(srnAsstM, 1)

            if srnAsstM > 30:  # （sum-应收票据）/资产总计
                garbageReason += "srnAsstM great than 30:" + str(srnAsstM) + ". "
                garbageCnt += 1
                df.iloc[i, df.columns.get_loc("stopProcess")] = 1

        # revAbnM
        c_fr_sale_sg = df.iloc[i]["c_fr_sale_sg"]
        revenue = df.iloc[i]["revenue"]
        if revenue != 0.0:
            rst = (c_fr_sale_sg + sumRcv) / revenue
            revAbnM = (1.17 - rst) * 100.0 / 1.17  # 靠近0标准，-10~10正常，（偏幅10%）数字为正且大好，说明交税少

            if revAbnM < -90 or revAbnM > 20:  # abnormal
                df.iloc[i, df.columns.get_loc("boolrevAbn")] = 1
                df.iloc[i, df.columns.get_loc("revAbnM")] = -10
                garbageReason += "revAbnM <-90 or >20:" + str(revAbnM) + ". "
                garbageCnt += 1

            else:
                df.iloc[i, df.columns.get_loc("boolrevAbn")] = 0
                df.iloc[i, df.columns.get_loc("revAbnM")] = round(revAbnM, 1)

        df.iloc[i, df.columns.get_loc("garbageCnt")] = garbageCnt
        df.iloc[i, df.columns.get_loc("garbageReason")] = garbageReason
        df.iloc[i, df.columns.get_loc("bonusCnt")] = bonusCnt
        df.iloc[i, df.columns.get_loc("bonusReason")] = bonusReason
        df.to_csv(csv_output, encoding="UTF-8", index=False)  # save csv every line
        # logging.info(csv_output)

    if not df.empty:
        df.to_csv(csv_output, encoding="UTF-8", index=False)  # only save when all complete. or save nothing. so we have intacted result.
        logging.info(__file__ + ": " + "analysze step 1 result saved to " + csv_output + " . len " + str(df.__len__()))


def _analyze_xiaoxiong_ct(ts_code, end_date, basic_df):
    logging.info(__file__ + " " + "=== analyze _analyze_xiaoxiong_ct, " + end_date + " ===")

    # changtou xueyuan, xiaoxiong di li
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0

    dict_rtn = {}
    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    # if re.match('\d{4}0630$', end_date) or re.match('\d{4}1231$', end_date):
    # if re.match('20180630', end_date) or re.match('20171231', end_date): #ryan debug
    # if True:
    #    pass
    # else:
    #    logging.info(__file__+" "+"_analyze_xiaoxiong_ct: not handle Q1, Q3 report, " + end_date)
    #    return

    date_match = re.match("(\d{4})(\d{2})(\d{2})$", end_date)

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info(__file__ + " " + "stock has been not on market. " + ts_code + " , " + end_date)
        return dict_rtn

    if not (date_match):
        logging.info(__file__ + " " + "Error, date format unknown " + end_date)
        return dict_rtn

    year = int(date_match.group(1))
    month = int(date_match.group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)
    ann_date_this = tmp["ann_date"]
    ann_date_4q_before = tmp["ann_date_4q_before"]
    ann_date_8q_before = tmp["ann_date_8q_before"]

    this_revenue = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="revenue", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_revenue_4q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field="revenue", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_revenue_8q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field="revenue", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    this_accounts_receiv = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="accounts_receiv", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_accounts_receiv_4q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field="accounts_receiv", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_accounts_receiv_8q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field="accounts_receiv", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    try:
        rule_1_year_1 = (this_revenue - this_revenue_4q_before) - (this_accounts_receiv - this_accounts_receiv_4q_before)
        rule_1_year_2 = (this_revenue_4q_before - this_revenue_8q_before) - (this_accounts_receiv_4q_before - this_accounts_receiv_8q_before)
        if rule_1_year_1 < 0 and rule_1_year_2 < 0:  # bigger is better
            # logging.info(__file__+" "+"garbage")
            # 连续两年应收账款上升幅度超过营业收入上升幅度，没赚钱，收到白条。
            garbageReason += "Accounts receivable increased more than business income for two consecutive years.连续两年应收账款上升幅度超过营业收入上升幅度，没赚钱，收到白条。 "
            garbageCnt += 1
        else:
            pass
            # logging.info(__file__+" "+"pass rule 1")
    except:
        pass

    this_inventories = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="inventories", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_inventories_4q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field="inventories", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_inventories_8q_before = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field="inventories", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    try:
        rule_2_year_1 = (this_revenue - this_revenue_4q_before) - (this_inventories - this_inventories_4q_before)
        rule_2_year_2 = (this_revenue_4q_before - this_revenue_8q_before) - (this_inventories_4q_before - this_inventories_8q_before)
        if rule_2_year_1 < -10e7 and rule_2_year_2 < -10e7:  # bigger is better
            # 连续两年存货上升幅度超过营业收入上升幅度，产品滞销
            garbageReason += "Increase of inventory more than increase of business income for two consecutive years. 连续两年存货上升幅度超过营业收入上升幅度，产品滞销 "
            garbageCnt += 1
        else:
            pass
            # logging.info(__file__+" "+"pass rule 2")
    except:
        pass

    this_total_cur_liab = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="total_cur_liab", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_total_cur_assets = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="total_cur_assets", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    try:
        rule_3_year_1 = this_total_cur_assets * 1.0 / (this_total_cur_liab + 1)
        if this_total_cur_liab > 0 and this_total_cur_assets > 0 and rule_3_year_1 < 0.1:  # bigger is better
            # 流动负债远大于流动资产
            garbageReason += "Current liabilities far outweigh current assets. 流动负债远大于流动资产 "
            garbageCnt += 1
        else:
            pass
            # logging.info(__file__+" "+"pass rule 3")
    except:
        pass

    this_c_inf_fr_operate_a = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="c_inf_fr_operate_a", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)  # 经营活动现金流入小计
    this_st_cash_out_act = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="st_cash_out_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)  # 经营活动现金流出小计
    this_net_profit = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="net_profit", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)  # 净利润 (元，下同)

    try:
        rule_4_year_1 = (this_c_inf_fr_operate_a - this_st_cash_out_act) - this_net_profit
        if rule_4_year_1 > 10e6:  # bigger is better
            # 经营活动现金流量净值大于净利润
            bonusReason += "Net cash flow of operating activities > net profit. 经营活动现金流量净值大于净利润 "
            bonusCnt += 1
            # logging.info(__file__+" "+"bonus, rule 4")
    except:
        pass

    this_free_cashflow = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field="free_cashflow", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    try:
        if this_free_cashflow > 10e7:  # bigger is better
            bonusReason += "Free cashflow > 10E7. "
            bonusCnt += 1
            # logging.info(__file__+" "+"bonus, rule 5")
    except:
        pass

    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    return dict_rtn


def _analyze_white_horse_ct(ts_code, end_date, basic_df):
    logging.info(__file__ + " " + "=== analyze _analyze_white_horse_ct, " + end_date + " ===")
    # changtou bai ma gu
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0

    dict_rtn = {}
    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info(__file__ + " " + "stock has been not on market. " + ts_code + " , " + end_date)
        return dict_rtn

    if re.match("\d{4}0630$", end_date) or re.match("\d{4}1231$", end_date) or re.match("201[9|8|7|6|5]", end_date) or re.match("202[0|1|2|3|4|5]", end_date):
        # if re.match('20180630', end_date) or re.match('20171231', end_date):
        pass
    else:
        return dict_rtn

    year = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(1))
    month = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)

    #### White Horse Stock of  Chang tou xue yuan
    this_roe = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    this_pb = finlib.Finlib().get_ts_quarter_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="pb", base_dir=fund_base)
    # this_or_yoy = finlib.Finlib().get_ts_quarter_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="or_yoy", base_dir=fund_base)
    # this_equity_yoy = finlib.Finlib().get_ts_quarter_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="equity_yoy", base_dir=fund_base)
    # this_ocf_yoy = finlib.Finlib().get_ts_quarter_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="ocf_yoy", base_dir=fund_base)

    # if this_pb is None:
    #    this_pb = 0

    roe_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    roe_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    roe_3y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_3y_before"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    roe_4y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_4y_before"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    roe_5y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_5y_before"], field="roe", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    # 营业收入同比增长率(%)
    or_yoy_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="or_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    or_yoy_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="or_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    or_yoy_3y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_3y_before"], field="or_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # or_yoy_4y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_4y_before"], field="or_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # or_yoy_5y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_5y_before"], field="or_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)


    # 净资产同比增长率
    equity_yoy_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="equity_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    equity_yoy_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="equity_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    equity_yoy_3y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_3y_before"], field="equity_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # equity_yoy_4y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_4y_before"], field="equity_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # equity_yoy_5y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_5y_before"], field="equity_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)


    # 经营活动产生的现金流量净额同比增长率(%)
    ocf_yoy_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="ocf_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    ocf_yoy_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="ocf_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    ocf_yoy_3y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_3y_before"], field="ocf_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # ocf_yoy_4y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_4y_before"], field="ocf_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    # ocf_yoy_5y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_5y_before"], field="ocf_yoy", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)



    roeC = 20
    or_yoy_c = 15
    equity_yoy_c =15
    ocf_yoy_c =15
    

    try:
        # at 2015, 5 years before is 2010, so bigload need load 2010
        if this_roe >= roeC and roe_1y >= roeC and roe_2y >= roeC and roe_3y >= roeC and roe_4y >= roeC and roe_5y >= roeC:
            bonusReason += "ROE > " + str(roeC) + " consecutively (6years). "
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

            if this_pb is not None and this_pb > 0 and this_pb < 8:
                bonusReason += "white horse "
                bonusCnt += 1
                logging.info(__file__ + " " + "bonus. " + bonusReason + " " + ts_code + " " + end_date)

        if or_yoy_1y >= or_yoy_c and or_yoy_2y >= or_yoy_c and or_yoy_3y >= or_yoy_c:
            bonusReason += " 营业收入同比增长率(%) or_yoy > " + str(or_yoy_c) + " consecutively (3years). "
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)
        elif or_yoy_1y < 0:
            garbageReason += "营业收入同比增长率(%) or_yoy < 0. "+str(round(or_yoy_1y, 2)+"; ")
            garbageCnt += 1
            stopProcess = False
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if equity_yoy_1y >= equity_yoy_c and equity_yoy_2y >= equity_yoy_c and equity_yoy_3y >= equity_yoy_c:
            bonusReason += "净资产同比增长率 equity_yoy > " + str(equity_yoy_c) + " consecutively (3years). "
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)
        elif equity_yoy_1y < 0:
            garbageReason += "净资产同比增长率 equity_yoy_1y < 0. "+str(round(equity_yoy_1y, 2)+"; ")
            garbageCnt += 1
            stopProcess = False
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if ocf_yoy_1y >= ocf_yoy_c and ocf_yoy_2y >= ocf_yoy_c and ocf_yoy_3y >= ocf_yoy_c:
            bonusReason += "经营活动产生的现金流量净额同比增长率(%) ocf_yoy > " + str(ocf_yoy_c) + " consecutively (3years). "
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)
        elif or_yoy_1y < 0:
            garbageReason += "经营活动产生的现金流量净额同比增长率(%) ocf_yoy_1y < 0. "+str(round(ocf_yoy_1y, 2)+"; ")
            garbageCnt += 1
            stopProcess = False
            logging.info(__file__ + " " + "garbage. " + garbageReason)





    except:
        pass

    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    return dict_rtn


def _analyze_profit_to_gr(ts_code, end_date, basic_df):
    logging.info(__file__ + " " + "=== analyze _analyze_profit_to_gr, " + end_date + "  ===")
    # changtou bai ma gu
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0
    stopProcess = False

    dict_rtn = {}
    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason
    dict_rtn["stopProcess"] = stopProcess

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info(__file__ + " " + "stock has been not on market. " + ts_code + " , " + end_date)
        return dict_rtn

    if re.match("\d{4}0630$", end_date) or re.match("\d{4}1231$", end_date) or re.match("201[9|8|7|6|5]", end_date) or re.match("202[0|1|2|3|4|5]", end_date):
        # if re.match('20180630', end_date) or re.match('20171231', end_date):
        pass
    else:
        return dict_rtn

    year = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(1))
    month = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)

    #### White Horse Stock of  Chang tou xue yuan
    this_profit_to_gr = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    profit_to_gr_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    profit_to_gr_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    this_n_cashflow_act = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    n_cashflow_act_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    n_cashflow_act_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    try:
        if this_profit_to_gr > profit_to_gr_1y and profit_to_gr_1y > profit_to_gr_2y:
            bonusReason += "profit_to_gr continue increase consecutively 3 years."
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if this_n_cashflow_act > n_cashflow_act_1y and n_cashflow_act_1y > n_cashflow_act_2y:
            bonusReason += "n_cashflow_act continue increase consecutively 3 years."
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if this_profit_to_gr > 20 and this_profit_to_gr < 101:
            bonusReason += "profit/gross income > 20%." + str(this_profit_to_gr)
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if this_profit_to_gr < 0:
            garbageReason += "profit_to_gr < 0." + str(this_profit_to_gr)
            garbageCnt += 1
            stopProcess = True
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if this_profit_to_gr > 100:  # manipulated finicial statement
            garbageReason += "manipulated, profit larger than gross income."
            garbageCnt += 1
            stopProcess = True
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if this_profit_to_gr < 10:
            garbageReason += "profit/gross income < 10% " + str(this_profit_to_gr) + "."
            garbageCnt += 1
            stopProcess = True
            logging.info(__file__ + " " + "garbage. " + garbageReason)

    except:
        pass

    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason
    dict_rtn["stopProcess"] = stopProcess

    return dict_rtn


def _analyze_beneish(ts_code, end_date, basic_df):
    logging.info(__file__ + " " + "=== analyze _analyze_profit_to_gr, " + end_date + "  ===")
    # changtou bai ma gu
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0

    dict_rtn = {}
    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info(__file__ + " " + "stock has been not on market. " + ts_code + " , " + end_date)
        return dict_rtn

    if re.match("\d{4}0630$", end_date) or re.match("\d{4}1231$", end_date) or re.match("201[9|8|7|6|5]", end_date) or re.match("202[0|1|2|3|4|5]", end_date):
        # if re.match('20180630', end_date) or re.match('20171231', end_date):
        pass
    else:
        return dict_rtn

    year = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(1))
    month = int(re.match("(\d{4})(\d{2})(\d{2})$", end_date).group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)

    #### White Horse Stock of  Chang tou xue yuan
    this_profit_to_gr = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    profit_to_gr_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    profit_to_gr_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="profit_to_gr", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    this_n_cashflow_act = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    n_cashflow_act_1y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_1y_before"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)
    n_cashflow_act_2y = finlib.Finlib().get_ts_field(ts_code=ts_code, ann_date=tmp["ann_date_2y_before"], field="n_cashflow_act", big_memory=big_memory_global, df_all_ts_pro=df_all_ts_pro, fund_base_merged=fund_base_merged)

    try:
        if this_profit_to_gr > profit_to_gr_1y and profit_to_gr_1y > profit_to_gr_2y:
            bonusReason += "profit_to_gr continue increase consecutively 3 years."
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if this_n_cashflow_act > n_cashflow_act_1y and n_cashflow_act_1y > n_cashflow_act_2y:
            bonusReason += "n_cashflow_act continue increase consecutively 3 years."
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if profit_to_gr > 70:
            bonusReason += "profit/gross income > 70%."
            bonusCnt += 1
            logging.info(__file__ + " " + "bonus. " + bonusReason)

        if profit_to_gr < 0:
            garbageReason += "profit_to_gr < 0."
            garbageCnt += 1
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if profit_to_gr > 100:
            garbageReason += "profit bigger than gross income."
            garbageCnt += 1
            logging.info(__file__ + " " + "garbage. " + garbageReason)

        if profit_to_gr < 30:
            garbageReason += "profit/gross income < 30%."
            garbageCnt += 1
            logging.info(__file__ + " " + "garbage. " + garbageReason)

    except:
        pass

    dict_rtn["garbageCnt"] = garbageCnt
    dict_rtn["bonusCnt"] = bonusCnt
    dict_rtn["bonusReason"] = bonusReason
    dict_rtn["garbageReason"] = garbageReason

    return dict_rtn


def _analyze_step_2(end_date):
    # end_date in format 20171231

    # add columns to the sheet
    logging.info(__file__ + " " + "=== analyze step 2, " + end_date + "  ===")

    csv_input = fund_base_report + "/step1/rpt_" + end_date + ".csv"
    output_dir = fund_base_report + "/step2"
    csv_output = output_dir + "/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if not os.path.isfile(csv_input):
        logging.info(__file__ + " " + "input file not found " + csv_input)
        return

    if os.stat(csv_input).st_size < 10:
        logging.info("empty input file " + csv_input)
        return

    df = pd.read_csv(csv_input, converters={"end_date": str})
    df = df[df["stopProcess"] != 1].reset_index().drop("index", axis=1)

    df = pd.DataFrame([0] * df.__len__(), columns=["scoreTotRev"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreGPM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreFiExpGr"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreSaExpGr"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreAdExpGr"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreTrYoy"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreSeExp"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreOpP"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreRevenue"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreOptPrftM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreNPN"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreNCA"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreNP"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCPM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreLA"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreSumAss"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreTotP"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreSumRcv"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreSumRcvNet"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoresSrnAsstM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreBzSa"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreBzAsM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCfrSSg"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreHen"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCow"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCI"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCLM"]).join(df)

    df = pd.DataFrame([0] * df.__len__(), columns=["scoreRevCogM"]).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreCurAssliaM"]).join(df)

    df = pd.DataFrame([0] * df.__len__(), columns=["score"]).join(df)  # the sum of all the score

    len = df.__len__()
    cols = df.columns.tolist()

    for i in range(len):
        logging.info(__file__ + " " + "analyze step_2 " + str(i) + " of " + str(len) + ". ")

        scoreTotRev = round(stats.percentileofscore(df["total_revenue"], df.iloc[i]["total_revenue"]), 2)
        df.iloc[i, df.columns.get_loc("scoreTotRev")] = scoreTotRev

        scoreRevCogM = round(stats.percentileofscore(df["revCogM"], df.iloc[i]["revCogM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreRevCogM")] = scoreRevCogM

        scoreCurAssliaM = round(stats.percentileofscore(df["curAssliaM"], df.iloc[i]["curAssliaM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCurAssliaM")] = scoreCurAssliaM

        scoreGPM = round(stats.percentileofscore(df["grossprofit_margin"], df.iloc[i]["grossprofit_margin"]), 2)
        df.iloc[i, df.columns.get_loc("scoreGPM")] = scoreGPM

        scoreFiExpGr = round(stats.percentileofscore(df["finExpToGr"], df.iloc[i]["finExpToGr"]), 2)
        df.iloc[i, df.columns.get_loc("scoreFiExpGr")] = 100 - scoreFiExpGr

        scoreSaExpGr = round(stats.percentileofscore(df["saleexp_to_gr"], df.iloc[i]["saleexp_to_gr"]), 2)
        df.iloc[i, df.columns.get_loc("scoreSaExpGr")] = 100 - scoreSaExpGr

        scoreAdExpGr = round(stats.percentileofscore(df["adminexp_of_gr"], df.iloc[i]["adminexp_of_gr"]), 2)
        df.iloc[i, df.columns.get_loc("scoreAdExpGr")] = 100 - scoreAdExpGr

        scoreTrYoy = round(stats.percentileofscore(df["tr_yoy"], df.iloc[i]["tr_yoy"]), 2)
        df.iloc[i, df.columns.get_loc("scoreTrYoy")] = scoreTrYoy

        scoreOpP = round(stats.percentileofscore(df["operate_profit"], df.iloc[i]["operate_profit"]), 2)
        df.iloc[i, df.columns.get_loc("scoreOpP")] = scoreOpP

        scoreRevenue = round(stats.percentileofscore(df["revenue"], df.iloc[i]["revenue"]), 2)
        df.iloc[i, df.columns.get_loc("scoreRevenue")] = scoreRevenue

        scoreOptPrftM = round(stats.percentileofscore(df["optPrftM"], df.iloc[i]["optPrftM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreOptPrftM")] = scoreOptPrftM

        scoreNPN = round(stats.percentileofscore(df["n_income_attr_p"], df.iloc[i]["n_income_attr_p"]), 2)
        df.iloc[i, df.columns.get_loc("scoreNPN")] = scoreNPN

        scoreNCA = round(stats.percentileofscore(df["n_cashflow_act"], df.iloc[i]["n_cashflow_act"]), 2)
        df.iloc[i, df.columns.get_loc("scoreNCA")] = scoreNCA

        scoreNP = round(stats.percentileofscore(df["net_profit"], df.iloc[i]["net_profit"]), 2)
        df.iloc[i, df.columns.get_loc("scoreNP")] = scoreNP

        scoreCPM = round(stats.percentileofscore(df["cashProfitM"], df.iloc[i]["cashProfitM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCPM")] = scoreCPM

        scoreLA = round(stats.percentileofscore(df["lightAssert"], df.iloc[i]["lightAssert"]), 2)
        df.iloc[i, df.columns.get_loc("scoreLA")] = scoreLA

        scoreSumAss = round(stats.percentileofscore(df["sum_assert"], df.iloc[i]["sum_assert"]), 2)
        df.iloc[i, df.columns.get_loc("scoreSumAss")] = scoreSumAss

        scoreTotP = round(stats.percentileofscore(df["total_profit"], df.iloc[i]["total_profit"]), 2)
        df.iloc[i, df.columns.get_loc("scoreTotP")] = scoreTotP

        scoreSumRcv = round(stats.percentileofscore(df["sumRcv"], df.iloc[i]["sumRcv"]), 2)
        df.iloc[i, df.columns.get_loc("scoreSumRcv")] = 100 - scoreSumRcv

        scoreSumRcvNet = round(stats.percentileofscore(df["sumRcvNet"], df.iloc[i]["sumRcvNet"]), 2)
        df.iloc[i, df.columns.get_loc("scoreSumRcvNet")] = 100 - scoreSumRcvNet

        scoresSrnAsstM = round(stats.percentileofscore(df["srnAsstM"], df.iloc[i]["srnAsstM"]), 2)
        df.iloc[i, df.columns.get_loc("scoresSrnAsstM")] = 100 - scoresSrnAsstM

        scoreBzSa = round(stats.percentileofscore(df["bz_sales"], df.iloc[i]["bz_sales"]), 2)
        df.iloc[i, df.columns.get_loc("scoreBzSa")] = scoreBzSa

        scoreBzAsM = round(stats.percentileofscore(df["bzsAstM"], df.iloc[i]["bzsAstM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreBzAsM")] = scoreBzAsM

        scoreCfrSSg = round(stats.percentileofscore(df["c_fr_sale_sg"], df.iloc[i]["c_fr_sale_sg"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCfrSSg")] = scoreCfrSSg

        scoreHen = round(stats.percentileofscore(df["hen"], df.iloc[i]["hen"]), 2)
        df.iloc[i, df.columns.get_loc("scoreHen")] = scoreHen

        scoreCow = round(stats.percentileofscore(df["cow"], df.iloc[i]["cow"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCow")] = scoreCow

        scoreCI = round(stats.percentileofscore(df["cashInc"], df.iloc[i]["cashInc"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCI")] = scoreCI

        scoreCLM = round(stats.percentileofscore(df["cashLiabM"], df.iloc[i]["cashLiabM"]), 2)
        df.iloc[i, df.columns.get_loc("scoreCLM")] = scoreCLM

        final_score = scoreTotRev * 1.5 + scoreGPM * 2 + scoreFiExpGr + scoreSaExpGr + scoreAdExpGr + scoreTrYoy + scoreOpP + scoreRevenue + scoreOptPrftM + scoreNPN + scoreNCA + scoreNP + scoreCPM * 2 + scoreLA + scoreSumAss + scoreTotP + scoreSumRcv + scoreSumRcvNet + scoresSrnAsstM + scoreBzSa * 2 + scoreBzAsM * 2 + scoreCfrSSg * 2 + scoreHen * 1.5 + scoreCow * 2 + scoreCI + scoreCLM + scoreRevCogM * 2 + scoreCurAssliaM * 2

        if df.iloc[i]["stopProcess"] == 1:
            final_score = -1

        df.iloc[i, df.columns.get_loc("score")] = final_score

    col_name_list_step2 = ["ts_code", "name", "score", "garbageReason"]
    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list_step2))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info(__file__ + " " + "warning, no column named " + i + " in cols")

    df = df[cols]

    df = df.sort_values("score", ascending=False, inplace=False)
    df = df.reset_index().drop("index", axis=1)

    if not df.empty:
        df.to_csv(csv_output, encoding="UTF-8", index=False)
        logging.info(__file__ + ": " + "analysze step 2 result saved to " + csv_output + " . len " + str(df.__len__()))
    else:
        logging.info(__file__ + ": " + "empty step 2 result")


def _analyze_step_3(end_date):
    # end_date in format 20171231

    # add columns to the sheet
    logging.info(__file__ + " " + "=== analyze step 3, " + end_date + "  ===")
    csv_input = fund_base_report + "/step2/rpt_" + end_date + ".csv"

    output_dir = fund_base_report + "/step3"
    csv_output = output_dir + "/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if not os.path.isfile(csv_input):
        logging.info(__file__ + " " + "input file not found " + csv_input)
        return

    df = pd.read_csv(csv_input, converters={"end_date": str})

    df = pd.DataFrame([0] * df.__len__(), columns=["sos"]).join(df)  # score of score

    len = df.__len__()
    cols = df.columns.tolist()

    for i in range(len):
        logging.info(__file__ + " " + "analyze step_3 " + str(i) + " of " + str(len) + ". ")

        sos = round(stats.percentileofscore(df["score"], df.iloc[i]["score"]), 2)
        df.iloc[i, df.columns.get_loc("sos")] = sos

    col_name_list_step3 = ["ts_code", "name", "sos", "score", "garbageReason"]
    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list_step3))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info(__file__ + " " + "warning, no column named " + i + " in cols")

    df = df[cols]

    df = df.sort_values("score", ascending=False, inplace=False)
    df = df.reset_index().drop("index", axis=1)

    if not df.empty:
        df.to_csv(csv_output, encoding="UTF-8", index=False)
        logging.info(__file__ + ": " + "analysze step 3 result saved to " + csv_output)

        sl_3 = "/home/ryan/DATA/result/latest_fundamental_year_pro.csv"
        if os.path.lexists(sl_3):
            os.unlink(sl_3)
        os.symlink(csv_output, sl_3)
        logging.info(__file__ + " " + "make symbol link " + sl_3 + " --> " + csv_output)


def _analyze_step_4():
    # end_date in format 20171231

    output_dir = fund_base_report + "/step4"
    csv_output = output_dir + "/multiple_years_score.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logging.info(__file__ + " " + "=== analyze step 4 ===")

    # if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
    #     logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
    #     return

    df = df_result = pd.DataFrame()

    x = os.listdir(fund_base_report + "/step3/")
    periord_list = []

    for f in x:
        csv_input = fund_base_report + "/step3/" + f  # f: rpt_200712313.csv

        periord = re.match("rpt_(\d{6}).*.csv", f).group(1)  # periord: 200712
        periord_list.append(periord)

        df_1 = pd.read_csv(csv_input, converters={"end_date": str})
        df_1 = df_1[["ts_code", "name", "sos", "end_date"]]

        df = df.append(df_1)
        # logging.info(__file__+" "+". len "+str(df.__len__()))

    uniq_ts_code = df["ts_code"].unique()

    periord_list.sort()
    for periord in periord_list:
        df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=[periord]).join(df_result)

    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=["number_in_top_30"]).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=["score_avg"]).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=["score_over_years"]).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=["name"]).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=["ts_code"]).join(df_result)

    i = 0
    for ts_code in uniq_ts_code:
        logging.info(__file__ + " " + "=== analyze step4 " + ts_code + " " + str(i) + " of " + str(df_result.__len__()) + " ===")

        stock_df = df[df["ts_code"] == ts_code].sort_values(by="end_date", ascending=False)
        name = stock_df.iloc[0]["name"]
        top_30_df = stock_df[stock_df["sos"] > 70].reset_index().drop("index", axis=1)
        top_30_df_num = top_30_df.__len__()

        df_result.loc[i] = pd.Series({"ts_code": ts_code, "name": name, "number_in_top_30": top_30_df_num})

        avg_n = avg_sum = 0

        score_over_years = 0

        # aaa = stock_df['end_date'].unique()
        # bbb = list(aaa).reverse()
        base = 0.95

        col_n = 0

        for d in stock_df["end_date"].unique():
            if (str(d) == "0") or (str(d) == "0.0") or (pd.isnull(d)):
                continue

            sd = re.match("(\d{6})\d\d", d).group(1)

            score_of_date = stock_df[stock_df["end_date"] == d].iloc[0]["sos"]

            if np.isnan(score_of_date):
                # score_over_years = 0
                continue

            df_result.iloc[i, df_result.columns.get_loc(sd)] = score_of_date

            avg_n += 1
            avg_sum += score_of_date
            factor = base ** col_n

            # logging.info(__file__+" "+"\tfactor "+str(factor)+". period "+str(sd))

            score_over_years += factor * score_of_date  # the latest * 1, next *0.7, next 0.7^2, next 0.7^3
            col_n += 1

        avg = 0
        if avg_n != 0:
            avg = round(avg_sum / avg_n, 2)

        df_result.iloc[i, df_result.columns.get_loc("score_avg")] = avg
        df_result.iloc[i, df_result.columns.get_loc("score_over_years")] = round(score_over_years)
        i += 1
        pass  # end of end_date for loop

    pass  # end of ts_code for loop

    if not df_result.empty:
        df_result = df_result.sort_values("score_over_years", ascending=False, inplace=False)
        df_result = df_result.reset_index().drop("index", axis=1)
        df_result.to_csv(csv_output, encoding="UTF-8", index=False)
        logging.info(__file__ + ": " + "analysze step 4 result saved to " + csv_output + " , len " + str(df_result.__len__()))


def _analyze_step_5():
    logging.info(__file__ + " " + "=== analyze step 5 ===")

    csv_input = fund_base_report + "/step4/multiple_years_score.csv"

    output_dir = fund_base_report + "/step5"
    csv_output = output_dir + "/multiple_years_score.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
    #     logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
    #     return

    df = pd.DataFrame()

    # logging.info(__file__+" "+"loading "+csv_input)
    df = pd.read_csv(csv_input)

    df = pd.DataFrame([0] * df.__len__(), columns=["scoreA"]).join(df)  # score of score over years

    for i in range(df.__len__()):
        ts_code = df.iloc[i]["ts_code"]
        logging.info(__file__ + " " + "analyze_step_5 " + str(i) + " of " + str(df.__len__()) + ". ")

        # score_over_years
        score_soy = round(stats.percentileofscore(df["score_over_years"], df.iloc[i]["score_over_years"]), 2)

        # score_avg
        score_sa = round(stats.percentileofscore(df["score_avg"], df.iloc[i]["score_avg"]), 2)

        # number_in_top_30
        score_nit30 = round(stats.percentileofscore(df["number_in_top_30"], df.iloc[i]["number_in_top_30"]), 2)

        df.iloc[i, df.columns.get_loc("scoreA")] = round((score_soy + score_sa + score_nit30) / 3.0, 2)

    if not df.empty:
        df = df.sort_values("scoreA", ascending=False, inplace=False)
        df = df.reset_index().drop("index", axis=1)
        df.to_csv(csv_output, encoding="UTF-8", index=False)
        logging.info(__file__ + ": " + "analysze step 5 result saved to " + csv_output + " , len " + str(df.__len__()))


def _analyze_step_6():
    csv_input_3 = fund_base_report + "/step5/multiple_years_score.csv"

    if debug_global:
        csv_input_1 = "/home/ryan/DATA/result/latest_fundamental_year_pro_debug.csv"  # symlink to /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_201806303.csv
    else:
        csv_input_1 = "/home/ryan/DATA/result/latest_fundamental_year_pro.csv"  # symlink to /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_20171231.csv

    csv_input_2 = "/home/ryan/DATA/result/latest_fundamental_quarter.csv"

    output_dir = fund_base_report + "/step6"
    csv_output = output_dir + "/multiple_years_score.csv"
    csv_selected_output = output_dir + "/multiple_years_score_selected.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logging.info(__file__ + " " + "=== analyze step 6 ===")

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info(__file__ + " " + "file has been updated in 6 days, will not calculate. " + csv_output)
        return

    # if (not force_run_global) and finlib.Finlib().is_cached(csv_selected_output, day=6):
    #     logging.info(__file__ + " " + "file has been updated in 6 days, will not calculate. " + csv_selected_output)
    #     return

    df = pd.DataFrame()

    # stock_code = "SZ000402"

    logging.info(__file__ + " " + "loading " + csv_input_1)
    df_1 = pd.read_csv(csv_input_1)
    df_1 = finlib.Finlib().ts_code_to_code(df=df_1)  # code:SH600519
    df_1 = finlib.Finlib().remove_garbage(df_1)
    # df_1 = df_1[df_1['code']==stock_code]

    logging.info(__file__ + " " + "loading " + csv_input_2)
    df_2 = pd.read_csv(csv_input_2, converters={"code": str})
    df_2 = finlib.Finlib().add_market_to_code(df=df_2)  # code: SH600519
    df_2 = finlib.Finlib().remove_garbage(df_2)

    if debug_global:
        df_2 = df_2[df_2["code"] == "SH600519"]

    logging.info(__file__ + " " + "loading " + csv_input_3)
    df_3 = pd.read_csv(csv_input_3)
    df_3 = finlib.Finlib().ts_code_to_code(df=df_3)
    df_3 = finlib.Finlib().remove_garbage(df_3)
    df_3 = df_3[["code", "scoreA"]]  # code: SH600519, scoreA:NaN

    df = pd.merge(df_1, df_2, how="outer", on=["code"], suffixes=("", "_stock_basics"))
    df = pd.merge(df, df_3, how="outer", on=["code"], suffixes=("", "_df_3"))
    df = df.fillna(0)

    # logging.info(df.head(10))

    # exit(0)

    if debug_global:
        df = df[df["code"] == "600519"]

    df = pd.DataFrame([0] * df.__len__(), columns=["ValuePrice"]).join(df)  # the stock price should be
    df = pd.DataFrame([0] * df.__len__(), columns=["CurrentPrice"]).join(df)  # the stock price  actually be
    df = pd.DataFrame([0] * df.__len__(), columns=["V_C_P"]).join(df)  # how value is the price now. bigger is better
    df = pd.DataFrame([0] * df.__len__(), columns=["VCP_P"]).join(df)  # percent of VCP
    df = pd.DataFrame([0] * df.__len__(), columns=["FCV"]).join(df)  # 企业自由现金流量估值
    df = pd.DataFrame([0] * df.__len__(), columns=["FCV_P"]).join(df)  # 企业自由现金流量估值
    df = pd.DataFrame([0] * df.__len__(), columns=["FCV_5V"]).join(df)  # 自由现金流量 5 years value
    df = pd.DataFrame([0] * df.__len__(), columns=["FCV_10V"]).join(df)  # 自由现金流量 10 years value
    # df = pd.DataFrame([0] * df.__len__(), columns=['FCV_20V']).join(df) #自由现金流量 20 years value
    df = pd.DataFrame([0] * df.__len__(), columns=["ROE_Cnt"]).join(df)  #
    df = pd.DataFrame([0] * df.__len__(), columns=["ROE_Mean"]).join(df)  #
    df = pd.DataFrame([200] * df.__len__(), columns=["FCV_NYears"]).join(df)  # 自由现金流量 after N years == current price
    df = pd.DataFrame([0] * df.__len__(), columns=["FCV_NYears_P"]).join(df)  # percent of ( 0 ~100)
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreB"]).join(df)  # scoreA + FCV_NYears_P + FCV_P + VCP_P
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreB_tmp"]).join(df)  #
    df = pd.DataFrame([0] * df.__len__(), columns=["scoreAB"]).join(df)  #

    for i in range(df.__len__()):
        code = df.iloc[i]["code"]
        logging.info(__file__ + " " + "analyze step_6 " + str(i) + " of " + str(df.__len__()) + ". ")

        guben = df.iloc[i]["totals"] * 10 ** 8  # totals,总股本(亿)

        if guben == 0:
            logging.info(__file__ + " " + "Fatal error, guben = 0, " + code)
            # exit(1)
            guben = 10 ** 20  # a very huge/big number,so result is very small to ignored.

        total_cur_assets = int(df.iloc[i]["total_cur_assets"])  # 流动资产合计
        total_nca = int(df.iloc[i]["total_nca"])  # 非流动资产合计
        CurrentPrice = finlib.Finlib().get_price(code_m=code)
        CurrentPrice = round(CurrentPrice, 2)
        free_cashflow = int(df.iloc[i]["free_cashflow"])  # 企业自由现金流量
        df.iloc[i, df.columns.get_loc("FCV")] = free_cashflow

        FCV_5V = FCV_10V = FCV_nV = 0
        Cash_Discount_Rate = 0.92

        if big_memory_global:
            regex_group = re.match("(\w{2})(\d{6})", code)  # sh600519 --> 600519.sh
            ts_code = regex_group.group(2) + "." + regex_group.group(1)

            df_the_code = df_all_ts_pro[df_all_ts_pro["ts_code"] == ts_code]
            df_the_code = df_the_code[df_the_code["roe"] != 0]
            roe_mean = df_the_code["roe"].mean()  # 600519 --> roe_mean 21.25

            if not np.isnan(roe_mean):
                df.iloc[i, df.columns.get_loc("ROE_Mean")] = round(roe_mean, 2)
                df.iloc[i, df.columns.get_loc("ROE_Cnt")] = df_the_code.__len__()
                Profit_Increase_Rate = 1 + round(roe_mean / 100, 2)
            else:
                Profit_Increase_Rate = 1.10
        else:
            Profit_Increase_Rate = 1.10

        for t_cnt in range(5):
            FCV_5V += free_cashflow * (Profit_Increase_Rate ** t_cnt) * (Cash_Discount_Rate ** t_cnt)

        for t_cnt in range(10):
            FCV_10V += free_cashflow * (Profit_Increase_Rate ** t_cnt) * (Cash_Discount_Rate ** t_cnt)

        # for t_cnt in range(20):
        #    FCV_20V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        # for t_cnt in range(50):
        #    FCV_50V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        # for t_cnt in range(100):
        #    FCV_100V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        for t_cnt in range(200):
            FCV_nV += free_cashflow * (Profit_Increase_Rate ** t_cnt) * (Cash_Discount_Rate ** t_cnt)
            if FCV_nV / guben > CurrentPrice:
                df.iloc[i, df.columns.get_loc("FCV_NYears")] = t_cnt

                break

        FCV_5V = FCV_5V / guben
        FCV_10V = FCV_10V / guben
        # FCV_20V = FCV_20V/guben
        # FCV_50V = FCV_50V/guben
        # FCV_100V = FCV_100V/guben

        df.iloc[i, df.columns.get_loc("FCV_5V")] = int(FCV_5V)
        df.iloc[i, df.columns.get_loc("FCV_10V")] = int(FCV_10V)

        total_cur_liab = df.iloc[i]["total_cur_liab"]  # 流动负债合计
        total_ncl = df.iloc[i]["total_ncl"]  # 非流动负债合计

        ValuePrice = (total_cur_assets + total_nca - total_cur_liab - total_ncl) / guben
        ValuePrice = round(ValuePrice, 2)
        df.iloc[i, df.columns.get_loc("ValuePrice")] = ValuePrice

        df.iloc[i, df.columns.get_loc("CurrentPrice")] = CurrentPrice

        if CurrentPrice == 0.0:
            V_C_P = 0
        else:
            V_C_P = round(ValuePrice / CurrentPrice, 2)

        df.iloc[i, df.columns.get_loc("V_C_P")] = V_C_P

    # loop_2 of step6, calculate VCP_P, FCV_P
    df = df.reset_index().drop("index", axis=1)

    df_cmp = df[df["FCV"] > 0]

    for i in range(df.__len__()):
        df.iloc[i, df.columns.get_loc("VCP_P")] = round(stats.percentileofscore(df["V_C_P"], df.iloc[i]["V_C_P"]), 2)

        if df.iloc[i, df.columns.get_loc("FCV")] > 0:
            df.iloc[i, df.columns.get_loc("FCV_P")] = round(stats.percentileofscore(df_cmp["FCV"], df.iloc[i]["FCV"]), 2)
            df.iloc[i, df.columns.get_loc("FCV_NYears_P")] = 100 - round(stats.percentileofscore(df_cmp["FCV_NYears"], df.iloc[i]["FCV_NYears"]), 2)

        scoreB_tmp = 0
        scoreB_tmp += df.iloc[i, df.columns.get_loc("VCP_P")]
        scoreB_tmp += df.iloc[i, df.columns.get_loc("FCV_P")]
        scoreB_tmp += df.iloc[i, df.columns.get_loc("FCV_NYears_P")]
        df.iloc[i, df.columns.get_loc("scoreB_tmp")] = round(scoreB_tmp, 2)

    # loop 3 of step 6,
    for i in range(df.__len__()):
        df.iloc[i, df.columns.get_loc("scoreB")] = round(stats.percentileofscore(df["scoreB_tmp"], df.iloc[i]["scoreB_tmp"]), 2)
        df.iloc[i, df.columns.get_loc("scoreAB")] = round((df.iloc[i, df.columns.get_loc("scoreA")] + df.iloc[i, df.columns.get_loc("scoreB")]) / 2, 2)

    # df = df.sort_values('V_C_P', ascending=False, inplace=False)
    df = df.sort_values("scoreAB", ascending=False, inplace=False)
    # df = df.reset_index().drop('index', axis=1)
    # df = df[['code','name','scoreA','scoreB', 'VCP_P', 'FCV_P', 'FCV_NYears_P', 'FCV', 'FCV_NYears', 'V_C_P','ValuePrice','CurrentPrice', 'FCV_5V','FCV_10V','FCV_20V','FCV_50V','FCV_100V',]]
    df = df[["code", "name", "scoreAB", "scoreA", "scoreB", "VCP_P", "FCV_P", "FCV_NYears_P", "ROE_Mean", "ROE_Cnt", "FCV", "FCV_NYears", "V_C_P", "ValuePrice", "CurrentPrice", "FCV_5V", "FCV_10V"]]

    df_selected = df
    df_selected = df_selected[df_selected["scoreAB"] > 90]
    df_selected = df_selected[df_selected["scoreA"] > 90]
    df_selected = df_selected[df_selected["scoreB"] > 50]
    df_selected = df_selected[df_selected["VCP_P"] > 50]
    df_selected = df_selected[df_selected["FCV_P"] > 50]
    df_selected = df_selected[df_selected["FCV_NYears_P"] > 50]

    df_selected = finlib.Finlib().remove_garbage(df_selected, code_field_name="code", code_format="C2D6")

    df.to_csv(csv_output, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "analysze step 6 result saved to " + csv_output + " , len " + str(df.__len__()))

    df_selected.to_csv(csv_selected_output, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "analysze step 6 selected result saved to " + csv_selected_output + " , len " + str(df_selected.__len__()))


# def verify_fund_increase():
def _analyze_step_7():

    logging.info(__file__ + " " + "=== analyze step 7 : verify_fund_increase ===")  # buy at month after the financial report,hold a quarter.

    output_dir = fund_base_report + "/step7"
    csv_output = output_dir + "/verify_fund_increase.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
    #     logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
    #     return

    csv_input = fund_base_report + "/step4/multiple_years_score.csv"
    df_input = pd.read_csv(csv_input, converters={"end_date": str})
    df_input = finlib.Finlib().ts_code_to_code(df_input)
    df_input = finlib.Finlib().remove_garbage(df_input)

    # debug:
    # df_input=df_input[df_input['ts_code'] == '600519.SH'].reset_index().drop('index', axis=1) #good stock
    # df_input=df_input[df_input['ts_code'] == '603709.SH'].reset_index().drop('index', axis=1) #worst stock
    # df_input=df_input[df_input['score_over_years']>=90].reset_index().drop('index', axis=1)
    # df_input=df_input.head(2)
    # df_input=df_input.tail(2).reset_index().drop('index', axis=1)

    df_result = df_input

    # make the columns of the result df
    col_list = []

    for c in df_input.columns:
        # fund score of the quarter
        if re.match("\d{6}", c):
            # logging.info(c)
            df_result = pd.DataFrame([-100] * df_result.__len__(), columns=["inc" + c]).join(df_result)
            ##col_list.append("score"+c)  #increase since the quarter to today
            col_list.append("inc" + c)  # increase since the quarter to today
            col_list.append(c)
        else:
            col_list.append(c)

    df_result = df_result[col_list]
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_inc_avg"]).join(df_result)
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_win_p"]).join(df_result)
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_cnt_win"]).join(df_result)

    len = df_result.__len__()

    for i in range(len):

        ktr_cnt_win = ktr_cnt_all = ktr_sum = 0.0

        logging.info(__file__ + " " + "==== analyze_step_7 " + str(i) + " of " + str(len) + " ====")
        the_df = df_result.iloc[i]

        code = the_df["code"]
        price_today = finlib.Finlib().get_price(code_m=code)

        col_list = list(df_input.columns)
        col_list.reverse()

        for c in col_list:
            mat = re.match("(\d{4})(\d{2})", c)
            if mat:
                logging.info(__file__ + " " + "code " + code + " period " + c)
                year = mat.group(1)
                month = mat.group(2)
                score = the_df[c]

                if str(year) < "2015":
                    logging.info(__file__ + " " + "Not process year before 2015")
                    continue

                price_the_day = finlib.Finlib().get_price(code_m=code, date=year + "-" + month + "-" + "31")

                if np.isnan(score):
                    continue

                if score > 95:
                    # start of the buy and sell (ktr)
                    # compare price of +1month vs +4month
                    month_ktr_start = int(month) + 1  # ktr:know the report. suppose Q2 06.31 report published at 07.31
                    month_ktr_end = int(month) + 4  # next quarter publish data

                    year_ktr_start = year_ktr_end = int(year)
                    ktr_cnt_all += 1

                    if month_ktr_start > 12:
                        month_ktr_start = month_ktr_start % 12
                        year_ktr_start += 1

                    if month_ktr_end > 12:
                        month_ktr_end = month_ktr_end % 12
                        year_ktr_end += 1

                    if month_ktr_start < 10:
                        month_ktr_start = "0" + str(month_ktr_start)

                    if month_ktr_end < 10:
                        month_ktr_end = "0" + str(month_ktr_end)

                    price_ktr_start = finlib.Finlib().get_price(code_m=code, date=str(year_ktr_start) + "-" + str(month_ktr_start) + "-" + "31")
                    price_ktr_end = finlib.Finlib().get_price(code_m=code, date=str(year_ktr_end) + "-" + str(month_ktr_end) + "-" + "31")

                    if price_ktr_start != 0.0 and price_ktr_end != 0.0:
                        quarter_inc = (price_ktr_end - price_ktr_start) * 100 / price_ktr_start
                        quarter_inc = round(quarter_inc, 2)

                        ktr_sum += quarter_inc

                        if quarter_inc > 0:
                            ktr_cnt_win += 1

                        # logging.info(__file__+" "+"score "+str(score)+", "+str(quarter_inc)+": "+str(year_ktr_start)+str(month_ktr_start)+" -> "+str(year_ktr_end)+str(month_ktr_end))

                    # end of the buy and sell (ktr)

                if price_the_day == 0.0:
                    increase = None
                else:
                    increase = (price_today - price_the_day) * 100 / price_the_day
                    increase = round(increase, 2)

                logging.info(__file__ + " " + str(code) + " " + str(c) + ", score: " + str(score) + " inc: " + str(increase) + " from " + str(price_the_day) + " to " + str(price_today))
                df_result.iloc[i, df_result.columns.get_loc("inc" + c)] = increase
                pass  # end of the for loop of the column

        if ktr_cnt_all > 0.0:
            df_result.iloc[i, df_result.columns.get_loc("ktr_cnt_win")] = int(ktr_cnt_win)
            df_result.iloc[i, df_result.columns.get_loc("ktr_win_p")] = round(ktr_cnt_win * 100 / ktr_cnt_all, 2)
            df_result.iloc[i, df_result.columns.get_loc("ktr_inc_avg")] = round(ktr_sum / ktr_cnt_all)
        pass  # end of the for loop of the rows

    # df_result = df_result.sort_values('score_over_years', ascending=False, inplace=False)
    df_result = df_result.reset_index().drop("index", axis=1)
    df_result.to_csv(csv_output, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "verify_fund_increase result saved to " + csv_output + " , len " + str(df_result.__len__()))


def _analyze_step_8():
    logging.info(__file__ + " " + "=== analyze step 8 ===")

    # csv_input_1 = fund_base_report + "/step6/multiple_years_score_selected.csv" #scoreA>90, V_C_P >0.65
    csv_input_1 = fund_base_report + "/step6/multiple_years_score_selected.csv"
    csv_input_2 = fund_base_report + "/step7/verify_fund_increase.csv"

    output_dir = fund_base_report + "/step8"
    csv_output = output_dir + "/multiple_years_score.csv"
    csv_output_selected = output_dir + "/multiple_years_score_selected.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
    #     logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output)
    #     return
    #
    # if (not force_run_global) and finlib.Finlib().is_cached(csv_output_selected, day=1):
    #     logging.info(__file__ + " " + "file has been updated in 1 days, will not calculate. " + csv_output_selected)
    #     return

    df = pd.DataFrame()

    # stock_code = "SZ000402"

    logging.info(__file__ + " " + "loading " + csv_input_1)
    df_1 = pd.read_csv(csv_input_1)
    # df_1 = finlib.Finlib().ts_code_to_code(df=df_1)
    # logging.info(__file__+" "+"read line "+str(df_1.__len__()))

    logging.info(__file__ + " " + "loading " + csv_input_2)
    df_2 = pd.read_csv(csv_input_2, converters={"code": str})
    # df_2 = finlib.Finlib().add_market_to_code(df=df_2)
    # logging.info(__file__+" "+"read line " + str(df_2.__len__()))

    df = pd.merge(df_1, df_2, how="inner", on=["code"], suffixes=("", "_stock_basics"))
    #   len = df.__len__()
    # logging.info(__file__+" "+"after merge len "+str(len))

    df.to_csv(csv_output, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "analysze step 8 result saved to " + csv_output + " . len " + str(df.__len__()))

    # scoreAB = 80  <<< This has been filted in step6/selected.csv
    # df = df[df['scoreAB']>scoreAB]
    # logging.info(__file__+" "+"scoreAB > "+str(scoreAB) +" " + str(df.__len__()))

    ktr_win_p = 50
    df = df[df["ktr_win_p"] >= ktr_win_p]
    logging.info(__file__ + " " + "ktr_win_p >= " + str(ktr_win_p) + " " + str(df.__len__()))

    # ktr_inc_avg = 3
    # df = df[df['ktr_inc_avg']>ktr_inc_avg]
    # logging.info(__file__+" "+"ktr_inc_avg > "+str(ktr_inc_avg)+" " + str(df.__len__()))

    # ktr_cnt_win = 5
    # df = df[df['ktr_cnt_win']>ktr_cnt_win]
    # logging.info(__file__+" "+"ktr_cnt_win > "+str(ktr_cnt_win)+" " + str(df.__len__()))

    df.to_csv(csv_output_selected, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "analysze step 8 selected result saved to " + csv_output_selected + " , len " + str(df.__len__()))


def analyze(fully_a=False, daily_a=True, fast=True):
    # df = pd.read_csv(csv_income, converters={'end_date': str})

    # ed = list(df['end_date'].unique())

    report_status = finlib.Finlib().get_report_publish_status()
    period_list = report_status["period_to_be_checked_lst"]  # period to be checked at this time point (based on month)

    time_map = finlib.Finlib().get_year_month_quarter()

    if fully_a:
        if fast:
            period_list = [finlib.Finlib().get_report_publish_status()["completed_year_rpt_date"]]  # @2019.03.10, it is 20171231
        else:
            period_list = time_map["full_period_list_yearly"]
            period_list.extend([time_map["ann_date_1q_before"], time_map["ann_date_2q_before"], time_map["ann_date_3q_before"]])
    elif daily_a:
        # pass
        # print("Have not decide which period to check daily")
        period_list = time_map["fetch_most_recent_report_perid"]  # @2019.03.10, it is 20181231
        # period_list = [finlib.Finlib().get_report_publish_status()['completed_quarter_date']] #@2019.03.10, it is 20180930
        # period_list = [finlib.Finlib().get_report_publish_status()['completed_year_rpt_date']] #@2019.03.10, it is 20171231

    period_list = list(set(period_list))
    period_list.sort(reverse=False)  # run new latest, so lastest_symbol_link points to the latest period. 20210117
    # period_list.sort(reverse=True)  #check from new to old. 20181231-->20001231

    # if debug_global:
    #    period_list=["20171231"]
    # _analyze_step_7() #ryan debug
    # _analyze_step_8() #ryan debug
    # exit()

    last_year_1231 = str(datetime.datetime.today().year - 1) + "1231"
    if datetime.datetime.today().month <= 3:
        if last_year_1231 in period_list:
            period_list.remove(last_year_1231)

    for e in period_list:
        # logging.info(__file__ + " " + "e is " + str(e))
        if e <= "20201231" and daily_a:
            logging.info(__file__ + " " + "not process date before 20201231 in daily analysis. " + str(e))
            continue

        if e <= "20191231":
            logging.info(__file__ + " " + "not process date before 20191231. " + str(e))
            continue

        # logging.info(__file__+" "+"end_date " + e + ". ")

        # continue

        # beneish
        # beneish_csv = '/home/ryan/DATA/result/ag_beneish.csv'
        # ts_code,name,ann_date,M_8v,M_5v,DSRI,GMI,AQI,SGI,DEPI,SGAI,TATA,LVGI
        # beneish_df = pd.read_csv(beneish_csv, converters={'ann_date': str})
        # beneish_df = finlib.Finlib().

        # as many date lost on Q1, Q3 report, so only process half-year, and year report.
        # !!!! @todo ryan: parallary compare on yearly report; Q1, Q2, Q3 data can be used in self comparision.
        # !!!!
        if re.match("\d{4}1231$", e) or daily_a or fully_a or force_run_global:  # daily_a check the most recent only(small scope), so daily_a check all steps.
            # _analyze_step_1(end_date=e, beneish_df=beneish_df)  # field calculate
            _analyze_step_1(end_date=e)  # field calculate
            _analyze_step_2(end_date=e)  # score
            _analyze_step_3(end_date=e)  # score of score
            _analyze_step_4()  # evaluate the stock score in mutliple years.
            _analyze_step_5()  # 'scoreA'

            _analyze_step_6()  # under valued stock, valuePrice/actualPrice. scoreA,V_C_P, #time consuming.
            _analyze_step_7()  # time consuming
            _analyze_step_8()
        else:
            logging.info(__file__ + " " + "not handle Q1, Q2, Q3 report, " + e)
            continue

    # if fully_a or force_run_global:
    #     _analyze_step_6()  #under valued stock, valuePrice/actualPrice. scoreA,V_C_P, #time consuming.
    #     _analyze_step_7()  #time consuming
    #     _analyze_step_8()


def extract_white_horse():

    output_csv = fund_base_report + "/white_horse.csv"
    year_q = finlib.Finlib().get_year_month_quarter()

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list.rename(columns={"code": "ts_code"}, inplace=True)

    df_stock_list = stock_list

    for time_period in ["5y", "4y", "3y", "2y", "1y", "3q", "2q", "1q"]:
        df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=[time_period]))

    df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=["cnt_sum_white_horse"]))

    for time_period in ["5y", "4y", "3y", "2y", "1y", "3q", "2q", "1q"]:
        ann_date = "ann_date_" + time_period + "_before"
        input_csv = fund_base_report + "/step1/rpt_" + year_q[ann_date] + ".csv"
        df_q = pd.DataFrame()

        if not os.path.exists(input_csv):
            continue
        else:
            logging.info(__file__ + " " + "reading " + input_csv)
            df_q = pd.read_csv(input_csv, converters={"end_date": str})
            df_q = df_q.fillna("Nan")

            # df_q = df_q.fillna(0)
            df_q = df_q[df_q["bonusReason"].str.contains("white horse", na=False)]  # filter out the white horse
            logging.info(__file__ + " " + "found number of whitehorse " + str(df_q.__len__()))

            for c in df_q["ts_code"].values:
                for v in df_stock_list[df_stock_list["ts_code"] == c].index.values:
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc(time_period)] += 1
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc("cnt_sum_white_horse")] += 1

    df_stock_list = df_stock_list[df_stock_list["cnt_sum_white_horse"] > 0]
    df_stock_list = df_stock_list.sort_values("cnt_sum_white_horse", ascending=False, inplace=False)
    df_stock_list = finlib.Finlib().ts_code_to_code(df_stock_list)

    df_stock_list = df_stock_list.reset_index().drop("index", axis=1)
    df_stock_list.to_csv(output_csv, encoding="UTF-8", index=False)

    logging.info(__file__ + ": " + "white horse csv saved to " + output_csv + " . len " + str(df_stock_list.__len__()))


def extract_high_freecashflow_price_ratio():
    csv_input = fund_base_report + "/step6/multiple_years_score_selected.csv"
    output_csv = fund_base_report + "/freecashflow_price_ratio.csv"

    if not os.path.exists(csv_input):
        logging.info(__file__ + " " + "input csv doesn't exist, " + csv_input)
        return

    if not os.stat(csv_input).st_size >= 10:
        logging.info(__file__ + " " + "input csv is empty. " + csv_input)
        return

    df = pd.read_csv(csv_input)  # no end_date column in the csv
    df.sort_values("FCV_NYears", ascending=True, inplace=False)
    df = df.reset_index().drop("index", axis=1)
    df.to_csv(output_csv, encoding="UTF-8", index=False)

    logging.info(__file__ + ": " + "freecashflow_price_rati csv saved to " + output_csv + " . len " + str(df.__len__()))


def extract_hen_cow():
    output_csv = fund_base_report + "/hen_cow.csv"
    year_q = finlib.Finlib().get_year_month_quarter()

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list.rename(columns={"code": "ts_code"}, inplace=True)

    df_stock_list = stock_list

    for time_period in ["5y", "4y", "3y", "2y", "1y", "3q", "2q", "1q"]:
        df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=[time_period]))

    df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=["cnt_sum_hen_cow"]))

    for time_period in ["5y", "4y", "3y", "2y", "1y", "3q", "2q", "1q"]:

        weight_map = {"5y": 1 / 8.0, "4y": 2 / 8.0, "3y": 3 / 8.0, "2y": 4 / 8.0, "1y": 5 / 8.0, "3q": 6 / 8.0, "2q": 7 / 8.0, "1q": 8 / 8.0}
        ann_date = "ann_date_" + time_period + "_before"
        input_csv = fund_base_report + "/step2/rpt_" + year_q[ann_date] + ".csv"
        df_q = pd.DataFrame()

        if not os.path.exists(input_csv):
            continue
        else:
            logging.info(__file__ + " " + "reading " + input_csv)
            df_q = pd.read_csv(input_csv, converters={"end_date": str})
            df_q_hen = df_q[df_q["scoreHen"] > 80]  # filter out the white horse
            df_q_cow = df_q[df_q["scoreCow"] > 80]  # filter out the white horse
            df_q_hen_cow = df_q_cow.append(df_q_hen)
            # logging.info(__file__+" "+"df_q_hen len " + str(df_q_hen.__len__()))
            # logging.info(__file__+" "+"df_q_cow len " + str(df_q_cow.__len__()))
            # logging.info(__file__+" "+"hen_cow len "+str(df_q_hen_cow.__len__()))

            for c in df_q_hen_cow["ts_code"].values:
                for v in df_stock_list[df_stock_list["ts_code"] == c].index.values:
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc(time_period)] += 1
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc("cnt_sum_hen_cow")] += 1 * weight_map[time_period]

    df_stock_list = df_stock_list[df_stock_list["cnt_sum_hen_cow"] > 0]
    df_stock_list = df_stock_list.sort_values("cnt_sum_hen_cow", ascending=False, inplace=False)
    df_stock_list = df_stock_list.head(100)
    df_stock_list = df_stock_list.reset_index().drop("index", axis=1)
    df_stock_list = finlib.Finlib().ts_code_to_code(df_stock_list)

    df_stock_list.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + ": " + "hen cow csv saved to " + output_csv + " . len " + str(df_stock_list.__len__()))


# print recent express data
def express_notify():
    pass


# print recent express data
def disclosure_date_notify(days):

    input_csv = csv_disclosure_date_latest
    output_csv = csv_disclosure_date_latest_notify

    if not os.path.exists(input_csv):
        logging.info(__file__ + " " + "file not exist, quit. " + input_csv)

    # df = pd.read_csv(input_csv, converters={'code':str,'name':str,'ann_date':str,'end_date':str, 'pre_date':str,'actual_date':str,'modify_date':str}, encoding="utf-8" )
    df = pd.read_csv(input_csv, converters={"code": str, "ann_date": str, "end_date": str, "pre_date": str, "actual_date": str, "modify_date": str}, encoding="utf-8")

    import datetime

    todayS = datetime.datetime.today().strftime("%Y%m%d")

    endday = datetime.datetime.today() + datetime.timedelta(days)
    enddayS = endday.strftime("%Y%m%d")

    df_result = df
    df_result = df_result[df_result["pre_date"] >= todayS]
    df_result = df_result[df_result["pre_date"] <= enddayS]

    df_result = df_result.sort_values("pre_date", ascending=True, inplace=False)
    df_result = df_result.reset_index().drop("index", axis=1)

    df_result = finlib.Finlib().ts_code_to_code(df=df_result)
    df_result.to_csv(output_csv, encoding="UTF-8", index=False)

    logging.info(__file__ + ":  in " + str(days) + " days disclosure notify csv saved to " + output_csv + " . len " + str(df_result.__len__()))

    pass


def _fetch_pro_basic():
    ts.set_token(myToken)
    pro = ts.pro_api()

    if not os.path.isdir(fund_base_source):
        os.mkdir(fund_base_source)

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_basic.csv"

    if finlib.Finlib().is_cached(output_csv, 1):
        logging.info(__file__ + " " + "not fetch basic as the file updated in 1 day. " + output_csv)
        return ()

    df = pro.query("stock_basic", exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,list_date")
    df.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "pro basic saved to " + output_csv + " . len " + str(df.__len__()))
    return df


def _fetch_stock_company():
    ts.set_token(myToken)
    pro = ts.pro_api()

    if not os.path.isdir(fund_base_source):
        os.mkdir(fund_base_source)

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_stock_company.csv"

    if finlib.Finlib().is_cached(output_csv, 15):
        logging.info(__file__ + " " + "not fetch stock company as the file updated in 15 day. " + output_csv)
        return ()

    all_fields='ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope'.split(',')
    df_1 = pro.stock_company()
    fields_have_fetched = df_1.columns.to_list()

    df_2 = pro.stock_company(fields=','.join(set(all_fields) - set(fields_have_fetched)))

    df = pd.merge(df_1, df_2, left_index=True, right_index=True)
    df = finlib.Finlib().ts_code_to_code(df)
    df = finlib.Finlib().add_stock_name_to_df(df)
    df = finlib.Finlib().adjust_column(df,col_name_list=['code','name','exchange','employees','chairman',
                                                         'manager','secretary','reg_capital',
                                                         'setup_date','province','city','office',
                                                         'introduction', 'business_scope', 'main_business'
                                                         ])
    df = df.sort_values(by='employees', ascending=False)
    df.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "stock company saved to " + output_csv + " . len " + str(df.__len__()))
    return df


def _fetch_pro_pledge_stat_detail():
    ts.set_token(myToken)
    pro = ts.pro_api()

    if not os.path.isdir(fund_base_source):
        os.mkdir(fund_base_source)

    dir = fund_base_source + "/pledge"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv_stat = dir + "/pledge_stat.csv"
    output_csv_detail = dir + "/pledge_detail.csv"

    if finlib.Finlib().is_cached(output_csv_stat, 5) and (not force_run_global):
        logging.info(__file__ + " " + "not fetch pledge_stat as the file updated in 1 day. " + output_csv_stat)
        return ()

    if finlib.Finlib().is_cached(output_csv_detail, 5) and (not force_run_global):
        logging.info(__file__ + " " + "not fetch pledge_stat as the file updated in 1 day. " + output_csv_detail)
        return ()

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list = finlib.Finlib().add_ts_code_to_column(stock_list)

    stock_cnt = 0
    df_result_stat= pd.DataFrame()
    df_result_detail= pd.DataFrame()
    for ts_code in stock_list["ts_code"]:
        stock_cnt += 1
        logging.info(str(stock_cnt) + " of " + str(stock_list.__len__())+", fetching pledge_stat, "+ts_code)

        try:
            df_stat = pro.query("pledge_stat",ts_code=ts_code)
            df_detail = pro.query("pledge_detail",ts_code=ts_code)
            time.sleep(60/50) #抱歉，您每分钟最多访问该接口50次

        except:
            logging.info(__file__ + " " + "exception in _fetch_pro_pledge_stat")
        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(str(traceback.print_exception(*sys.exc_info())).encode("utf8"))
                logging.info(sys.exc_value.message)  # print the human readable unincode
                logging.info(__file__ + " " + "ts_code: " + ts_code)
                sys.exc_clear()

        if df_stat.__len__()>0:
            df_stat = df_stat.head(1)
            df_result_stat = pd.concat([df_result_stat, df_stat], sort=False).reset_index().drop("index", axis=1)
            df_result_stat.to_csv(output_csv_stat, encoding="UTF-8", index=False)

        if df_detail.__len__()>0:
            df_detail_not_relased = df_detail[df_detail['is_release']=='0']
            # print(finlib.Finlib().pprint(df_detail_not_relased))
            p_total_ratio_sum = round(df_detail_not_relased['p_total_ratio'].sum(),2)

            df_detail_tmp = pd.DataFrame.from_dict({'ts_code':[ts_code],'p_total_ratio_sum':[p_total_ratio_sum] })
            df_result_detail = pd.concat([df_result_detail, df_detail_tmp], sort=False).reset_index().drop("index", axis=1)
            df_result_detail.to_csv(output_csv_detail, encoding="UTF-8", index=False)

    df_result_stat = df_result_stat.sort_values('pledge_ratio', ascending=False)
    df_result_stat = finlib.Finlib().ts_code_to_code(df=df_result_stat)
    df_result_stat = finlib.Finlib().add_stock_name_to_df(df=df_result_stat)
    df_result_stat.to_csv(output_csv_stat, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "pledge_stat saved to " + output_csv_stat + " . len " + str(df_result_stat.__len__()))


    df_result_detail = df_result_detail.sort_values('p_total_ratio_sum', ascending=False)
    df_result_detail = finlib.Finlib().ts_code_to_code(df=df_result_detail)
    df_result_detail = finlib.Finlib().add_stock_name_to_df(df=df_result_detail)
    df_result_detail.to_csv(output_csv_detail, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "pledge_stat details saved to " + output_csv_detail + " . len " + str(df_result_detail.__len__()))



    return(df_result_stat)


def get_pro_basic():
    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_basic.csv"
    return pd.read_csv(output_csv)


def _fetch_pro_concept():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_concept.csv"

    if finlib.Finlib().is_cached(output_csv, 1) and (not force_run_global):
        logging.info(__file__ + " " + "not fetch concept as the file updated in 1 day. " + output_csv)
        return ()

    # df_result = pd.DataFrame(columns=['cat_name', 'cat_code'])
    df_result = pd.DataFrame()

    df_c = pro.concept()
    cnt = df_c.__len__()
    i = 0
    for id in df_c["code"]:
        i += 1

        # df_sub = pd.DataFrame()
        cat_name = df_c[df_c["code"] == id]["name"].iloc[0]
        logging.info(__file__ + " " + "query concept details, " + str(i) + " of " + str(cnt) + ", id " + str(id) + " name " + cat_name)

        try:
            df_cd = pro.concept_detail(id=id, fields="ts_code,name")
            time.sleep(0.5)  # 抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
            new_value_df = pd.DataFrame([id] * df_cd.__len__(), columns=["cat_code"])
            df_cd = df_cd.join(new_value_df)

            new_value_df = pd.DataFrame([cat_name] * df_cd.__len__(), columns=["cat_name"])
            df_cd = df_cd.join(new_value_df)

            df_result = pd.concat([df_result, df_cd], sort=False).reset_index().drop("index", axis=1)

        except:
            logging.info(__file__ + " " + "exception in get_pro_concept")
        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(str(traceback.print_exception(*sys.exc_info())).encode("utf8"))
                # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                logging.info(sys.exc_value.message)  # print the human readable unincode
                logging.info(__file__ + " " + "cat_id: " + id + " cat_name: " + cat_name)
                sys.exc_clear()

    df_result.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "pro concept saved to " + output_csv + " . len " + str(df_result.__len__()))
    return df_result


def _fetch_cctv_news():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/cctv_news"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/cctv_news.csv"

    if finlib.Finlib().is_cached(output_csv, 1) and (not force_run_global):
        logging.info(__file__ + " " + "not fetch cctv news as the file updated in 1 day")
        return ()

    df_result = pd.DataFrame(columns=["date", "title", "content"])

    if os.path.exists(output_csv):
        logging.info(__file__ + " " + "loading " + output_csv)
        df_result = pd.read_csv(output_csv, converters={"date": str})

    # df_result = pd.DataFrame(columns=['cat_name', 'cat_code'])

    # 数据开始于2006年6月，超过12年历史
    date = datetime.date(2006, 6, 15)
    today = datetime.date.today()

    while date <= today:
        date_S = date.strftime("%Y%m%d")

        if df_result[df_result["date"] == date_S].__len__() > 0:
            logging.info(__file__ + " " + "." + date_S)  # already have the records
            date = date + datetime.timedelta(1)
            continue

        logging.info(__file__ + " " + "getting cctv news " + date_S)

        try:
            df_cctv_news = pro.cctv_news(date=date_S)
            df_result = df_result.append(df_cctv_news)
            logging.info(__file__ + " " + "len " + str(df_result.__len__()))
            #            df_result.to_csv(output_csv, encoding='UTF-8', index=False)

            time.sleep(1)
        except:
            logging.info(__file__ + " " + "exception in fetching cctv news")
        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(str(traceback.print_exception(*sys.exc_info())).encode("utf8"))
                # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                logging.info(sys.exc_value.message)  # print the human readable unincode
                logging.info(__file__ + " " + "exception in fetching cctv news")
                sys.exc_clear()

        date = date + datetime.timedelta(1)

    df_result.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "cctv news saved to " + output_csv + " . len " + str(df_result.__len__()))
    return df_result


def _fetch_stk_holdertrade(fast_fetch=False):
    fields = ["ts_code", "ann_date", "holder_name", "holder_type", "in_de", "change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share", "begin_date", "close_date"]

    today_S = datetime.date.today().strftime("%Y%m%d")

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)
    if debug_global:
        stock_list = stock_list[stock_list["code"] == "600519.SH"]

    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/holdertrade"  # 股东增减持

    today_holder_trade_csv = "/home/ryan/DATA/result/today_holder_trade.csv"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    if (finlib.Finlib().is_cached(today_holder_trade_csv, 1)) and (not force_run_global):
        logging.info(__file__ + " " + "file has been updated in 1 day, not fetch again " + today_holder_trade_csv)
    else:
        df_today = pro.stk_holdertrade(ann_date=today_S, fields=fields)
        df_today.to_csv(today_holder_trade_csv, encoding="UTF-8", index=False)
        logging.info(__file__ + " " + "Saved today stock holder trade to " + today_holder_trade_csv + " . len " + str(df_today.__len__()))

        if debug_global:
            df_today = df_today[df_today["ts_code"] == "600519.SH"]

        # update each csv
        for ts_code in df_today["ts_code"]:
            logging.info(ts_code)
            output_csv = dir + "/" + ts_code + ".csv"

            df_new = df_today[df_today["ts_code"] == ts_code]

            if os.path.exists(output_csv):
                logging.info(__file__ + " " + "loading " + output_csv)
                df_base = pd.read_csv(output_csv, converters={"ann_date": str, "begin_date": str, "close_date": str})
                df_base = df_base.append(df_new)
            else:
                df_base = df_new

            df_base.to_csv(output_csv, encoding="UTF-8", index=False)
            logging.info(ts_code + " , append today stock holder trade to " + output_csv + " . len " + str(df_today.__len__()))

    # fetch all base
    if force_run_global:
        cnt = 0
        cnt_all = str(stock_list["code"].__len__())

        for ts_code in stock_list["code"]:
            cnt += 1
            output_csv = dir + "/" + ts_code + ".csv"

            if finlib.Finlib().is_cached(output_csv, 5):
                logging.info(__file__ + " " + "not fetch holder trade as the file updated in 5 day. " + output_csv)
                continue

            try:
                df = pro.stk_holdertrade(ts_code=ts_code, fields=fields)
                df.to_csv(output_csv, encoding="UTF-8", index=False)
                logging.info(str(cnt) + " of " + cnt_all + " , saved stock holder trade to " + output_csv + " . len " + str(df.__len__()))
                time.sleep(0.8)
            except:
                logging.info(__file__ + " " + "exception, sleeping 30sec then renew the ts_pro connection")

            finally:

                if sys.exc_info() == (None, None, None):
                    pass  # no exception
                else:
                    # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8')) #python2
                    logging.info(str(traceback.print_exception(*sys.exc_info())).encode("utf8"))  # python3
                    logging.info(sys.exc_value.message)  # print the human readable unincode
                    logging.info(__file__ + " " + "query: stk_holdertrade, ts_code: " + ts_code)
                    sys.exc_clear()

    return ()


def get_pro_concept():

    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_concept.csv"

    return pd.read_csv(output_csv)


def _fetch_pro_repurchase():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_repurchase.csv"

    if finlib.Finlib().is_cached(output_csv, 1):
        logging.info(__file__ + " " + "not fetch repurchase as the file updated in 1 day")
        return ()

    # df = pro.repurchase(ann_date='', start_date='20190101', end_date='20180510')
    df = pro.repurchase()
    time.sleep(0.5)  # 抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
    df.to_csv(output_csv, encoding="UTF-8", index=False)
    logging.info(__file__ + " " + "pro repurchase saved to " + output_csv + " . len " + str(df.__len__()))
    return df


def get_pro_repurchase():
    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_repurchase.csv"
    return pd.read_csv(output_csv)


def concept_top():
    fund_csv = fund_base + "/merged/merged_all_20181231.csv"
    df_fund = pd.read_csv(fund_csv)

    input_csv = fund_base_source + "/market" + "/pro_concept.csv"
    df = pd.read_csv(input_csv)

    output_csv = "/home/ryan/DATA/result/concept_top.csv"
    df_out = pd.DataFrame()

    for cat_code in df["cat_code"].unique():
        # print(cat_code)
        df_sub = df[df["cat_code"] == cat_code].reset_index().drop("index", axis=1)
        cat_name = df_sub.iloc[0]["cat_name"]

        df_fund_sub = df_fund[df_fund["ts_code"].isin(df_sub["ts_code"].to_list())].reset_index().drop("index", axis=1)
        roe_mean = df_fund_sub["roe"].mean()
        df_roe = df_fund_sub[df_fund_sub["roe"].rank(pct=True) > 0.85][["name", "ts_code"]]  # 净资产收益率
        df_profit_dedt = df_fund_sub[df_fund_sub["profit_dedt"].rank(pct=True) > 0.85][["name", "ts_code"]]  # 扣除非经常性损益后的净利润
        df_netprofit_margin = df_fund_sub[df_fund_sub["netprofit_margin"].rank(pct=True) > 0.85][["name", "ts_code"]]  # 销售净利率
        df_npta = df_fund_sub[df_fund_sub["npta"].rank(pct=True) > 0.85][["name", "ts_code"]]  # 总资产净利润
        df_netdebt = df_fund_sub[df_fund_sub["netdebt"].rank(pct=True) < 0.15][["name", "ts_code"]]  # 净债务
        df_debt_to_assets = df_fund_sub[df_fund_sub["debt_to_assets"].rank(pct=True) < 0.15][["name", "ts_code"]]  # 资产负债率
        df_q_roe = df_fund_sub[df_fund_sub["q_roe"].rank(pct=True) > 0.85][["name", "ts_code"]]  # 净资产收益率(单季度)

        merged_inner = pd.merge(left=df_roe, right=df_profit_dedt, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_netprofit_margin, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_npta, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_netdebt, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_debt_to_assets, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_q_roe, how="inner", left_on="ts_code", right_on="ts_code", suffixes=("", "_x")).drop("name_x", axis=1)

        merged_inner.insert(2, "cat_code", cat_code)
        merged_inner.insert(3, "cat_name", cat_name)

        logging.info(str(cat_code) + " " + cat_name + ", " + str(merged_inner.__len__()) + " qualified stocks")

        if merged_inner.__len__() > 0:
            logging.info(merged_inner)
            pass

        df_out = pd.concat([df_out, merged_inner], sort=False).reset_index().drop("index", axis=1)

    df_out = finlib.Finlib().ts_code_to_code(df=df_out)
    df_out.to_csv(output_csv, encoding="UTF-8", index=False)

    logging.info("concept_top saved to " + output_csv + " ,len " + str(df_out.__len__()))
    pass


# This is from mainbz view. based on profit of the mainbz
def industry_top_mainbz_profit():
    to_csv = "/home/ryan/DATA/result/industry_top_mainbz_profit.csv"
    to_csv_100 = "/home/ryan/DATA/result/industry_top_mainbz_profit_100.csv"

    if finlib.Finlib().is_cached(file_path=to_csv, day=3) and (not force_run_global):
        logging.info("file generated in "+str(3)+" days, loading and return. "+ to_csv)
        return(pd.read_csv(to_csv),pd.read_csv(to_csv_100))

    f= '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_p.csv'
    df0 = pd.read_csv(f,converters={'end_date':str,})
    df0 = df0[~df0['bz_item'].str.contains("\\(行业\\)")]

    df = df0[df0['end_date']=="20201231"]
    # df = df0[df0['end_date']=="20210630"]
    #600519 20210630 only have bz_sales, bz_profit is not disclosed in the report
    #273690  贵州茅台  600519.SH  20210630     系列酒  6060452300.00            NaN           NaN       CNY            0
    #273691  贵州茅台  600519.SH  20210630     茅台酒 42949397800.00            NaN           NaN       CNY            0
    #
    df = df[~df['bz_profit'].isna()].reset_index().drop('index',axis=1)

    # bz_profit = bz_sales - bz_cost
    #which business most profitable
    df_profit_sum  = df.groupby(by='bz_item').sum().sort_values(by="bz_profit", ascending=False).reset_index()
    df_profit_mean = df.groupby(by='bz_item').mean().sort_values(by="bz_profit", ascending=False).reset_index()

    logging.info("top business by profit sum:\n"+finlib.Finlib().pprint(df_profit_sum.head(20)))
    logging.info("top business by profit mean:\n"+finlib.Finlib().pprint(df_profit_mean.head(20)))

    df['profit_ratio'] = (df['bz_profit'] / df['bz_sales']).round(2)
    df = df[df['profit_ratio'] > 0.12]

    df_rtn = pd.DataFrame()
    bz = df_profit_mean['bz_item'].unique()
    i=0
    for b in bz:
        i+=1
        df_sub = df[df['bz_item']==b].sort_values(by='bz_profit',ascending=False)
        logging.info("bz " + str(i) + " of " + str(bz.__len__()) + " " + b+ " companies has that business "+str(df_sub.__len__()))
        # logging.info(finlib.Finlib().pprint(df_sub.head(2)))
        if df_sub.__len__() < 3 and (df_sub['bz_profit'].max() < df_profit_mean['bz_profit'].mean()+2*df_profit_mean['bz_profit'].std()):
            logging.info("ignore bz "+b+" , less than 3 companies and profit < median+2std")
            continue

        df_rtn = df_rtn.append(df_sub[0:1])

        if df_sub.__len__() >=2 and df_sub.iloc[1]['bz_profit']/df_sub.iloc[0]['bz_profit'] > 0.8:
            df_rtn = df_rtn.append(df_sub[1:2])

        if df_sub.__len__() >=3 and df_sub.iloc[2]['bz_profit']/df_sub.iloc[0]['bz_profit'] > 0.8:
            df_rtn = df_rtn.append(df_sub[2:3])


        # df_rtn = df_rtn.append(df_sub.head(2))

    df_rtn = finlib.Finlib().ts_code_to_code(df_rtn)
    df_rtn = finlib.Finlib().add_industry_to_df(df=df_rtn)

    df_rtn = df_rtn.sort_values(by='bz_profit', ascending=False).reset_index().drop('index', axis=1)
    df_rtn = finlib.Finlib().df_format_column(df=df_rtn, precision='%.1e')
    df_rtn = finlib.Finlib().adjust_column(df=df_rtn, col_name_list=['code','name','end_date','profit_ratio','bz_item','industry_name_L1_L2_L3'])


    #add increase
    df_inc = finlib.Finlib().get_stock_increase(short=True)
    df_rtn = pd.merge(left=df_rtn, right=df_inc, how="inner", on='code')

    df_rtn.to_csv(to_csv, encoding='UTF-8', index=False)
    logging.info("mainbz profit longtou saved to "+to_csv+" , len "+str(df_rtn.__len__()))

    df_rtn_100 = df_rtn[df_rtn['profit_ratio']>0.15][['code','name']].drop_duplicates().head(100).reset_index().drop('index',axis=1)

    df_rtn_100.to_csv(to_csv_100, encoding='UTF-8', index=False)
    logging.info("mainbz profit longtou 100 saved to "+to_csv_100+" , len "+str(df_rtn_100.__len__()))
    return(df_rtn,df_rtn_100)

def _industry_top_mv_eps(df,industry,top=2):
    if df.__len__() <= 3 or df.__len__() < top*2.5:
        logging.info("insufficent stocks in industry "+str(industry)+" , only has "+str(df.__len__())+" stocks.")
        logging.info(finlib.Finlib().pprint(df))
        return(pd.DataFrame())

    df['circ_mv_perc'] = df['circ_mv'].apply(lambda _d: stats.percentileofscore(df['circ_mv'], _d))


    #formular for industry leader.
    df['score'] = df['circ_mv_perc']*0.8+df['eps_incr_-1_perc']*0.1+df['eps_incr_perc']*0.1

    df = df[['code','name','score', 'circ_mv', 'eps_incr', 'eps_incr_-1','industry_name_L1_L2_L3']].sort_values(by='score',ascending=False)

    df = df.reset_index().drop('index', axis=1)
    logging.info("top "+str(top)+" of industry " + str(industry))
    # logging.info(finlib.Finlib().pprint(df.head(top)))
    return(df.head(top))

# This is based on cir_mkt_value + eps_incr + industry_wg view.
def industry_top_mv_eps():
    to_csv = "/home/ryan/DATA/result/industry_top_mv_eps.csv"
    to_csv_100 = "/home/ryan/DATA/result/industry_top_mv_eps_100.csv"

    if finlib.Finlib().is_cached(file_path=to_csv, day=3) and (not force_run_global):
        logging.info("file generated in " + str(3) + " days, loading and return. "+to_csv)
        return (pd.read_csv(to_csv), pd.read_csv(to_csv_100))

    # a = finlib.Finlib().get_report_publish_status()

    # python t_daily_fundamentals_2.py --fetch_pro_fund  or python t_daily_fundamentals_2.py --fetch_data_all
    # python t_daily_fundamentals_2.py  --sum_mainbz
    # python t_daily_fundamentals_2.py  --percent_mainbz_f
    f = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv'
    df = pd.read_csv(f)
    df = df[df['perc_sales']>=60]  ##主营业务收入占比不低于60%
    df = df[['ts_code', 'name', 'end_date', 'bz_cnt','perc_sales' ]]
    df = df.sort_values(by='perc_sales', ascending=False).reset_index().drop('index', axis=1)
    df = finlib.Finlib().ts_code_to_code(df=df)
    # df = finlib.Finlib().add_industry_to_df(df=df,source='all')
    # df = finlib.Finlib().add_industry_to_df(df=df,source='ts')
    df = finlib.Finlib().add_industry_to_df(df=df,source='wg')
    df = finlib.Finlib().add_amount_mktcap(df=df)

    f = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_indicator.csv'
    df_last = pd.read_csv(f)
    df_last = finlib.Finlib().ts_code_to_code(df=df_last)
    df_last = df_last[['code', 'eps']]
    df = pd.merge(left=df, right=df_last, on='code', how="inner")
    logging.info("appended latest eps to df")

    periods = finlib.Finlib().get_last_4q_n_years(n_year=3)

    delta_year = 0
    for p in periods:
        if re.search(r'\d\d1231',p) != None: #yearly report
            delta_year -= 1

            f = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged'+"/"+"merged_all_"+p+".csv"
            df_p = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)
            df_p['bargaining_power'] = (       df_p['adv_receipts']*1.0  #预收款项
                                               + df_p['notes_payable']*0  #应付票据
                                               +df_p['acct_payable']*0 #应付账款
                                       ) \
                                       - (df_p['notes_receiv']*0 #应收票据. 应收票据是其他企业因为欠债而签发的不能立即兑付票据，票据包括支票、银行本票和商业汇票。
                                          +df_p['accounts_receiv']*0 #应收账款. 应收账款是企业因为销售产品而应当在一年内向客户收取的销货款，也就是其他企业欠的货款。 企业因为采用赊销的办法促销商品，出售后不立即收取货款就形成了应收账款
                                          +df_p['oth_receiv']*0) #其他应收款
            # df_p['bargaining_power']=df_p['bargaining_power']*100/df_p['revenue']


            df_p = df_p[['code','eps','bargaining_power']]
            # df_p = df_p[['code','eps','notes_payable','acct_payable','adv_receipts','notes_receiv','accounts_receiv','oth_receiv']]
            df_p = df_p.rename(columns={
                "eps": "eps_"+str(delta_year),
                "bargaining_power": "bargaining_power_"+str(delta_year),

                # "notes_payable": "notes_payable_"+str(delta_year), #应付票据 weight 0.2
                # "acct_payable": "acct_payable_"+str(delta_year), #应付账款 weight 0.2
                # "adv_receipts": "adv_receipts_"+str(delta_year), #预收款项 weight 0.6

                # "notes_receiv": "notes_receiv_"+str(delta_year), #应收票据
                # "accounts_receiv": "accounts_receiv_"+str(delta_year), #应收账款
                # "oth_receiv": "oth_receiv"+str(delta_year), #其他应收款

                                        }, inplace=False)

            df = pd.merge(left=df, right=df_p, on='code', how="inner")
            logging.info("appended period "+p+" eps to df")

    #2017年至2019年、2020年中报每股收益均大于0进行初步筛选
    df = df[(df['eps']>0) & (df['eps_-1']>0) & (df['eps_-2']>0) & (df['eps_-3']>0)]

    # 净利润增幅（用eps代替）
    # the current but uncompleted eps increase at now. Based on the latest report and previous yearly report
    df['eps_incr'] = (df['eps'] - df['eps_-1'])/ df['eps']
    df['eps_incr_perc'] = df['eps_incr'].apply(lambda _d: stats.percentileofscore(df['eps_incr'], _d))
    logging.info(""+finlib.Finlib().pprint(df.sort_values(by='eps_incr_perc',ascending=False).head(30)))

    # df['bp_incr'] = (df['bargaining_power_-1'] - df['bargaining_power_-2'])/ df['bargaining_power_-2']


    # the completed eps increase. based on year-1 and year-2 report
    df['eps_incr_-1'] = (df['eps_-1'] - df['eps_-2'])/ df['eps_-2']
    df['eps_incr_-1_perc'] = df['eps_incr_-1'].apply(lambda _d: stats.percentileofscore(df['eps_incr_-1'], _d))
    logging.info("" + finlib.Finlib().pprint(df.sort_values(by='eps_incr_-1_perc', ascending=False).head(30)))

    df_rtn = pd.DataFrame()
    industries = df['industry_name_L1_L2_L3'].unique()
    for i in industries:
        df_sub = df[df['industry_name_L1_L2_L3']==i]
        df_industry_top = _industry_top_mv_eps(df=df_sub,industry=i,top=3)
        df_rtn = df_rtn.append(df_industry_top)

    df_rtn = finlib.Finlib().df_format_column(df=df_rtn, precision="%.1e")



    #add increase
    df_inc = finlib.Finlib().get_stock_increase(short=True)
    df_rtn = pd.merge(left=df_rtn, right=df_inc, how="inner", on='code')

    df_rtn.to_csv(to_csv, encoding='UTF-8', index=False)
    logging.info("industry_top_mv_eps saved to "+to_csv+" , len "+str(df_rtn.__len__()))
    
    df_rtn_100 =df_rtn[['code','name']].drop_duplicates().head(100).reset_index().drop("index", axis=1)
    df_rtn_100.to_csv(to_csv_100, encoding='UTF-8', index=False)
    logging.info("industry_top_mv_eps_100 saved to "+to_csv_100+" , len "+str(df_rtn_100.__len__()))

    return(df_rtn,df_rtn_100)


def main():
    ########################
    #
    #########################

    logging.info(__file__ + " " + "\n")
    logging.info(__file__ + " " + "SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-f", "--fetch_data_all", action="store_true", dest="fetch_all_f", default=False, help="fetch all the quarterly fundatation history data")

    parser.add_option("--fetch_pro_basic", action="store_true", dest="fetch_pro_basic_f", default=False, help="")

    parser.add_option("--fetch_stk_holdertrade", action="store_true", dest="fetch_stk_holdertrade_f", default=False, help="")
    parser.add_option("--fetch_pro_fund", action="store_true", dest="fetch_pro_fund_f", default=False, help="fetch pro income,balance,cashflow,mainbz,dividend,indicator,audit,forecast,express,disclosure ")
    parser.add_option("--fetch_basic_quarterly", action="store_true", dest="fetch_basic_quarterly_f", default=False, help="")
    parser.add_option("--fetch_basic_daily", action="store_true", dest="fetch_basic_daily_f", default=False, help="")
    parser.add_option("--fetch_info_daily", action="store_true", dest="fetch_info_daily_f", default=False, help="")
    parser.add_option("--fetch_pro_concept", action="store_true", dest="fetch_pro_concept_f", default=False, help="")
    parser.add_option("--fetch_pro_repurchase", action="store_true", dest="fetch_pro_repurchase_f", default=False, help="")
    parser.add_option("--fetch_cctv_news", action="store_true", dest="fetch_cctv_news_f", default=False, help="")
    parser.add_option("--fetch_new_share", action="store_true", dest="fetch_new_share_f", default=False, help="")
    parser.add_option("--fetch_industry_l123", action="store_true", dest="fetch_industry_l123_f", default=False, help="")
    parser.add_option("--fetch_change_name", action="store_true", dest="fetch_change_name_f", default=False, help="")
    parser.add_option("--fetch_pledge_stat_detail", action="store_true", dest="fetch_pledge_stat_detail_f", default=False, help="")
    parser.add_option("--fetch_stock_company", action="store_true", dest="fetch_stock_company_f", default=False, help="")

    parser.add_option("-e", "--extract_latest", action="store_true", dest="extract_latest_f", default=False, help="extract latest quarter data")

    parser.add_option("-m", "--merge_quarterly", action="store_true", dest="merge_quarterly_f", default=False, help="merge date by quarter")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on the merged quarterly csv")

    parser.add_option("--concept_top", action="store_true", dest="concept_top_f", default=False, help="analyze top 3 stocks in each concept")

    parser.add_option("--overwrite", action="store_true", dest="overwrite_f", default=False, help="overwrite existing analyse output csv, using with -a")

    parser.add_option("--fully_a", action="store_true", dest="fully_a_f", default=False, help="run all the analyze steps, using with -a")

    parser.add_option("-d", "--daily_a", action="store_true", dest="daily_a_f", default=False, help="only run analyze_step_6, using with -a")

    parser.add_option("-s", "--sum_mainbz", action="store_true", dest="sum_mainbz_f", default=False, help="sum_mainbz, output fina_mainbz_sum.csv")

    parser.add_option("--percent_mainbz_f", action="store_true", dest="percent_mainbz_f", default=False, help="calc each item percent in the mainbz, output source/latest/fina_mainbz_percent.csv")

    parser.add_option("-c", "--fast_fetch", action="store_true", dest="fast_fetch_f", default=False, help="only fetch stocks whose high score >70.")

    parser.add_option("--wh_hencow_fcf", action="store_true", dest="white_horse_hencow_fcf_f", default=False, help="extract white horse, hen, cow, freecashflow.")
    parser.add_option("--industry_top", action="store_true", dest="industry_top_f", default=False, help="analysis industry_top from mv_eps, mainbz_profit views. results saved to TWO csv.")

    parser.add_option("--merge_individual", action="store_true", dest="merge_individual_f", default=False, help="consolidate indiviaul from each period.")

    parser.add_option("--merge_local", action="store_true", dest="merge_local_f", default=False, help="consolidate individual csv to source/[*].csv")
    parser.add_option("--merge_local_basic", action="store_true", dest="merge_local_basic_f", default=False, help="consolidate daily basic to source/basic.csv")

    parser.add_option("--big_memory", action="store_true", dest="big_memory_f", default=False, help="consumes 4G memory to load all the jaqs and tushare data to two df")

    parser.add_option("-u", "--debug", action="store_true", dest="debug_f", default=False, help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--express_notify", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")
    parser.add_option("--generate_today_fund1_fund2_stock_basic", action="store_true", dest="generate_today_fund1_fund2_stock_basic_f", default=False, help="merge and calc fund1 and fund2 today basic")

    parser.add_option("--disclosure_date_notify_day", type="int", dest="disclosure_date_notify_day_f", default=None, help="generate stock list that will be disclosured in the give days.")

    # parser.add_option("-v", "--verify_fund_increase", action="store_true",
    #                  dest="verify_fund_increase_f", default=False,
    #                  help="verify quartly score and buy and increase to today")

    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all_f
    fetch_new_share_f = options.fetch_new_share_f
    fetch_industry_l123_f = options.fetch_industry_l123_f
    fetch_change_name_f = options.fetch_change_name_f
    extract_latest_f = options.extract_latest_f
    merge_quarterly_f = options.merge_quarterly_f
    analyze_f = options.analyze_f
    concept_top_f = options.concept_top_f
    overwrite_f = options.overwrite_f
    fully_a_f = options.fully_a_f
    daily_a_f = options.daily_a_f
    sum_mainbz_f = options.sum_mainbz_f
    percent_mainbz_f = options.percent_mainbz_f
    fast_fetch_f = options.fast_fetch_f
    white_horse_hencow_fcf_f = options.white_horse_hencow_fcf_f
    industry_top_f = options.industry_top_f
    merge_individual_f = options.merge_individual_f
    merge_local_f = options.merge_local_f
    merge_local_basic_f = options.merge_local_basic_f
    big_memory_f = options.big_memory_f
    debug_f = options.debug_f
    force_run_f = options.force_run_f
    disclosure_date_notify_day_f = options.disclosure_date_notify_day_f
    generate_today_fund1_fund2_stock_basic_f = options.generate_today_fund1_fund2_stock_basic_f

    # verify_fund_increase_f = options.verify_fund_increase_f

    logging.info(__file__ + " " + "fetch_all_f: " + str(fetch_all_f))
    logging.info(__file__ + " " + "fetch_new_share_f: " + str(fetch_new_share_f))
    logging.info(__file__ + " " + "fetch_industry_l123_f: " + str(fetch_industry_l123_f))
    logging.info(__file__ + " " + "extract_latest_f: " + str(extract_latest_f))
    logging.info(__file__ + " " + "merge_quarterly_f: " + str(merge_quarterly_f))
    logging.info(__file__ + " " + "analyze_f: " + str(analyze_f))
    logging.info(__file__ + " " + "concept_top_f: " + str(concept_top_f))
    logging.info(__file__ + " " + "overwrite_f: " + str(overwrite_f))
    logging.info(__file__ + " " + "fully_a_f: " + str(fully_a_f))
    logging.info(__file__ + " " + "daily_a_f: " + str(daily_a_f))
    logging.info(__file__ + " " + "sum_mainbz_f: " + str(sum_mainbz_f))
    logging.info(__file__ + " " + "percent_mainbz_f: " + str(percent_mainbz_f))
    logging.info(__file__ + " " + "fast_fetch_f: " + str(fast_fetch_f))
    logging.info(__file__ + " " + "white_horse_hencow_fcf_f: " + str(white_horse_hencow_fcf_f))
    logging.info(__file__ + " " + "industry_top_f: " + str(industry_top_f))
    logging.info(__file__ + " " + "merge_individual_f: " + str(merge_individual_f))
    logging.info(__file__ + " " + "merge_local_f: " + str(merge_local_f))
    logging.info(__file__ + " " + "merge_local_basic_f: " + str(merge_local_basic_f))
    logging.info(__file__ + " " + "big_memory_f: " + str(big_memory_f))
    logging.info(__file__ + " " + "debug_f: " + str(debug_f))
    logging.info(__file__ + " " + "force_run_f: " + str(force_run_f))
    logging.info(__file__ + " " + "disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))
    logging.info(__file__ + " " + "generate_today_fund1_fund2_stock_basic: " + str(generate_today_fund1_fund2_stock_basic_f))
    # logging.info(__file__+" "+"disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))

    set_global(debug=debug_f, big_memory=big_memory_f, force_run=force_run_f)

    if options.fetch_all_f or options.fetch_pro_fund_f:
        set_global_pro_fetch_field()

    if options.industry_top_f:


        df1,df1_100 = industry_top_mainbz_profit()
        df2,df2_100 = industry_top_mv_eps()

        df3 = pd.merge(left=df2, right=df1, how='inner', on='code')
        df3.to_csv("/home/ryan/DATA/result/industry_top_fund2.csv")
        print(1)

    if options.fetch_pro_basic_f:
        _fetch_pro_basic()

    if options.fetch_stock_company_f:
        _fetch_stock_company()

    if options.fetch_stk_holdertrade_f:
        _fetch_stk_holdertrade(fast_fetch=fast_fetch_f)

    if options.fetch_pro_fund_f:
        fetch_pro_fund(fast_fetch=fast_fetch_f)

    if options.fetch_basic_quarterly_f:
        fetch_basic_quarterly()

    if options.fetch_basic_daily_f:
        fetch_basic_daily(fast_fetch=fast_fetch_f)

    if options.fetch_info_daily_f:
        fetch_info_daily(fast_fetch=fast_fetch_f)

    if options.fetch_pro_concept_f:
        _fetch_pro_concept()

    if options.fetch_pro_repurchase_f:
        _fetch_pro_repurchase()

    if options.fetch_cctv_news_f:
        _fetch_cctv_news()

    if options.concept_top_f:
        concept_top()

    if options.fetch_new_share_f:
        fetch_new_share()


    if options.fetch_industry_l123_f:
        fetch_industry_l123()


    if options.fetch_change_name_f:
        _fetch_change_name()

    if options.fetch_pledge_stat_detail_f:
        _fetch_pro_pledge_stat_detail()

    if options.generate_today_fund1_fund2_stock_basic_f:
        finlib.Finlib().generate_today_fund1_fund2_stock_basic()
        finlib.Finlib().generate_common_fund_df()

    if options.fetch_all_f:
        ##############
        # fast first
        ##############
        _fetch_change_name()
        fetch_new_share()
        fetch_basic_daily(fast_fetch=fast_fetch_f)  # 300 credits
        _fetch_pro_concept()  # 300 credits
        _fetch_pro_repurchase()  # 600 credits
        # _fetch_cctv_news() #120 credits.  5 times/minute

        ##############
        # then timecosting
        ##############
        _fetch_pro_basic()
        # _fetch_stk_holdertrade(fast_fetch=fast_fetch_f) #don't have 2000 api credits
        fetch_pro_fund(fast_fetch=fast_fetch_f)
        fetch_basic_quarterly()
        _fetch_pro_pledge_stat_detail()

    elif merge_individual_f:
        # generate source/individual_per_stock/*_basic.csv from source/basic.csv
        merge_individual_bash_basic(fast_fetch=fast_fetch_f)

        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()["process_fund_or_not"]:
            logging.info(__file__ + " " + "not processing fundermental data at this month. ")
            exit()
        else:
            # generate source/individual_per_stock/*.csv from source/*.csv
            merge_individual()

    elif merge_local_f:
        # generate source/*.csv, e.g source(source.dev)/income.csv;
        # combine source/income.csv ... from source/individual_per_stock/*.csv
        # merge_local()
        # not fetching/calculating fundamental data at month 5,6,9, 11, 12
        if (not force_run_global) and (not finlib.Finlib().get_report_publish_status()["process_fund_or_not"]):
            logging.info(__file__ + " " + "not processing fundamental data at this month. ")
            exit()
        else:
            merge_local_bash(debug=debug_global)
    elif merge_local_basic_f:
        merge_local_bash_basic(csv_basic, fast=fast_fetch_f)
    elif extract_latest_f:
        extract_latest()
    elif merge_quarterly_f:
        merge_local_bash_basic_quarterly()

        if (debug_f) or (big_memory_f):
            merge_quarterly(fast=fast_fetch_f)
        else:
            logging.info(__file__ + " " + "merge quarterly requires lot memory, use with either --big_memory, or --debug")
    elif sum_mainbz_f:
        sum_fina_mainbz()
    elif percent_mainbz_f:
        percent_fina_mainbz()
    elif analyze_f:

        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()["process_fund_or_not"]:
            logging.info(__file__ + " " + "not processing fundermental data at this month. ")
            exit()
        else:
            analyze(fully_a=fully_a_f, daily_a=daily_a_f, fast=fast_fetch_f)
    # elif verify_fund_increase_f:
    #    verify_fund_increase()
    elif white_horse_hencow_fcf_f:
        # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/white_horse.csv
        extract_white_horse()

        # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/hen_cow.csv
        extract_hen_cow()

        # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/freecashflow_price_ratio.csv
        extract_high_freecashflow_price_ratio()
    elif disclosure_date_notify_day_f:
        disclosure_date_notify(days=disclosure_date_notify_day_f)

    logging.info("script completed")
    os._exit(0)


### MAIN ####
if __name__ == "__main__":
    main()
