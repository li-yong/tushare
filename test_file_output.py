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

#import matplotlib.pyplot as plt
#import matplotlib.dates as mdates

from sklearn.cluster import KMeans

base = "/home/ryan/DATA"
base_rst = "/home/ryan/DATA/result"
base_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"


def verify_file_exist_update(hide_pass=True, print_len=True):

    #base_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"
    #print("====  "+base_fund_2+ " ====")
    #finlib.Finlib().file_verify(base_fund_2 + "/merged/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    #exit()

    #hide_pass = True
    #print_len = True  #slow

    finlib.Finlib().file_verify("/home/ryan/DATA/result/fundamental_peg_2019_4.csv", day=3, hide_pass=hide_pass, print_len=print_len)

    print("====  " + base + " ====")
    #finlib.Finlib().file_verify(base+"/DAY_JAQS/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_No_Adj/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/AG/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/CH/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/KG/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/HK/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/MG/*.csv", day=3, hide_pass=True, print_len=False)
    #finlib.Finlib().file_verify(base + "/DAY_Global/US/*.csv", day=3, hide_pass=True, print_len=False)

    print("====  " + base_rst + " ====")
    finlib.Finlib().file_verify(base_rst + "/pe_pb_rank.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/pe_pb_rank_selected.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/today/announcement.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fenghong_profit_analysis.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fenghong_score.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/Fundamental_Quarter_Report_2019_4.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/latest_fundamental_quarter.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/latest_fundamental_day.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/today/fundamentals.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/jaqs_quarterly_fundamental.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental_peg.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental_peg_2019_4.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental_peg_2019_4_selected.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/latest_fundamental_day.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/latest_fundamental_quarter.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/Fundamental_Quarter_Report_2019_4.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/latest_fundamental_peg.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/fundamental_peg_2019_4.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/industry_top.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/area_top.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/jaqs/jaqs_all.pickle", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/ts_all.pickle", day=3, hide_pass=hide_pass, print_len=print_len)

    print("====  " + base_fund_2 + " ====")
    #finlib.Finlib().file_verify(base_fund_2 + "/individual_per_stock/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    #finlib.Finlib().file_verify(base_fund_2 + "/merged/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    #finlib.Finlib().file_verify(base_fund_2 + "/source/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)

    #finlib.Finlib().file_verify(base_fund_2 + "/source/latest/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)

    #finlib.Finlib().file_verify(base_fund_2 + "/source/market/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)

    #finlib.Finlib().file_verify(base_fund_2 + "/source/individual_per_stock/*.csv", day=3, hide_pass=hide_pass, print_len=False)

    print("====  " + base_fund_2 + "/report ====")
    finlib.Finlib().file_verify(base_fund_2 + "/report/step1/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step2/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step3/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step4/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step5/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step6/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step7/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/step8/*.csv", day=3, hide_pass=hide_pass, print_len=print_len)

    finlib.Finlib().file_verify(base_fund_2 + "/report/white_horse.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/hen_cow.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/freecashflow_price_ratio.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/key_points_AG_today.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_fund_2 + "/report/key_points_AG_today_selected.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    finlib.Finlib().file_verify(base_rst + "/today/talib_and_pv_no_db_filter_AG.csv", day=3, hide_pass=hide_pass, print_len=print_len)
    #finlib.Finlib().file_verify("", day=3, hide_pass=hide_pass, print_len=print_len)
    #finlib.Finlib().file_verify("", day=3, hide_pass=hide_pass, print_len=print_len)

    print(" ==== verify_file_exist_update completed ====")


def verify_price_utd():
    today_s = finlib.Finlib().get_last_trading_day()  #2019-03-06
    today_s_us = finlib.Finlib().get_last_trading_day_us()  #2019-03-06
    today_p = finlib.Finlib().get_price("SH600519", today_s)

    today_s_short = re.sub("-", "", finlib.Finlib().get_last_trading_day())  #20190306

    #######check basic

    csvf = base_fund_2 + "/source/basic_daily/basic_" + today_s_short + ".csv"
    finlib.Finlib().file_verify(csvf, day=1, hide_pass=False, print_len=True)
    finlib.Finlib().pprint(pd.read_csv(csvf).iloc[-1:])
    if (not pd.isnull(today_p)) and (today_p != 10000000000):
        print("PASS. AG basic is update to date. " + csvf + "\n")
    else:
        print("FAIL. AG basic is not update to date " + csvf + "\n")

    #######check ag
    csvf = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(csvf)
    finlib.Finlib().pprint(df.iloc[-1:])
    if len(df[df["date"] == today_s]) == 1:
        print("PASS. AG stock is update to date. " + csvf + "\n")
    else:
        print("FAIL. AG stock is not update to date. " + csvf + "\n")

    #######check hk
    csvf = "/home/ryan/DATA/DAY_Global/HK/00700.csv"
    df = pd.read_csv(csvf)
    finlib.Finlib().pprint(df.iloc[-1:])
    if len(df[df["date"] == today_s]) == 1:
        print("PASS. HK stock price is update to date." + csvf + "\n")
    else:
        print("FAIL. HK stock price is not update to date." + csvf + "\n")

    #######check us
    today_s_us = finlib.Finlib().get_last_trading_day_us()

    csvf = "/home/ryan/DATA/DAY_Global/stooq/US/AAPL.csv"
    df = pd.read_csv(csvf)
    finlib.Finlib().pprint(df.iloc[-1:])
    if len(df[df["date"] == today_s_us]) == 1:
        print("PASS. US stock price is update to date. " + csvf + "\n")
    else:
        print("FAIL. US stock price is not update to date " + csvf + "\n")

    print(" ==== verify_price_utd completed ====")


def verify_fund_data():
    d = finlib.Finlib().get_report_publish_status()['completed_year_rpt_date']  #'20171231'

    dir_root = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"
    dir_individual = dir_root + "/source/individual/" + d

    df_merged = pd.read_csv(dir_root + "/merged/merged_all_" + d + ".csv", encoding="utf-8")
    df_merged = df_merged[df_merged['ts_code'] == '600519.SH']
    if df_merged.__len__() != 1:
        print("FAIL, merged len not equal 1.  df_merged")
        return ()

    ################
    df_sr = pd.read_csv(dir_individual + "/600519.SH_balancesheet.csv", encoding="utf-8")

    if df_sr.__len__() != 1:
        print("FAIL, source balancesheet len not equal 1. " + dir_individual + "/600519.SH_balancesheet.csv")
        return ()

    if (df_sr.iloc[0]['total_share'] != df_merged.iloc[0]['total_share']):
        print("FAIL, df_merged, field: total_share")

    if (df_sr.iloc[0]['money_cap'] != df_merged.iloc[0]['money_cap']):
        print("FAIL, df_merged, field: money_cap")

    if (df_sr.iloc[0]['prepayment'] != df_merged.iloc[0]['prepayment']):
        print("FAIL, df_merged, field: prepayment")

    ################
    df_sr = pd.read_csv(dir_individual + "/600519.SH_cashflow.csv", encoding="utf-8")
    if df_sr.__len__() != 1:
        print("FAIL, source cashflow len not equal 1")
        return ()

    if (df_sr.iloc[0]['net_profit'] != df_merged.iloc[0]['net_profit']):
        print("FAIL, df_merged, field: net_profit")

    if (df_sr.iloc[0]['c_fr_sale_sg'] != df_merged.iloc[0]['c_fr_sale_sg']):
        print("FAIL, df_merged, field: c_fr_sale_sg")

    if (df_sr.iloc[0]['c_paid_to_for_empl'] != df_merged.iloc[0]['c_paid_to_for_empl']):
        print("FAIL, df_merged, field: c_paid_to_for_empl")

    ################
    df_sr = pd.read_csv(dir_individual + "/600519.SH_income.csv", encoding="utf-8")
    if df_sr.__len__() != 1:
        print("FAIL, source income len not equal 1")
        return ()

    if (df_sr.iloc[0]['basic_eps'] != df_merged.iloc[0]['basic_eps']):
        print("FAIL, df_merged, field: basic_eps")

    if (df_sr.iloc[0]['total_revenue'] != df_merged.iloc[0]['total_revenue']):
        print("FAIL, df_merged, field: total_revenue")

    if (df_sr.iloc[0]['total_cogs'] != df_merged.iloc[0]['total_cogs']):
        print("FAIL, df_merged, field: total_cogs")

    ################
    df_sr = pd.read_csv(dir_individual + "/600519.SH_fina_indicator.csv", encoding="utf-8")
    if df_sr.__len__() != 1:
        print("FAIL, source fina_indicator len not equal 1")
        return ()

    if (df_sr.iloc[0]['revenue_ps'] != df_merged.iloc[0]['revenue_ps']):
        print("FAIL, df_merged, field: revenue_ps")

    if (df_sr.iloc[0]['gross_margin'] != df_merged.iloc[0]['gross_margin']):
        print("FAIL, df_merged, field: total_revenue")

    if (df_sr.iloc[0]['fcff'] != df_merged.iloc[0]['fcff']):
        print("FAIL, df_merged, field: fcff")

    print("PASS, ~/DATA/pickle/Stock_Fundamental/fundamentals_2/merged")

    #------------------------------------------------------------------
    df_sr = pd.read_csv(dir_individual + "/600519.SH_balancesheet.csv", encoding="utf-8")
    df_feature = pd.read_csv(dir_root + "/source/balancesheet.csv", encoding="utf-8")
    df_feature = df_feature.query("ts_code=='600519.SH' and end_date=='" + d + "'")

    if df_feature.__len__() != 1:
        print("FAIL, source balancesheet len not equal 1. " + dir_root + "/source/balancesheet.csv")
        return ()

    if (df_sr.iloc[0]['total_share'] != df_feature.iloc[0]['total_share']):
        print("FAIL, df_feature, field: total_share")

    if (df_sr.iloc[0]['money_cap'] != df_feature.iloc[0]['money_cap']):
        print("FAIL, df_feature, field: money_cap")

    if (df_sr.iloc[0]['prepayment'] != df_feature.iloc[0]['prepayment']):
        print("FAIL, df_feature, field: prepayment")

    #---
    df_sr = pd.read_csv(dir_individual + "/600519.SH_income.csv", encoding="utf-8")
    df_feature = pd.read_csv(dir_root + "/source/income.csv", encoding="utf-8")
    df_feature = df_feature.query("ts_code=='600519.SH' and end_date=='" + d + "'")

    if df_feature.__len__() != 1:
        print("FAIL, source income len not equal 1. " + dir_root + "/source/income.csv")
        return ()

    if (df_sr.iloc[0]['basic_eps'] != df_feature.iloc[0]['basic_eps']):
        print("FAIL, df_feature, field: basic_eps")

    if (df_sr.iloc[0]['total_revenue'] != df_feature.iloc[0]['total_revenue']):
        print("FAIL, df_feature, field: total_revenue")

    if (df_sr.iloc[0]['total_cogs'] != df_feature.iloc[0]['total_cogs']):
        print("FAIL, df_feature, field: total_cogs")

    # ---
    # '''
    df_sr = pd.read_csv(dir_individual + "/600519.SH_cashflow.csv", encoding="utf-8")
    df_feature = pd.read_csv(dir_root + "/source/cashflow.csv", encoding="utf-8")
    df_feature = df_feature.query("ts_code=='600519.SH' and end_date=='" + d + "'")

    if df_feature.__len__() != 1:
        print("FAIL, source cashflow len not equal 1. " + dir_root + "/source/cashflow.csv")
        return ()

    if (df_sr.iloc[0]['net_profit'] != df_feature.iloc[0]['net_profit']):
        print("FAIL, df_feature, field: net_profit")

    if (df_sr.iloc[0]['c_fr_sale_sg'] != df_feature.iloc[0]['c_fr_sale_sg']):
        print("FAIL, df_feature, field: c_fr_sale_sg")

    if (df_sr.iloc[0]['c_paid_to_for_empl'] != df_feature.iloc[0]['c_paid_to_for_empl']):
        print("FAIL, df_feature, field: c_paid_to_for_empl")
    # '''
    #---
    df_sr = pd.read_csv(dir_individual + "/600519.SH_fina_indicator.csv", encoding="utf-8")
    df_feature = pd.read_csv(dir_root + "/source/fina_indicator.csv", encoding="utf-8")
    df_feature = df_feature.query("ts_code=='600519.SH' and end_date=='" + d + "'")

    if df_feature.__len__() != 1:
        print("FAIL, source fina_indicator len not equal 1. " + dir_root + "/source/fina_indicator.csv")
        return ()

    if (df_sr.iloc[0]['revenue_ps'] != df_feature.iloc[0]['revenue_ps']):
        print("FAIL, df_feature, field: revenue_ps")

    if (df_sr.iloc[0]['gross_margin'] != df_feature.iloc[0]['gross_margin']):
        print("FAIL, df_feature, field: gross_margin")

    if (df_sr.iloc[0]['fcff'] != df_feature.iloc[0]['fcff']):
        print("FAIL, df_feature, field: fcff")

    print("PASS, ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv")
    print(" ==== verify_fund_data completed ====")


def main():
    # print_len=True ==> slow

    verify_price_utd()
    verify_fund_data()
    verify_file_exist_update(hide_pass=False, print_len=True)
    #verify_file_exist_update(hide_pass=True, print_len = True)


### MAIN ####
if __name__ == '__main__':
    main()
    exit(0)
