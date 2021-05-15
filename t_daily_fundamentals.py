# coding: utf-8

import sys, traceback, threading
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
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
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info.
global force_run_global

#most recent report date
the_latest_report_date = finlib.Finlib().get_report_publish_status()
year = the_latest_report_date['completed_quarter_year']  #int 2018
quarter = the_latest_report_date['completed_quarter_number']  #int 3
#print("head year is "+str(year))

#year = re.match("(\d{4})\d{4}", finlib.Finlib().get_year_month_quarter()['stable_report_perid']).group(1)
#quarter=4

#dump_csv= "/home/ryan/DATA/result/today/fundamentals_"+todayS+".csv"  #Fund Ana based on Today data
dump_csv_d = "/home/ryan/DATA/result/today/fundamentals.csv"  #Fund Ana based on Today data
dump_csv_q = "/home/ryan/DATA/result/Fundamental_Quarter_Report_" + str(year) + "_" + str(quarter) + ".csv"  #Fund Ana based on Quarter Report

#Fund Ana based on Quarter Report
#soft link to --> Fundamental_Quarter_Report_2018_2.csv
dump_csv_q_sym_link = "/home/ryan/DATA/result/latest_fundamental_quarter.csv"

#Fund Ana based on Quarter Report
#soft link to --> /home/ryan/DATA/result/today/fundamentals.csv
#dump_csv_d= "/home/ryan/DATA/result/latest_fundamental_day.csv"
dump_csv_d_sym_link = "/home/ryan/DATA/result/latest_fundamental_day.csv"
#df_q_r_csv= "~/Quarter_fundamentals_Dump_"+str(year)+"_"+str(quarter)+".csv"

#base_dir='/home/ryan/DATA/DAY'


def refine_hist_data(df=None, renew=False, debug=False):
    #feed by result of _combine_all()

    df_result = pd.DataFrame()
    refined_fundamental_merged_csv = "/home/ryan/DATA/result/fundamental.csv"

    if finlib.Finlib().is_cached(refined_fundamental_merged_csv, 7) and (not force_run_global) and (not renew):
        logging.info(__file__+" "+"file has already been updated in 7 days. " + refined_fundamental_merged_csv)
        df_result = pandas.read_csv(refined_fundamental_merged_csv, converters={'code': str})
        return (df_result)

    if os.path.isfile(refined_fundamental_merged_csv) and (not renew):
        logging.info(__file__+" "+"loading refineded fund from " + refined_fundamental_merged_csv)
        df_result = pandas.read_csv(refined_fundamental_merged_csv, converters={'code': str})
        return (df_result)

    if df == None:
        df = pd.read_csv("/home/ryan/DATA/tmp/fundamental_merged.csv", converters={'code': str})
        logging.info(__file__+" "+"loading " + "/home/ryan/DATA/tmp/fundamental_merged.csv")

    df.fillna(0, inplace=True)

    if debug:
        df = df.query("code=='000651'")

    all_code = df['code'].unique()
    all_code_len = all_code.__len__()

    cnt = 1
    for code in all_code:
        logging.info(code + " " + str(cnt) + "/" + str(all_code_len))
        cnt += 1
        df_a_stock = df[df['code'] == code].reset_index().drop('index', axis=1)

        for i in range(0, df_a_stock.__len__()):
            if (df_a_stock.iloc[i]['eps'] == 0) and (df_a_stock.iloc[i]['eps_df_profit'] != 0):
                df_a_stock.at[i, 'eps'] = df_a_stock.iloc[i]['eps_df_profit']

            if (df_a_stock.iloc[i]['roe'] == 0) and (df_a_stock.iloc[i]['roe_df_profit'] != 0):
                df_a_stock.at[i, 'roe'] = df_a_stock.iloc[i]['roe_df_profit']

            if (df_a_stock.iloc[i]['eps_yoy'] == 0) and (df_a_stock.iloc[i]['epsg'] != 0):
                df_a_stock.at[i, 'eps_yoy'] = df_a_stock.iloc[i]['epsg']

            if (df_a_stock.iloc[i]['net_profits'] == 0) and (df_a_stock.iloc[i]['net_profits_df_profit'] != 0):
                df_a_stock.at[i, 'net_profits'] = df_a_stock.iloc[i]['net_profits_df_profit'] * 100

            for j in df_a_stock.columns:

                if i == 0 and (pd.isnull(df_a_stock.iloc[0][j]) or df_a_stock.iloc[0][j] == '--'):
                    #logging.info(__file__+" "+"updating code "+code +" row "+ str(i) + " column " + j + " to 0")
                    df_a_stock.at[0, j] = 0
                elif df_a_stock.iloc[i][j] == 0:
                    #logging.info(__file__+" "+"updating  code "+code+ " row "  +str(i)+" "+j + " to "+str(df_a_stock.iloc[i-1][j]))
                    df_a_stock.at[i, j] = df_a_stock.iloc[i - 1][j]  #update with previous quarter data

        df_result = df_result.append(df_a_stock)

    df_result = df_result.reset_index().drop('index', axis=1)
    df_result.code = df_result.code.astype(str)  #convert the code from numpy.int to string.
    df_result = finlib.Finlib().change_df_columns_order(df_result, col_list_to_head=['code', 'name', 'year_quarter', 'trade_date'])
    df_result.to_csv(refined_fundamental_merged_csv, encoding='UTF-8', index=False)

    logging.info(__file__ + ": " + "fundamental_merged_csv saved to " + refined_fundamental_merged_csv + " , len " + str(df_result.__len__()))
    return (df_result)


#run after the data is there.
#this function should be considered as a temp funcation, it's result should feed to refine_hist_data to get
#the finally useful result
def _combine_all(year_end, quarter_end, debug=False):
    #load df_pro_basic, to substitue df_jaqs
    df_pro_basic = finlib.Finlib().load_ts_pro_basic_quarterly()

    fundamental_merged_csv = "/home/ryan/DATA/tmp/fundamental_merged.csv"
    df_result = pd.DataFrame()

    #for y in range(2000,int(year_end)+1):
    for y in range(2019, int(year_end) + 1):

        for q in range(1, 5):
            if str(y) == year_end and str(q) > quarter_end:
                logging.info(__file__+" "+"request quarter is in future. year " + str(y) + " q " + str(q))
                continue

            #get the df_pro_basic
            if str(q) == "1":
                file_date = "03"  # cannot decied which day it is, could be 0329, if 0331 is not a trading day.
                file_date_calen = "0331"

            elif str(q) == "2":
                file_date = "06"
                file_date_calen = "0630"

            elif str(q) == "3":
                file_date = "09"
                file_date_calen = "0930"

            elif str(q) == "4":
                file_date = "12"
                file_date_calen = "1231"

            merged_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged"
            if not os.path.isdir(merged_dir):
                os.mkdir(merged_dir)
            merged_quarter_csv = merged_dir + "/merged_all_" + str(y) + str(file_date_calen) + ".csv"

            #if finlib.Finlib().is_cached(merged_quarter_csv, 2) and (not force_run_global):
            #    logging.info(__file__+" "+"file has already been updated in 1 days. " + merged_quarter_csv)

            df_pro_basic_sub = df_pro_basic[df_pro_basic.trade_date.str.match(str(y) + file_date)]
            df_pro_basic_sub = finlib.Finlib().remove_market_from_tscode(df_pro_basic_sub)

            y_q = str(y) + "_" + str(q)  #'2017-1'
            logging.info(__file__+" "+"y_q " + y_q)  #ryan debug
            (df_report, df_profit, df_operation, df_growth, df_debtpaying, df_cashflow) = fetch(y, q)

            #df_the_quarter  = df_basic #removed this basic as it is always the newest

            #df_the_quarter  = df_report
            df_the_quarter = df_pro_basic_sub

            for df_s in ('df_report', 'df_profit', 'df_operation', 'df_growth', 'df_debtpaying', 'df_cashflow'):
                df = pd.DataFrame()  #empty it
                logging.info(__file__+" "+"merging " + y_q + " " + df_s)

                df = eval(df_s)

                try:
                    df_the_quarter = pd.merge(df_the_quarter, df, on='code', how='outer', suffixes=('', '_' + df_s))
                except:
                    logging.info('exception when merging ' + df_s + ', y ' + str(y) + " q " + str(q))
                    continue

            new_value_df = pd.DataFrame([y_q] * df_the_quarter.__len__(), columns=['year_quarter'])
            df_the_quarter = df_the_quarter.join(new_value_df)  # inserted column at the tail

            df_the_quarter = finlib.Finlib().remove_df_columns(df_the_quarter, "name_.*")  # remove dup name_*, e.g name_df_cashflow
            df_the_quarter = finlib.Finlib().change_df_columns_order(df_the_quarter, ['code', 'name', 'year_quarter', 'trade_date'])
            df_the_quarter = df_the_quarter.drop_duplicates()
            df_the_quarter.code = df_the_quarter.code.astype(str)

            df_the_quarter.to_csv(merged_quarter_csv, encoding='UTF-8', index=False)
            logging.info(__file__+" "+"fundamental quarterly merged csv saved, " + merged_quarter_csv + " len " + str(df_the_quarter.__len__()))

            df_result = df_result.append(df_the_quarter)  #append the merged quarter to the result
            logging.info('merged result ' + str(df_result.__len__()))

    cols = [
        #my adding
        'year_quarter',
        #report
        'code',
        'name',
        'eps',
        'eps_yoy',
        'bvps',
        'roe',
        'epcf',
        'net_profits',
        'profits_yoy',  # 'distrib', 'report_date',
        #profit
        'net_profit_ratio',
        'gross_profit_rate',
        'business_income',
        'bips',
        #growth
        'mbrg',
        'nprg',
        'nav',
        'targ',
        'epsg',
        'seg',
        #cashflow
        'cf_sales',
        'rateofreturn',
        'cf_nm',
        'cf_liabilities',
        'cashflowratio',
        #debtpaying
        'currentratio',
        'quickratio',
        'cashratio',
        'icratio',
        'sheqratio',
        'adratio',
        #operation
        'arturnover',
        'arturndays',
        'inventory_turnover',
        'inventory_days',
        'currentasset_turnover',
        'currentasset_days',
    ]
    #df_result = df_result[cols]  #ryan 20180519, keep all rows

    df_result = finlib.Finlib().remove_df_columns(df_result, "name_.*")  #remove dup name_*, e.g name_df_cashflow
    df_result = finlib.Finlib().change_df_columns_order(df_result, ['code', 'name', 'year_quarter'])

    df_result = df_result.drop_duplicates()
    df_result.code = df_result.code.astype(str)  #convert the code from numpy.int to string.

    #debug
    #df_result = df_result[df_result['code'] == '000651'].reset_index().drop('index', axis=1)

    if debug:
        pass  #debug moved to refine_hist_data

    df_result.to_csv(fundamental_merged_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "fundamental_merged_csv saved to " + fundamental_merged_csv + " , len " + str(df_result.__len__()))

    return (df_result)


def fetch_pickle():
    #dump= "/home/ryan/DATA/pickle/sme.pickle"
    try:
        csvf = "/home/ryan/DATA/pickle/sme.csv"
        df_sme = ts.get_sme_classified()  #中小板
        df_sme = finlib.Finlib().add_market_to_code(df_sme)
        #df_sme.to_pickle(dump)
        #logging.info(__file__+": "+"SME saved to "+dump)
        df_sme.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "SME saved to " + csvf + " , len " + str(df_sme.__len__()))

        #dump= "/home/ryan/DATA/pickle/gem.pickle"
        csvf = "/home/ryan/DATA/pickle/gem.csv"
        df_gem = ts.get_gem_classified()  #创业板
        df_gem = finlib.Finlib().add_market_to_code(df=df_gem)
        #df_gem.to_pickle(dump)
        #logging.info(__file__+": "+"gem saved to "+dump)
        df_gem.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "df_gem saved to " + csvf + " , len " + str(df_gem.__len__()))

        #dump= "/home/ryan/DATA/pickle/sz50.pickle"
        csvf = "/home/ryan/DATA/pickle/sz50.csv"
        df_sz50 = ts.get_sz50s()  # SZ50成份股
        df_sz50 = finlib.Finlib().add_market_to_code(df=df_sz50)
        #df_sz50.to_pickle(dump)
        #logging.info(__file__+": "+"SZ50 saved to "+dump)
        df_sz50.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "df_sz50 saved to " + csvf + " , len " + str(df_sz50.__len__()))

        #dump= "/home/ryan/DATA/pickle/hs300.pickle"
        csvf = "/home/ryan/DATA/pickle/hs300.csv"
        df_hs300 = ts.get_hs300s()  # 沪深300成份股
        df_hs300 = finlib.Finlib().add_market_to_code(df=df_hs300)
        #df_hs300.to_pickle(dump)
        #logging.info(__file__+": "+"HS300 saved to "+dump)
        df_hs300.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "df_hs300 saved to " + csvf + " , len " + str(df_hs300.__len__()))

        #dump= "/home/ryan/DATA/pickle/ZZ500.pickle"
        csvf = "/home/ryan/DATA/pickle/ZZ500.csv"
        df_zz500 = ts.get_zz500s()  #中证500成份股
        df_zz500 = finlib.Finlib().add_market_to_code(df=df_zz500)
        #df_zz500.to_pickle(dump)
        #logging.info(__file__+": "+"ZZ500 saved to "+dump)
        df_zz500.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "df_zz500 saved to " + csvf + " , len " + str(df_zz500.__len__()))

        #dump = "/home/ryan/DATA/pickle/trading_day_2019.pickle"
        #csvf = "/home/ryan/DATA/pickle/trading_day_2019.csv"

        finlib.Finlib().get_ag_trading_day()

    except:
        logging.info(__file__+" "+"exception in t_daily_fundamentals.py  fetch_pickle()")
    finally:
        if sys.exc_info() == (None, None, None):
            pass  # no exception
        else:
            logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
            logging.info(sys.exc_value.message)  # print the human readable unincode
            sys.exc_clear()


def fetch(year, quarter, overwrite=False):

    #logging.info(__file__+" "+"fetching year "+str(year)+" quarter "+str(quarter))
    fund_base = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/" + str(year)

    # 业绩报告
    # code, 代码
    # name, 名称
    # esp, 每股收益
    # eps_yoy, 每股收益同比( %)
    # bvps, 每股净资产
    # roe, 净资产收益率( %)
    # epcf, 每股现金流量(元)
    # net_profits, 净利润(万元)
    # profits_yoy, 净利润同比( %)
    # distrib, 分配方案
    # report_date, 发布日期
    csv_report = fund_base + "/report_" + str(year) + "_" + str(quarter) + ".csv"

    # 盈利能力
    # code, 代码
    # name, 名称
    # roe, 净资产收益率( %)
    # net_profit_ratio, 净利率( %)
    # gross_profit_rate, 毛利率( %)
    # net_profits, 净利润(万元)
    # esp, 每股收益
    # business_income, 营业收入(百万元)
    # bips, 每股主营业务收入(元)
    csv_profit = fund_base + "/profit_" + str(year) + "_" + str(quarter) + ".csv"

    # 营运能力
    # code,代码
    # name,名称
    # arturnover,应收账款周转率(次)
    # arturndays,应收账款周转天数(天)
    # inventory_turnover,存货周转率(次)
    # inventory_days,存货周转天数(天)
    # currentasset_turnover,流动资产周转率(次)
    # currentasset_days,流动资产周转天数(天)
    csv_operation = fund_base + "/operation_" + str(year) + "_" + str(quarter) + ".csv"

    # 成长能力
    # code, 代码
    # name, 名称
    # mbrg, 主营业务收入增长率( %)
    # nprg, 净利润增长率( %)
    # nav, 净资产增长率
    # targ, 总资产增长率
    # epsg, 每股收益增长率
    # seg, 股东权益增长率
    csv_growth = fund_base + "/growth_" + str(year) + "_" + str(quarter) + ".csv"

    # 偿债能力
    # code, 代码
    # name, 名称
    # currentratio, 流动比率
    # quickratio, 速动比率
    # cashratio, 现金比率
    # icratio, 利息支付倍数
    # sheqratio, 股东权益比率
    # adratio, 股东权益增长率
    csv_debtpaying = fund_base + "/debtpaying_" + str(year) + "_" + str(quarter) + ".csv"

    # 现金流量
    # code, 代码
    # name, 名称
    # cf_sales, 经营现金净流量对销售收入比率
    # rateofreturn, 资产的经营现金流量回报率
    # cf_nm, 经营现金净流量与净利润的比率
    # cf_liabilities, 经营现金净流量对负债比率
    # cashflowratio, 现金流量比率
    csv_cashflow = fund_base + "/cashflow_" + str(year) + "_" + str(quarter) + ".csv"

    df_report = df_profit = df_operation = df_growth = df_debtpaying = df_cashflow = pd.DataFrame()
    if not os.path.exists(fund_base):
        os.makedirs(fund_base)

    if ((not finlib.Finlib().is_cached(csv_report,5)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Report, ts.get_report_data" + ". Y " + str(year) + ",Q " + str(quarter))  # 业绩报告（主表）
        try:
            df_report = ts.get_report_data(year, quarter)
            df_report.code = df_report.code.astype(str)  # convert the code from numpy.int to string.
            df_report.reset_index().drop('index', axis=1).to_csv(csv_report, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_report + " , len " + str(df_report.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_report)
    else:
        #logging.info(__file__+" "+"Loading Report"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_report = pandas.read_csv(csv_report, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_report)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_report)

    if ((not finlib.Finlib().is_cached(csv_profit)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Profit, ts.get_profit_data" + ". Y " + str(year) + ",Q " + str(quarter))  # 盈利能力
        try:
            df_profit = ts.get_profit_data(year, quarter)
            df_profit.code = df_profit.code.astype(str)  # convert the code from numpy.int to string.
            df_profit.reset_index().drop('index', axis=1).to_csv(csv_profit, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_profit + " , len " + str(df_profit.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_profit)
    else:
        #logging.info(__file__+" "+"Loading Profit"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_profit = pandas.read_csv(csv_profit, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_profit)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_profit)

    if ((not finlib.Finlib().is_cached(csv_operation)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Operation, ts.get_operation_data" + ". Y " + str(year) + ",Q " + str(quarter))  #营运能力
        try:
            df_operation = ts.get_operation_data(year, quarter)
            df_operation.code = df_operation.code.astype(str)  # convert the code from numpy.int to string.
            df_operation.reset_index().drop('index', axis=1).to_csv(csv_operation, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_operation + " , len " + str(df_operation.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_operation)
    else:
        #logging.info(__file__+" "+"Loading Operation"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_operation = pandas.read_csv(csv_operation, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_operation)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_operation)

    if ((not finlib.Finlib().is_cached(csv_growth)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Growth, ts.get_growth_data" + ". Y " + str(year) + ",Q " + str(quarter))  # 成长能力
        try:
            df_growth = ts.get_growth_data(year, quarter)
            df_growth.code = df_growth.code.astype(str)  # convert the code from numpy.int to string.
            df_growth.reset_index().drop('index', axis=1).to_csv(csv_growth, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_growth + " , len " + str(df_growth.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_growth)
    else:
        #logging.info(__file__+" "+"Loading Growth"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_growth = pandas.read_csv(csv_growth, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_growth)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_growth)

    if ((not finlib.Finlib().is_cached(csv_debtpaying)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Debtpaying, ts.get_debtpaying_data" + ". Y " + str(year) + ",Q " + str(quarter))  # 偿债能力
        try:
            df_debtpaying = ts.get_debtpaying_data(year, quarter)
            df_debtpaying.code = df_debtpaying.code.astype(str)  # convert the code from numpy.int to string.
            df_debtpaying.reset_index().drop('index', axis=1).to_csv(csv_debtpaying, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_debtpaying + " , len " + str(df_debtpaying.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_debtpaying)
    else:
        #logging.info(__file__+" "+"Loading Debtpaying"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_debtpaying = pandas.read_csv(csv_debtpaying, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_debtpaying)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_debtpaying)

    if ((not finlib.Finlib().is_cached(csv_cashflow)) or overwrite) and (year >= 2010):
        logging.info(__file__+" "+"\nGetting Cashflow, ts.get_cashflow_data" + ". Y " + str(year) + ",Q " + str(quarter))  # 现金流量
        try:
            df_cashflow = ts.get_cashflow_data(year, quarter)
            df_cashflow.code = df_cashflow.code.astype(str)  # convert the code from numpy.int to string.
            df_cashflow.reset_index().drop('index', axis=1).to_csv(csv_cashflow, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + csv_cashflow + " , len " + str(df_cashflow.__len__()))
        except:
            logging.info(__file__ + ": " + "get exception " + csv_cashflow)
    else:
        #logging.info(__file__+" "+"Loading Cashflow"+". Y "+str(year)+ ",Q "+str(quarter))
        try:
            df_cashflow = pandas.read_csv(csv_cashflow, converters={'code': str})
            logging.info(__file__ + ": " + "loading " + csv_cashflow)
        except:
            logging.info(__file__ + ": " + "read exception " + csv_cashflow)

    return (df_report, df_profit, df_operation, df_growth, df_debtpaying, df_cashflow)

    #===== Reference =====#
    #This implemented in t_fenghong.py


def fetch_all():

    # finlib.Finlib().fetch_today_stock_basic_fund1()  #get today basic

    fetch_pickle()

    # not fetching/calculating fundermental data at month 5,6,9, 11, 12
    if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
        print("not processing fundermental data at this month. ")
        return ()
    else:
        year_q = finlib.Finlib().get_year_month_quarter()
        fetch(year_q['last_quarter']['year'], year_q['last_quarter']['quarter'], overwrite=True)  #Manually Change last Quarterly
        fetch(year_q['year'], year_q['quarter'], overwrite=True)  #Manually Change last Quarterly
        fetch(year=2020, quarter=3, overwrite=True)
        fetch(2020, 2, overwrite=True)
        fetch(2020, 1, overwrite=True)
        fetch(2019, 4, overwrite=True)
        fetch(2018, 4, overwrite=True)
        fetch(2017, 4, overwrite=True)
        fetch(2016, 4, overwrite=True)

def quarterly_fundamental_any(df_basic, year_quarter, debug=False):

    fields_basic = [
        'code',  #代码
        'name',  #名称
        'industry',  #所属行业
        'area',  #地区
        'pe',  #市盈率
        'outstanding',  #流通股本(亿)
        'totals',  #总股本(亿)
        'totalAssets',  #总资产(万)
        'liquidAssets',  #流动资产
        'fixedAssets',  #固定资产
        'reserved',  #公积金
        'reservedPerShare',  #每股公积金
        'esp',  #每股收益
        'bvps',  #每股净资
        'pb',  #市净率
        'timeToMarket',  #上市日期
        'undp',  #未分利润
        'perundp',  # 每股未分配
        'rev',  #收入同比(%)
        'profit',  #利润同比(%)
        'gpr',  #毛利率(%)
        'npr',  #净利润率(%)
        'holders',  #股东人数
    ]

    fields_report = [
        #'code', #代码
        #'name', #名称
        'eps',  #每股收益
        'eps_yoy',  #每股收益同比(%)
        'bvps',  #每股净资产
        'roe',  #净资产收益率(%)
        'epcf',  #每股现金流量(元)
        'net_profits',  #净利润(万元)
        'profits_yoy',  #净利润同比(%)
        'distrib',  #分配方案
        'report_date',  #发布日期
    ]

    fields_profit = [
        #'code', #代码
        #'name', #名称
        'roe',  #净资产收益率(%)
        'net_profit_ratio',  #净利率(%)
        'gross_profit_rate',  #毛利率(%)
        'net_profits',  #净利润(万元)
        'esp',  #每股收益
        'business_income',  #营业收入(百万元)
        'bips',  #每股主营业务收入(元)
    ]

    fields_operation = [
        #'code', #代码
        #'name', #名称
        'arturnover',  #应收账款周转率(次)
        'arturndays',  #应收账款周转天数(天)
        'inventory_turnover',  #存货周转率(次)
        'inventory_days',  #存货周转天数(天)
        'currentasset_turnover',  #流动资产周转率(次)
        'currentasset_days',  #流动资产周转天数(天)
    ]

    fields_growth = [
        #'code', #代码
        #'name', #名称
        'mbrg',  #主营业务收入增长率(%)
        'nprg',  #净利润增长率(%)
        'nav',  #净资产增长率
        'targ',  #总资产增长率
        'epsg',  #每股收益增长率
        'seg',  #股东权益增长率
    ]

    fields_debtpaying = [
        #'code', #代码
        #'name', #名称
        'currentratio',  #流动比率
        'quickratio',  #速动比率
        'cashratio',  #现金比率, Current ratio.
        'icratio',  #利息支付倍数
        'sheqratio',  #股东权益比率
        'adratio',  #股东权益增长率
    ]

    fields_cashflow = [
        #'code', #代码
        #'name', #名称
        'cf_sales',  #经营现金净流量对销售收入比率
        'rateofreturn',  #资产的经营现金流量回报率
        'cf_nm',  #经营现金净流量与净利润的比率
        'cf_liabilities',  #经营现金净流量对负债比率
        'cashflowratio',  #现金流量比率
    ]

    #year_quarter = '2018_1'

    if (not force_run_global) and finlib.Finlib().is_cached(dump_csv_q, day=5):
        logging.info(__file__+" "+"skip file, it been updated in 5 day. " + dump_csv_q)
        return

    year = re.match("(\d{4})_(\d{1})", year_quarter).group(1)
    quarter = re.match("(\d{4})_(\d{1})", year_quarter).group(2)

    if quarter == "1":
        file_date = "0331"
    elif quarter == "2":
        file_date = "0630"
    elif quarter == "3":
        file_date = "0930"
    elif quarter == "4":
        file_date = "1231"

    ###  get ts pro
    file_pro_basic = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/basic_" + year + file_date + ".csv"
    file_pro_merged = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + year + file_date + ".csv"

    if os.path.exists(file_pro_basic):
        df_pro_basic = pd.read_csv(file_pro_basic, converters={'code': str})
        logging.info(__file__ + ": " + "loading " + file_pro_basic)
        df_pro_basic = finlib.Finlib().remove_market_from_tscode(df_pro_basic)
    else:
        logging.error("no such file, " + file_pro_basic)
        exit()

    if os.path.exists(file_pro_merged):
        df_pro_merged = pd.read_csv(file_pro_merged, converters={'code': str})
        logging.info(__file__ + ": " + "loading " + file_pro_merged)
        df_pro_merged = finlib.Finlib().remove_market_from_tscode(df_pro_merged)
    else:
        logging.error("no such file, " + file_pro_merged)
        exit()

    ###   get ts, which already merged.
    file_merged = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_" + year + file_date + ".csv"

    if os.path.exists(file_merged):
        df_merged = pd.read_csv(file_merged, converters={'code': str})
        logging.info(__file__ + ": " + "loading " + file_merged)
    else:
        logging.error("no such file, " + file_merged)
        exit()

    #df_basic, df_other_six don't have the same issue.
    df_basic = df_basic.reset_index().drop('index', axis=1)

    #df_q_r = pd.merge(df_basic,df_merged_quarterly, on='code', how='outer',suffixes=('','_others'))
    df_q_r = pd.merge(df_merged, df_pro_merged, on='code', how='outer', suffixes=('', '_pro_merged'))
    df_q_r = pd.merge(df_q_r, df_pro_basic, on='code', how='outer', suffixes=('', '_pro_basic'))
    df_q_r = pd.merge(df_q_r, df_basic, on='code', how='outer', suffixes=('', '_basic'))
    #df_q_r = df_merged_quarterly

    #df_q_r = pd.merge(df_q_r,df_merged, on='code', how='outer',suffixes=('','_pro_basic'))
    #logging.info(("df_q_r len: " + str(df_q_r.__len__())))

    #df_q_r.drop('name_others',  axis=1, inplace=True)
    # logging.info(("df_q_r len: " + str(df_q_r.__len__())))

    #remove duplicate
    df_q_r.drop_duplicates(inplace=True)
    df_q_r.fillna(0, inplace=True)

    if debug:
        #df_q_r = df_q_r.query("code=='601398' and year_quarter=='2018_1'")
        df_q_r = df_q_r.query("industry=='家用电器'")  #already in 2018_1

    df_q_r = df_q_r.reset_index().drop('index', axis=1)
    logging.info(("df_q_r len: " + str(df_q_r.__len__())))

    #print "saving df_q_r to "+df_q_r_csv
    #df_q_r.to_csv(df_q_r_csv, encoding='UTF-8')

    logging.info(__file__+" "+"Fundamental Analysising Based on Quarter Report,  " + year + " " + quarter)

    new_value_df = pd.DataFrame({
        #'code': df_q_r['code'],
        'esp_ratio': [0] * df_q_r.__len__(),
    })

    df_q_r = new_value_df.join(df_q_r)

    code_price_map = {}

    for i in range(0, df_q_r.__len__()):
        code = df_q_r.iloc[i]['code']
        #esp = df_q_r.iloc[i]['esp']
        esp = df_q_r.iloc[i]['eps']

        #if code_price_map.has_key(code): #python2
        if code in code_price_map:  #python3
            price = code_price_map[code]
        else:
            code_m = finlib.Finlib().add_market_to_code_single(code)
            price = finlib.Finlib().get_price(code_m)
            code_price_map[code] = price

        if pd.isnull(price):
            esp_ratio = 0
        elif price > 0:
            esp_ratio = esp / price
        else:
            esp_ratio = 0

        df_q_r.iloc[i, df_q_r.columns.get_loc('esp_ratio')] = esp_ratio
    logging.info(__file__+" "+"adding esp_ratio completed")

    esp_ratio_perc_weight = 0.2
    pe_weight = 0.2
    pb_weight = 0.1
    roe_weight = 0.3  #removed since most roe is 0. #overcomed to read from last known roe
    #esp_weight = 0.1
    npr_weight = 0.1
    totalAssets_weight = 0.1

    #ryan add 20180520
    business_income_weight = 0.1
    net_profits_weight = 0.1

    pe_all = df_q_r['pe']
    pb_all = df_q_r['pb']
    roe_all = df_q_r['roe']

    #esp_all=df_q_r['esp']
    esp_all = df_q_r['eps']
    bvps_all = df_q_r['bvps']
    #perundp_all=df_q_r['perundp']
    #profit_all=df_q_r['profit']
    total_profit_all = df_q_r['total_profit']
    #gpr_all = df_q_r['gpr']
    gpr_all = df_q_r['grossprofit_margin']
    #npr_all = df_q_r['npr']
    npr_all = df_q_r['netprofit_margin']
    # holders_all = df_q_r['holders']
    net_profits_all = df_q_r['net_profit']
    business_income_all = df_q_r['business_income']
    bips_all = df_q_r['bips']
    currentasset_turnover_all = df_q_r['currentasset_turnover']
    nav_all = df_q_r['nav']
    targ_all = df_q_r['targ']
    rateofreturn_all = df_q_r['rateofreturn']
    cashflowratio_all = df_q_r['cashflowratio']
    esp_ratio_all = df_q_r['esp_ratio']
    # totalAssets_all = df_q_r['totalAssets']
    totalAssets_all = df_q_r['tangible_asset']

    # business_income_all = df_q_r['business_income']  #营业收入
    business_income_all = df_q_r['revenue']  #营业收入
    # net_profits_all = df_q_r['net_profits']  #净利润
    net_profits_all = df_q_r['n_income']  #净利润

    #ryan modified on 2018.04.23
    new_value_df = pd.DataFrame({
        #'code': df_q_r['code'],
        'result_value_quarter_fundation': [0] * df_q_r.__len__(),
        'esp_ratio_perc': [0] * df_q_r.__len__(),
        #'esp_ratio':[0]*df_q_r.__len__(),
        'esp_perc': [0] * df_q_r.__len__(),
        'pe_perc': [0] * df_q_r.__len__(),
        'pb_perc': [0] * df_q_r.__len__(),
        'roe_perc': [0] * df_q_r.__len__(),
        'totalAssets_perc': [0] * df_q_r.__len__(),
        'business_income_perc': [0] * df_q_r.__len__(),
        'net_profits_perc': [0] * df_q_r.__len__(),
    })

    df_q_r = new_value_df.join(df_q_r)
    #df_q_r = new_value_df.join(df_q_r, lsuffix='_new_value_df')

    for i in range(0, df_q_r.__len__()):
        code = df_q_r.iloc[i]['code']

        if code is None:
            logging.info(__file__+" "+"code is None")

        #logging.info(__file__+" "+"Quarter Analyse on "+code)
        #code_m = finlib.Finlib().add_market_to_code_single(code)
        #price = finlib.Finlib().get_price(code_m)

        pe = df_q_r.iloc[i]['pe']
        pb = df_q_r.iloc[i]['pb']
        roe = df_q_r.iloc[i]['roe']
        #esp = df_q_r.iloc[i]['esp']
        esp = df_q_r.iloc[i]['eps']

        #esp_ratio = 0

        #if price > 0:
        #    esp_ratio =esp/price

        bvps = df_q_r.iloc[i]['bvps']
        #perundp = df_q_r.iloc[i]['perundp']
        #profit = df_q_r.iloc[i]['profit']
        total_profit = df_q_r.iloc[i]['total_profit']
        gpr = df_q_r.iloc[i]['grossprofit_margin']
        npr = df_q_r.iloc[i]['netprofit_margin']
        # holders = df_q_r.iloc[i]['holders']
        net_profits = df_q_r.iloc[i]['net_profit']
        business_income = df_q_r.iloc[i]['business_income']
        bips = df_q_r.iloc[i]['bips']
        currentasset_turnover = df_q_r.iloc[i]['currentasset_turnover']
        nav = df_q_r.iloc[i]['nav']
        targ = df_q_r.iloc[i]['targ']
        rateofreturn = df_q_r.iloc[i]['rateofreturn']
        cashflowratio = df_q_r.iloc[i]['cashflowratio']
        esp_ratio = df_q_r.iloc[i]['esp_ratio']
        totalAssets = df_q_r.iloc[i]['tangible_asset']

        business_income = df_q_r.iloc[i]['revenue']
        net_profits = df_q_r.iloc[i]['n_income']

        pe_perc = 1 - stats.percentileofscore(pe_all, pe) / 100
        pb_perc = 1 - stats.percentileofscore(pb_all, pb) / 100
        roe_perc = stats.percentileofscore(roe_all, roe) / 100

        esp_perc = stats.percentileofscore(esp_all, esp) / 100
        bvps_perc = stats.percentileofscore(bvps_all, bvps) / 100
        #perundp_perc=stats.percentileofscore(perundp_all, perundp)/100
        profit_perc = stats.percentileofscore(total_profit_all, total_profit) / 100
        gpr_perc = stats.percentileofscore(gpr_all, gpr) / 100
        npr_perc = stats.percentileofscore(npr_all, npr) / 100
        # holders_perc = 1 - (stats.percentileofscore(holders_all, holders) / 100)
        net_profits_perc = stats.percentileofscore(net_profits_all, net_profits) / 100
        business_income_perc = stats.percentileofscore(business_income_all, business_income) / 100
        bips_perc = stats.percentileofscore(bips_all, bips) / 100
        currentasset_turnover_perc = stats.percentileofscore(currentasset_turnover_all, currentasset_turnover) / 100
        nav_perc = stats.percentileofscore(nav_all, nav) / 100
        targ_perc = stats.percentileofscore(targ_all, targ) / 100
        rateofreturn_perc = stats.percentileofscore(rateofreturn_all, rateofreturn) / 100
        cashflowratio_perc = stats.percentileofscore(cashflowratio_all, cashflowratio) / 100
        esp_ratio_perc = stats.percentileofscore(esp_ratio_all, esp_ratio) / 100
        totalAssets_perc = stats.percentileofscore(totalAssets_all, totalAssets) / 100
        business_income_perc = stats.percentileofscore(business_income_all, business_income) / 100
        net_profits_perc = stats.percentileofscore(net_profits_all, net_profits) / 100

        #df_q_r.iloc[i, df_q_r.columns.get_loc('esp_ratio')] = esp_ratio

        df_q_r.iloc[i, df_q_r.columns.get_loc('esp_ratio_perc')] = esp_ratio_perc  #cannot determin at this time,
        #  calc later after all records are clear
        #  Maybe we shouldnot have this, as esp_ratio is already in /RMB? Stocks can be compared in parellel.

        df_q_r.iloc[i, df_q_r.columns.get_loc('esp_perc')] = esp_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('pe_perc')] = pe_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('pb_perc')] = pb_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('roe_perc')] = roe_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('totalAssets_perc')] = totalAssets_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('business_income_perc')] = business_income_perc
        df_q_r.iloc[i, df_q_r.columns.get_loc('net_profits_perc')] = net_profits_perc

        critiria_not_meet = False
        critiria = ''

        if (roe <= 0.0):
            critiria_not_meet = True
            critiria += ' roe'

        if (pe <= 0.0):
            critiria_not_meet = True
            critiria += ' pe'

        if (esp_ratio <= 0.0):
            critiria_not_meet = True
            critiria += ' esp_ratio'

        if (pb <= 0.0):
            critiria_not_meet = True
            critiria += ' pb'

        if (npr <= 0.0):
            critiria_not_meet = True
            critiria += ' npr'

        if critiria_not_meet:
            result_value = 0
            df_q_r.iloc[i, df_q_r.columns.get_loc('result_value_quarter_fundation')] = result_value
            logging.info(code + " " + critiria + " indicator not meet the minimal critiria, mark overall score to 0 ")
            continue


        result_value = esp_ratio_perc * esp_ratio_perc_weight + \
                       pe_perc * pe_weight + \
                       pb_perc * pb_weight + \
                       npr_perc * npr_weight + \
                       roe_perc * roe_weight + \
                       totalAssets_perc * totalAssets_weight

        logging.info(__file__+" "+"code " + code + " result " + str(result_value))

        if not np.isnan(result_value):
            df_q_r.iloc[i, df_q_r.columns.get_loc('result_value_quarter_fundation')] = result_value

    cols = [
        'year_quarter',
        'code',
        'name',
        'result_value_quarter_fundation',
        'esp_ratio_perc',
        'esp_ratio',
        'esp_perc',
        'esp',
        'pe_perc',
        'pe',
        'roe_perc',
        'roe',
        'pb_perc',
        'pb',
        'totalAssets_perc',
        'totalAssets',
        'eps',
        'eps_yoy',
        'epsg',
        'industry',
        'area',
        'outstanding',
        'totals',
        'liquidAssets',
        'fixedAssets',
        'reserved',
        'reservedPerShare',
        'bvps',
        'timeToMarket',
        #'undp', 'perundp', 'rev', 'profit', 'gpr', 'npr', 'holders',
        'undp',
        'rev',
        'total_profit',
        'gpr',
        'npr',
        'holders',
        'epcf',
        'net_profits',
        'profits_yoy',
        'net_profit_ratio',
        'gross_profit_rate',
        'business_income',
        'bips',
        'arturnover',
        'arturndays',
        'inventory_turnover',
        'inventory_days',
        'currentasset_turnover',
        'currentasset_days',
        'mbrg',
        'nprg',
        'nav',
        'targ',
        'seg',
        'currentratio',
        'quickratio',
        'cashratio',
        'icratio',
        'sheqratio',
        'adratio',
        'cf_sales',
        'rateofreturn',
        'cf_nm',
        'cf_liabilities',
        'cashflowratio'
    ]

    #df_q_r = df_q_r[cols]
    df_q_r.sort_values('result_value_quarter_fundation', ascending=False, inplace=True)
    #df_q_r = df_q_r.DataFrame.reset_index().drop('index', axis=1) #RYAN:BUG?
    df_q_r = df_q_r.reset_index().drop('index', axis=1)

    df_q_r = finlib.Finlib().change_df_columns_order(df_q_r, ['code', 'name', 'year_quarter', 'trade_date', 'result_value_quarter_fundation', 'business_income_perc', 'esp_perc', 'esp_ratio_perc', 'net_profits_perc', 'pb_perc', 'pe_perc', 'roe_perc', 'totalAssets_perc', 'esp_ratio', 'peg_1', 'peg_4'])

    df_q_r.to_csv(dump_csv_q, encoding='UTF-8', index=True)

    if os.path.lexists(dump_csv_q_sym_link):
        os.unlink(dump_csv_q_sym_link)

    os.symlink(dump_csv_q, dump_csv_q_sym_link)

    logging.info(__file__ + ": " + "Fundamental Analysising Based on Quarter Report, result saved to " + dump_csv_q + " , len " + str(df_q_r.__len__()))
    logging.info(__file__ + ": " + "symbol link created  " + dump_csv_q_sym_link + " -> " + dump_csv_q + " , len " + str(df_q_r.__len__()))


#this func run after calc_ps. seq calc_peg->calc_ps->this
def _quarterly_fundamental_any_2(debug=False):

    #This function using same file as input and output,
    # checking cached will result the function not be run, so should not check the file age.
    #if (not force_run_global) and  finlib.Finlib().is_cached(dump_csv_q, day=5):
    #    logging.info(__file__+" "+"skip file, it been updated in 5 day. "+dump_csv_q)
    #    return

    if not os.path.isfile(dump_csv_q):
        logging.info(__file__+" "+"not found source file. " + dump_csv_q)
        exit(1)

    cols_position = ['score_sum', 'result_value_2', 'ps_perc', 'npr_perc', 'net_profits_perc', 'npr_mtp_profit', 'npr_mtp_profit_perc', 'peg_1_perc', 'peg_4_perc']

    df_base_q = pd.read_csv(dump_csv_q, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + dump_csv_q)

    base_cols_q = df_base_q.columns.tolist()

    for co in cols_position:
        if co in base_cols_q:
            df_base_q = df_base_q.drop(co, axis=1)  #remove the co in base if already exists

    if debug:
        df_base_q = df_base_q.query("industry=='家用电器'")

    new_value_df = pd.DataFrame({
        'code': df_base_q['code'],
        'score_sum': df_base_q['result_value_quarter_fundation'],
        'result_value_2': [0] * df_base_q.__len__(),
        'ps_perc': [0] * df_base_q.__len__(),
        'npr_perc': [0] * df_base_q.__len__(),
        'net_profits_perc': [0] * df_base_q.__len__(),
        'npr_mtp_profit': [0] * df_base_q.__len__(),  #npr*net_profit
        'npr_mtp_profit_perc': [0] * df_base_q.__len__(),  #npr*net_profit percent
        'peg_1_perc': [0] * df_base_q.__len__(),
        'peg_4_perc': [0] * df_base_q.__len__(),
    })

    df_base_q = pd.merge(new_value_df, df_base_q, on='code', how='outer', suffixes=('', '_df_base_q'))

    df_base_q.drop_duplicates(inplace=True)
    df_base_q.fillna(0, inplace=True)
    logging.info(("df_base_q len: " + str(df_base_q.__len__())))

    #update npr*net_profit
    for i in range(df_base_q.__len__()):
        tmp = df_base_q.iloc[i]['npr'] * df_base_q.iloc[i]['net_profits'] / 100
        df_base_q.iloc[i, df_base_q.columns.get_loc('npr_mtp_profit')] = int(tmp)

        peg_1 = df_base_q.iloc[i]['peg_1']
        peg_4 = df_base_q.iloc[i]['peg_4']

        if peg_1 < 0: df_base_q.iloc[i, df_base_q.columns.get_loc('peg_1')] = 0
        if peg_4 < 0: df_base_q.iloc[i, df_base_q.columns.get_loc('peg_4')] = 0

    #calc

    ps_weight = 0.01
    npr_weight = 0.0
    net_profits_weight = 0.0
    npr_mtp_profit_weight = 0.97
    peg_1_weight = 0.01
    peg_4_weight = 0.01

    ps_all = df_base_q['ps']
    npr_all = df_base_q['npr']
    net_profits_all = df_base_q['net_profits']
    npr_mtp_profit_all = df_base_q['npr_mtp_profit']
    peg_1_all = df_base_q['peg_1']
    peg_4_all = df_base_q['peg_4']

    for i in range(df_base_q.__len__()):
        code = df_base_q.iloc[i]['code']
        name = df_base_q.iloc[i]['name']

        if code is None:
            logging.info(__file__+" "+"code is None")

        ps = df_base_q.iloc[i]['ps']
        npr = df_base_q.iloc[i]['npr']
        net_profits = df_base_q.iloc[i]['net_profits']
        npr_mtp_profit = df_base_q.iloc[i]['npr_mtp_profit']
        peg_1 = df_base_q.iloc[i]['peg_1']
        peg_4 = df_base_q.iloc[i]['peg_4']

        ps_perc = 1 - stats.percentileofscore(ps_all, ps) / 100
        npr_perc = stats.percentileofscore(npr_all, npr) / 100
        net_profits_perc = stats.percentileofscore(net_profits_all, net_profits) / 100
        npr_mtp_profit_perc = stats.percentileofscore(npr_mtp_profit_all, npr_mtp_profit) / 100

        if peg_1 > 0:
            peg_1_perc = 1 - stats.percentileofscore(peg_1_all, peg_1) / 100
        else:
            peg_1_perc = 0

        if peg_4 > 0:
            peg_4_perc = 1 - stats.percentileofscore(peg_4_all, peg_4) / 100
        else:
            peg_4_perc = 0

        df_base_q.iloc[i, df_base_q.columns.get_loc('ps_perc')] = ps_perc
        df_base_q.iloc[i, df_base_q.columns.get_loc('npr_perc')] = npr_perc
        df_base_q.iloc[i, df_base_q.columns.get_loc('net_profits_perc')] = net_profits_perc
        df_base_q.iloc[i, df_base_q.columns.get_loc('npr_mtp_profit_perc')] = npr_mtp_profit_perc
        df_base_q.iloc[i, df_base_q.columns.get_loc('peg_1_perc')] = peg_1_perc
        df_base_q.iloc[i, df_base_q.columns.get_loc('peg_4_perc')] = peg_4_perc

        critiria_not_meet = False
        critiria = ''

        if (ps <= 0.0):
            critiria_not_meet = True
            critiria += ' ps'

        if (npr <= 0.0):
            critiria_not_meet = True
            critiria += ' npr'

        if (net_profits <= 0.0):
            critiria_not_meet = True
            critiria += ' net_profits'

        if (peg_1 <= 0.0) and (peg_4 <= 0.0):
            critiria_not_meet = True
            critiria += ' peg_1 and peg_4'

        if critiria_not_meet:
            result_value = 0
            df_base_q.iloc[i, df_base_q.columns.get_loc('result_value_2')] = result_value
            logging.info(str(code) + " " + str(name) + " " + critiria + " indicator not meet the minimal critiria, mark result_value_2 score to 0 ")
            continue


        result_value = ps_perc * ps_weight + \
                       npr_perc * npr_weight + \
                       net_profits_perc * net_profits_weight + \
                       npr_mtp_profit_perc * npr_mtp_profit_weight + \
                       peg_1_perc * peg_1_weight + \
                       peg_4_perc * peg_4_weight

        logging.info(__file__+" "+"code " + code + " result " + str(result_value))

        if not np.isnan(result_value):
            df_base_q.iloc[i, df_base_q.columns.get_loc('result_value_2')] = result_value
            df_base_q.iloc[i, df_base_q.columns.get_loc('score_sum')] += result_value

    df_base_q.sort_values('result_value_2', ascending=False, inplace=True)
    #df_q_r = df_q_r.DataFrame.reset_index().drop('index', axis=1) #RYAN:BUG?
    df_base_q = df_base_q.reset_index().drop('index', axis=1)

    #if debug:
    #    df_base_q.to_csv("~/DATA/result/debug.csv", encoding='UTF-8', index=True)
    #    logging.info(__file__+" "+"DEBUG Run #2 Fundamental Analysising Based on Quarter Report, result saved to ~/DATA/result/debug.csv")
    #else:
    #    df_base_q.to_csv(dump_csv_q, encoding='UTF-8', index=True)
    #    logging.info(__file__+" "+"Run #2 Fundamental Analysising Based on Quarter Report, result saved to "+dump_csv_q)
    df_base_q.to_csv(dump_csv_q, encoding='UTF-8', index=True)
    logging.info(__file__ + ": " + "Run #2 Fundamental Analysising Based on Quarter Report, result saved to " + dump_csv_q + " , len " + str(df_base_q.__len__()))


#this function need be run after _quaterly_fundamental_any_2


def industry_rank(quarterly_fundamental_csv=None):
    csv_industry_top = "/home/ryan/DATA/result/industry_top.csv"

    df_result = pd.DataFrame()

    if pd.isnull(quarterly_fundamental_csv):
        quarterly_fundamental_csv = dump_csv_q

    #logging.info(__file__+" "+"reading " + quarterly_fundamental_csv)
    df = pd.read_csv(quarterly_fundamental_csv, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + quarterly_fundamental_csv)

    industries = df['industry'].unique()

    for industry in industries:
        df_a_ind = df[df['industry'] == industry]
        #df_a_ind = df_a_ind.sort_values('result_value_quarter_fundation')
        df_a_ind = df_a_ind.sort_values('score_sum', ascending=False).reset_index().drop('index', axis=1)
        df_a_ind = pd.DataFrame([df_a_ind.__len__()] * df_a_ind.__len__(), columns=['peers']).join(df_a_ind)

        logging.info(str(industry) + " len " + str(df_a_ind.__len__()))
        #max_row = df_a_ind.loc[df_a_ind['result_value_quarter_fundation'].idxmax()].copy()
        #max_row.set_value('peers', df_a_ind.__len__())
        #df_result = df_result.append(max_row)
        df_result = df_result.append(df_a_ind.head(3))  #top 3 player in the industry

    cols = [
        'code',
        'name',
        'industry',
        'peers',
        'score_sum',
        'result_value_2',
        'result_value_quarter_fundation',
        'esp_ratio_perc',
        'esp_ratio',
        'esp_perc',
        'esp',
        'pe_perc',
        'pe',
        'roe_perc',
        'roe',
        'pb_perc',
        'pb',
        'totalAssets_perc',
        'totalAssets',
        'eps',
        'eps_yoy',
        'epsg',
        'area',
        'outstanding',
        'totals',
        'liquidAssets',
        'fixedAssets',
        'reserved',
        'reservedPerShare',
        'bvps',
        'timeToMarket',
        'undp',
        'perundp',
        'rev',
        'profit',
        'gpr',
        'npr',
        'holders',
        #'undp',              'rev', 'total_profit', 'gpr', 'npr', 'holders',
        'epcf',
        'net_profits',
        'profits_yoy',
        'net_profit_ratio',
        'gross_profit_rate',
        'business_income',
        'bips',
        'arturnover',
        'arturndays',
        'inventory_turnover',
        'inventory_days',
        'currentasset_turnover',
        'currentasset_days',
        'mbrg',
        'nprg',
        'nav',
        'targ',
        'seg',
        'currentratio',
        'quickratio',
        'cashratio',
        'icratio',
        'sheqratio',
        'adratio',
        'cf_sales',
        'rateofreturn',
        'cf_nm',
        'cf_liabilities',
        'cashflowratio'
    ]

    df_result = df_result[cols]
    #df_result = df_result.sort_values('result_value_quarter_fundation', ascending=False, inplace=False)
    df_result = df_result.sort_values('score_sum', ascending=False, inplace=False)
    df_result = df_result.reset_index().drop('index', axis=1)
    df_result.to_csv(csv_industry_top, encoding='UTF-8', index=True)

    logging.info(__file__ + ": " + "industry topest saved to " + csv_industry_top + " , len " + str(df_result.__len__()))
    return (df_result)


def area_rank(quarterly_fundamental_csv=None):
    csv_area_top = "/home/ryan/DATA/result/area_top.csv"

    df_result = pd.DataFrame()

    if pd.isnull(quarterly_fundamental_csv):
        quarterly_fundamental_csv = dump_csv_q

    #logging.info(__file__+" "+"reading "+quarterly_fundamental_csv)
    df = pd.read_csv(quarterly_fundamental_csv, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + quarterly_fundamental_csv)

    areas = df['area'].unique()

    for area in areas:
        df_a_ind = df[df['area'] == area]
        #df_a_ind = df_a_ind.sort_values('result_value_quarter_fundation' )
        df_a_ind = df_a_ind.sort_values('score_sum', ascending=False).reset_index().drop('index', axis=1)
        df_a_ind = pd.DataFrame([df_a_ind.__len__()] * df_a_ind.__len__(), columns=['peers']).join(df_a_ind)

        #
        logging.info(area + " len " + str(df_a_ind.__len__()))
        #max_row = df_a_ind.loc[df_a_ind['result_value_quarter_fundation'].idxmax()].copy()
        #max_row.set_value('peers', df_a_ind.__len__())
        #df_result = df_result.append(max_row)
        df_result = df_result.append(df_a_ind.head(3))  # top 3 player in the industry

    cols = [
        'code',
        'name',
        'area',
        'peers',
        'score_sum',
        'result_value_2',
        'result_value_quarter_fundation',
        'industry',
        'esp_ratio_perc',
        'esp_ratio',
        'esp_perc',
        'esp',
        'pe_perc',
        'pe',
        'roe_perc',
        'roe',
        'pb_perc',
        'pb',
        'totalAssets_perc',
        'totalAssets',
        'eps',
        'eps_yoy',
        'epsg',
        'outstanding',
        'totals',
        'liquidAssets',
        'fixedAssets',
        'reserved',
        'reservedPerShare',
        'bvps',
        'timeToMarket',
        'undp',
        'perundp',
        'rev',
        'profit',
        'gpr',
        'npr',
        'holders',
        #'undp',             'rev', 'total_profit', 'gpr', 'npr', 'holders',
        'epcf',
        'net_profits',
        'profits_yoy',
        'net_profit_ratio',
        'gross_profit_rate',
        'business_income',
        'bips',
        'arturnover',
        'arturndays',
        'inventory_turnover',
        'inventory_days',
        'currentasset_turnover',
        'currentasset_days',
        'mbrg',
        'nprg',
        'nav',
        'targ',
        'seg',
        'currentratio',
        'quickratio',
        'cashratio',
        'icratio',
        'sheqratio',
        'adratio',
        'cf_sales',
        'rateofreturn',
        'cf_nm',
        'cf_liabilities',
        'cashflowratio'
    ]

    df_result = df_result[cols]
    #df_result = df_result.sort_values('result_value_quarter_fundation', ascending=False, inplace=False)
    df_result = df_result.sort_values('score_sum', ascending=False, inplace=False)
    df_result = df_result.reset_index().drop('index', axis=1)
    df_result.to_csv(csv_area_top, encoding='UTF-8', index=True)

    logging.info(__file__ + ": " + "area topest saved to " + csv_area_top + " , len " + str(df_result.__len__()))
    return (df_result)


def today_fundamental_any(todayS=None):  # no longer work. tushare0

    today_fund_csv = ""
    dump_today = ""
    #if pd.isnull(todayS):
    if True:
        for i in range(5):
            todayS = (datetime.datetime.today() - datetime.timedelta(i)).strftime('%Y-%m-%d')
            logging.info(__file__+" "+"checking input file required on date " + todayS)

            today_fund_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_" + todayS + ".csv"
            dump_today = "/home/ryan/DATA/pickle/daily_update_source/" + todayS + "ts_ud.pickle"

            if os.path.exists(dump_today):
                logging.info(__file__+" "+"dump_today exists on " + todayS)

                if os.path.exists(today_fund_csv):
                    logging.info(__file__+" "+"based on availble dump_doay and today_fund_csv, todayS now is " + todayS)
                    break

    logging.info(__file__+" "+"Fundamental Analysising based on today data " + todayS)

    #dump_today = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_" + todayS + ".pickle"  #### ONLY THIS IS TMP
    #today_fund_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/fundamentals_" + todayS + ".csv"
    #dump_today = "/home/ryan/DATA/pickle/daily_update_source/"+todayS+"ts_ud.pickle"

    if not os.path.isfile(today_fund_csv):
        logging.info(__file__+" "+"csv_today file not exist, please run python t_daily_fundamentals.py  --fetch_data_all;. Abort. " + today_fund_csv)
        exit()

    if not os.path.isfile(dump_today):
        logging.info(__file__+" "+"dump file not exist, please run t_daily_update_csv_from_tushare.py. Abort." + dump_today)
        exit()

    df_basic = today_all = pd.DataFrame()

    #get basic
    df_basic = pandas.read_csv(today_fund_csv, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + today_fund_csv)

    today_all = pandas.read_pickle(dump_today)

    df_q_r = pd.read_csv(dump_csv_q, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + dump_csv_q)

    #RYAN: CALCULATE TODAYS DATA

    fvalues_weight = 0.2
    turnover_weight = 0.4
    amount_weight = 0.4

    #result_value_quarter_fundation_weight=0.99

    field_today_all = [
        'code',  #代码
        'name',  #名称
        'changepercent',  #涨跌幅
        'trade',  #现价
        'open',  #开盘价
        'high',  #最高价
        'low',  #最低价
        'settlement',  #昨日收盘价
        'volume',  #成交量
        'turnoverratio',  #换手率
        'amount',  #成交量
        'per',  #市盈率
        'pb',  #市净率
        'mktcap',  #总市值
        'nmc',  #流通市值
    ]

    today_all = finlib.Finlib().remove_market_from_tscode(today_all)
    today_all = pd.merge(today_all, df_basic, on='code', suffixes=('', '_df_basic'))


    #today_all.drop('pe_df_basic',  axis=1, inplace=True)
    #today_all.drop('name_df_basic',  axis=1, inplace=True)
    #today_all.drop('area_df_basic',  axis=1, inplace=True)
    #today_all.drop('totals_df_basic',  axis=1, inplace=True)
    #today_all.drop('industry_df_basic',  axis=1, inplace=True)

    #move cared column to head
    today_all = pd.merge(today_all, df_q_r, on='code', suffixes=('', '_df_q_r'))
    today_all.drop('name_df_q_r', axis=1, inplace=True)
    today_all.drop('esp_df_q_r', axis=1, inplace=True)
    today_all.drop('pe_df_q_r', axis=1, inplace=True)
    today_all.drop('pb_df_q_r', axis=1, inplace=True)
    today_all.drop('area_df_q_r', axis=1, inplace=True)
    today_all.drop('outstanding_df_q_r', axis=1, inplace=True)
    today_all.drop('totals_df_q_r', axis=1, inplace=True)
    today_all.drop('totalAssets_df_q_r', axis=1, inplace=True)
    today_all.drop('liquidAssets_df_q_r', axis=1, inplace=True)
    today_all.drop('fixedAssets_df_q_r', axis=1, inplace=True)
    today_all.drop('reserved_df_q_r', axis=1, inplace=True)
    today_all.drop('reservedPerShare_df_q_r', axis=1, inplace=True)
    today_all.drop('bvps_df_q_r', axis=1, inplace=True)
    today_all.drop('timeToMarket_df_q_r', axis=1, inplace=True)
    today_all.drop('undp_df_q_r', axis=1, inplace=True)
    #today_all.drop('perundp_df_q_r', axis=1, inplace=True)
    today_all.drop('rev_df_q_r', axis=1, inplace=True)
    today_all.drop('profit_df_q_r', axis=1, inplace=True)
    today_all.drop('gpr_df_q_r', axis=1, inplace=True)
    today_all.drop('npr_df_q_r', axis=1, inplace=True)
    today_all.drop('holders_df_q_r', axis=1, inplace=True)

    #for i in today_all.columns:
    #    logging.info(i)
    #exit(0)

    tmp = today_all['cashflowratio']
    today_all.drop('cashflowratio', axis=1, inplace=True)
    today_all.insert(0, 'cashflowratio', tmp)
    tmp = today_all['rateofreturn']
    today_all.drop('rateofreturn', axis=1, inplace=True)
    today_all.insert(0, 'rateofreturn', tmp)
    tmp = today_all['targ']
    today_all.drop('targ', axis=1, inplace=True)
    today_all.insert(0, 'targ', tmp)
    tmp = today_all['nav']
    today_all.drop('nav', axis=1, inplace=True)
    today_all.insert(0, 'nav', tmp)
    tmp = today_all['currentasset_turnover']
    today_all.drop('currentasset_turnover', axis=1, inplace=True)
    today_all.insert(0, 'currentasset_turnover', tmp)
    tmp = today_all['bips']
    today_all.drop('bips', axis=1, inplace=True)
    today_all.insert(0, 'bips', tmp)
    tmp = today_all['business_income']
    today_all.drop('business_income', axis=1, inplace=True)
    today_all.insert(0, 'business_income', tmp)
    tmp = today_all['net_profits']
    today_all.drop('net_profits', axis=1, inplace=True)
    today_all.insert(0, 'net_profits', tmp)
    tmp = today_all['holders']
    today_all.drop('holders', axis=1, inplace=True)
    today_all.insert(0, 'holders', tmp)
    tmp = today_all['npr']
    today_all.drop('npr', axis=1, inplace=True)
    today_all.insert(0, 'npr', tmp)
    tmp = today_all['gpr']
    today_all.drop('gpr', axis=1, inplace=True)
    today_all.insert(0, 'gpr', tmp)
    tmp = today_all['profit']
    today_all.drop('profit', axis=1, inplace=True)
    today_all.insert(0, 'profit', tmp)
    #tmp=today_all['total_profit']; today_all.drop('total_profit',  axis=1, inplace=True); today_all.insert(0,'total_profit',tmp)
    #tmp=today_all['perundp']; today_all.drop('perundp',  axis=1, inplace=True); today_all.insert(0,'perundp',tmp)
    tmp = today_all['bvps']
    today_all.drop('bvps', axis=1, inplace=True)
    today_all.insert(0, 'bvps', tmp)
    tmp = today_all['esp']
    today_all.drop('esp', axis=1, inplace=True)
    today_all.insert(0, 'esp', tmp)
    tmp = today_all['amount']
    today_all.drop('amount', axis=1, inplace=True)
    today_all.insert(0, 'amount', tmp)
    #    tmp=today_all['turnover']; today_all.drop('turnover',  axis=1, inplace=True); today_all.insert(0,'turnover',tmp)
    #    tmp=today_all['fvalues']; today_all.drop('fvalues',  axis=1, inplace=True); today_all.insert(0,'fvalues',tmp)
    tmp = today_all['pe']
    today_all.drop('pe', axis=1, inplace=True)
    today_all.insert(0, 'pe', tmp)
    tmp = today_all['name']
    today_all.drop('name', axis=1, inplace=True)
    today_all.insert(0, 'name', tmp)
    tmp = today_all['code']
    today_all.drop('code', axis=1, inplace=True)
    today_all.insert(0, 'code', tmp)

    #move result_value_quarter_fundation to head
    #today_all.drop('result_value_quarter_fundation',  axis=1, inplace=True)
    #today_all.insert(0,'result_value_quarter_fundation',result_value_quarter_fundation_all)
    tmp = today_all['result_value_quarter_fundation']
    today_all.drop('result_value_quarter_fundation', axis=1, inplace=True)
    today_all.insert(0, 'result_value_quarter_fundation', tmp)

    pe_all = today_all['pe']
    #    fvalues_all=today_all['fvalues']
    #    turnover_all=today_all['turnover']
    amount_all = today_all['amount']
    result_value_quarter_fundation_all = today_all['result_value_quarter_fundation']

    new_value_df = pd.DataFrame([0] * today_all.__len__(), columns=['result_value_today'])
    today_all = new_value_df.join(today_all)  #the inserted colun on the head

    new_value_df = pd.DataFrame([0] * today_all.__len__(), columns=['result_value_sum'])
    today_all = new_value_df.join(today_all)

    for i in range(0, today_all.__len__()):
        code = today_all.iloc[i]['code']
        #logging.info(__file__+" "+"daily analyse on " +code)

        pe = today_all.iloc[i]['pe']
        #        fvalues = today_all.iloc[i]['fvalues']
        #        turnover = today_all.iloc[i]['turnover']
        amount = today_all.iloc[i]['amount']
        result_value_quarter_fundation = today_all.iloc[i]['result_value_quarter_fundation']

        pe_perc = 1 - (stats.percentileofscore(pe_all, pe) / 100.0)
        #       fvalues_perc=1 - (stats.percentileofscore(fvalues_all, fvalues)/100.0)
        #       turnover_perc=stats.percentileofscore(turnover_all, turnover)/100.0
        amount_perc = stats.percentileofscore(amount_all, amount) / 100.0
        result_value_quarter_fundation_perc = stats.percentileofscore(result_value_quarter_fundation_all, result_value_quarter_fundation) / 100.0

        #RYAN: TODAY's VALUE CALCULATED HERE
        #result_value = fvalues_perc*fvalues_weight + \
        #               turnover_perc*turnover_weight +\
        #               amount_perc*amount_weight

        result_value = amount_perc * amount_weight

        # RYAN debug here.
        #result_value = result_value_quarter_fundation_perc*result_value_quarter_fundation_weight

        #if ((pe == 0) or (fvalues == 0)  or (turnover == 0) or (amount == 0)):
        if ((pe == 0) or (amount == 0)):
            result_value = 0
        today_all.iloc[i, today_all.columns.get_loc('result_value_today')] = result_value
        today_all.iloc[i, today_all.columns.get_loc('result_value_sum')] = result_value * 0.5 + today_all.iloc[i]['result_value_quarter_fundation'] * 0.5

        #df.set_value(i,'result_value', result_value)
        #df.loc[i]['result_value'] = result_value
        #df.iloc[i]['rsult_value'] = result_value

    #new_value_df = pd.DataFrame(new_value_array,columns=['result_value'])

    #for col in df.columns:
    #    df[col] = df[col].str.decode('utf8').str.encode('utf8')

    #df.to_excel(dump_xlsx, encoding='UTF-8')
    today_all.sort_values('result_value_sum', ascending=False, inplace=True)
    today_all.to_csv(dump_csv_d, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "Fundamental Analysising based on Today data, result saved to " + dump_csv_d + " , len " + str(today_all.__len__()))

    if os.path.lexists(dump_csv_d_sym_link):
        os.unlink(dump_csv_d_sym_link)

    os.symlink(dump_csv_d, dump_csv_d_sym_link)
    logging.info(__file__ + ": " + "symbol link created  " + dump_csv_d_sym_link + " -> " + dump_csv_d)


def peg_last_year(year, quarter, debug=False):
    year_quarter = str(year) + "_" + str(quarter)
    output_csv = "/home/ryan/DATA/result/fundamental_peg_" + year_quarter + ".csv"
    output_csv_2 = "/home/ryan/DATA/result/fundamental_peg_" + year_quarter + "_selected.csv"

    #fund_peg_csv = "/home/ryan/DATA/result/fundamental_peg.csv"

    fund_peg_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/peg/"
    fund_peg_csv += str(year) + finlib.Finlib().get_quarter_date(quarter) + ".csv"

    if not os.path.isfile(fund_peg_csv):
        logging.info(__file__+" "+"not found source peg file to get eps " + fund_peg_csv)
        exit(1)
    df_base = pd.read_csv(fund_peg_csv, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + fund_peg_csv)

    #query_1 = "year_quarter == '" + year_quarter + "'"
    query_2 = "roe > 15 and ((peg_1 > 0.0 and peg_1 < 0.5) and (peg_4 > 0.0 and peg_4 < 0.5))"
    #df_result_1 = df_base.query(query_1).sort_values(by='peg_1', ascending=True)
    df_result_2 = df_base.query(query_2).sort_values(by='peg_1', ascending=True)

    # df_result_1 = finlib.Finlib().remove_df_columns(df_result_1, "name_.*")
    # df_result_1 = finlib.Finlib().change_df_columns_order(df_result_1, ['code', 'name', 'year_quarter', 'roe', 'peg_1', 'peg_4'])
    # df_result_1 = df_result_1.drop_duplicates()
    # df_result_1.code = df_result_1.code.astype(str)

    df_result_2 = finlib.Finlib().remove_df_columns(df_result_2, "name_.*")
    df_result_2 = finlib.Finlib().change_df_columns_order(df_result_2, ['code', 'name',  'end_date', 'roe', 'peg_1', 'peg_4'])
    df_result_2 = df_result_2.drop_duplicates()
    df_result_2.code = df_result_2.code.astype(str)

    # df_result_1.to_csv(output_csv, encoding='UTF-8', index=False)
    df_result_2.to_csv(output_csv_2, encoding='UTF-8', index=False)
    # logging.info(__file__ + ": " + year_quarter + " fundmental peg result saved to " + output_csv + " , len " + str(df_result_1.__len__()))
    logging.info(__file__ + ": fundmental peg Selectd result saved to " + output_csv_2 + " , len " + str(df_result_2.__len__()))

    sl_1 = "/home/ryan/DATA/result/latest_fundamental_peg.csv"
    if os.path.lexists(sl_1):
        os.unlink(sl_1)
    os.symlink(fund_peg_csv, sl_1)
    logging.info(__file__+" "+"make symbol link " + sl_1 + " --> " + output_csv)

    sl_2 = "/home/ryan/DATA/result/latest_fundamental_peg_selected.csv"
    if os.path.lexists(sl_2):
        os.unlink(sl_2)
    os.symlink(output_csv_2, sl_2)
    logging.info(__file__+" "+"make symbol link " + sl_2 + " --> " + output_csv_2)

    return ()


def calc_peg(debug=False):
    a = finlib.Finlib().get_year_month_quarter()
    b = finlib.Finlib().get_report_publish_status()

    #for p in a['full_period_list_yearly']:
    for p in a['full_period_list']:
        logging.info(__file__+" "+"=== calc_peg" + p + "=== ")

        if debug:
            p = "20190630"

        regex = re.match("(\d{4})(\d{2})(\d{2})", p)
        year_this = regex.group(1)
        month_this = regex.group(2)


        t = finlib.Finlib().get_year_month_quarter(year=int(year_this), month=int(month_this))

        t_1q = t['ann_date_1q_before']
        t_4q = t['ann_date_4q_before']

        #ryan debug
        #p =finlib.Finlib().get_report_publish_status()['completed_year_rpt_date'] #20191231
        #t_1q = '20181231'
        #t_4q = '20171231'


        #input and output file, peg column will be inserted to the file inplacely.
        merged_fund_f = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + p + ".csv"
        merged_fund_f_4q = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + t_4q + ".csv"
        merged_fund_f_1q = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + t_1q + ".csv"


        if (not force_run_global) \
                and os.path.exists(merged_fund_f) \
                and p < a['fetch_most_recent_report_perid'][0]:
            logging.info(__file__+" "+"ignore calc peg for period " + p)
            continue

        if not os.path.exists(merged_fund_f):
            print("target file to be updated does not exist " + merged_fund_f)
            continue
        #exit(0)

        if not os.path.exists(merged_fund_f_4q):
            print("skip, need -4Q file to calcuate peg " + merged_fund_f_4q)
            continue

        if not os.path.exists(merged_fund_f_1q):
            print("skip, need -1Q file to calcuate peg " + merged_fund_f_1q)
            continue

        df = pd.read_csv(merged_fund_f )
        df = finlib.Finlib().ts_code_to_code(df)
        df =df[['code','name','end_date','eps','roe']]
        logging.info(__file__ + ": " + "loading " + merged_fund_f)

        df_4q = pd.read_csv(merged_fund_f_4q )
        df_4q = finlib.Finlib().ts_code_to_code(df_4q)
        df_4q = df_4q[['code', 'name', 'end_date', 'eps','roe']]
        logging.info(__file__ + ": " + "loading " + merged_fund_f_4q)

        df_1q = pd.read_csv(merged_fund_f_1q)
        df_1q = finlib.Finlib().ts_code_to_code(df_1q)
        df_1q = df_1q[['code', 'name', 'end_date', 'eps','roe']]
        logging.info(__file__ + ": " + "loading " + merged_fund_f_1q)


        #get latest pe
        todayS_l = finlib.Finlib().get_last_trading_day()
        basic_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_"+todayS_l+".csv"
        basic_df = pd.read_csv(basic_csv)
        basic_df = finlib.Finlib().ts_code_to_code(basic_df)
        basic_df = basic_df[['code','pe']]
        logging.info(__file__ + ": " + "loading " + basic_csv)


        #get latest price
        todayS_l = finlib.Finlib().get_last_trading_day()
        regex = re.match("(\d{4})(\d{2})(\d{2})", todayS_l)

        todayS_l = regex.group(1)+"-"+regex.group(2)+"-"+regex.group(3)
        dump = "/home/ryan/DATA/pickle/daily_update_source/" + todayS_l + "ts_ud.pickle"
        df_today_price = pandas.read_pickle(dump)
        df_today_price = finlib.Finlib().ts_code_to_code(df_today_price)


        df_rst = pd.merge(df, df_1q, on='code', how='inner', suffixes=('', '_1q')).drop('name_1q', axis=1)
        df_rst = pd.merge(df_rst, df_4q, on='code', how='inner', suffixes=('', '_4q')).drop('name_4q', axis=1)
        df_rst = pd.merge(df_rst, df_today_price, on='code', how='inner', suffixes=('', '_price'))
        df_rst = pd.merge(df_rst, basic_df, on='code', how='inner', suffixes=('', '_basic'))

        # df = df.fillna(0)
        # df_4q = df_4q.fillna(0)
        # df_1q = df_1q.fillna(0)

        # egr:  earnings growth rate = 100 * ((EPS_this / EPS_last) - 1)  (=20% e.g.)
        # peg:  Price/egr
        # if not "egr_4" in df.columns:
        #     df = pd.DataFrame([0] * df.__len__(), columns=['egr_4']).join(df)
        #
        # if not "egr_1" in df.columns:
        #     df = pd.DataFrame([0] * df.__len__(), columns=['egr_1']).join(df)
        #
        # if not "peg_4" in df.columns:
        #     df = pd.DataFrame([0] * df.__len__(), columns=['peg_4']).join(df)
        #
        # if not "peg_1" in df.columns:
        #     df = pd.DataFrame([0] * df.__len__(), columns=['peg_1']).join(df)

        # #update line by line
        # if debug:
        #     df = df[df['code'] == '600519']

        df_rst['egr_4'] = 100 * (df_rst['eps'] / df_rst['eps_4q'] - 1)
        df_rst['egr_1'] = 100 * (df_rst['eps'] / df_rst['eps_1q'] - 1)
        df_rst['peg_4'] = df_rst['pe'] / df_rst['egr_4']
        df_rst['peg_1'] = df_rst['pe'] / df_rst['egr_1']

        # dflen = df.__len__()
        # for i in range(dflen):
        #     code = str(df.iloc[i]['code'])
        #     year_quarter = str(df.iloc[i]['end_date'])
        #     name = str(df.iloc[i]['name'])
        #
        #     logging.info(str(i) + " of " + str(dflen) + " " + code + " " + name + " " + year_quarter)
        #
        #     eps_this = df.iloc[i]['eps']
        #     eps_last_4 = 0
        #     eps_last_1 = 0
        #     egr_4 = 0
        #     egr_1 = 0
        #     #close_this = df.iloc[i]['close']
        #     close_this = finlib.Finlib().get_price(code_m=code)
        #
        #     if float(eps_this) <= 0.0:
        #         logging.info(__file__+" "+"eps_this <= 0")
        #         continue
        #
        #     ## getting 4 quarter and 1q  before data for this code
        #     df_code_4q = df_4q[df_4q.code == code]
        #     df_code_1q = df_1q[df_1q.code == code]
        #
        #     if df_code_4q.empty:
        #         logging.info(__file__+" "+"no code for 4q" + code)
        #         continue
        #
        #     if df_code_1q.empty:
        #         logging.info(__file__+" "+"no data for 1q" + code)
        #         continue
        #
        #     eps_last_4 = df_code_4q['eps'].values[0]
        #     eps_last_1 = df_code_1q['eps'].values[0]
        #
        #     if float(eps_last_4) <= 0.0:
        #         logging.info(__file__+" "+"negative eps of eps_last_4")
        #         continue
        #
        #     if float(eps_last_1) <= 0.0:
        #         logging.info(__file__+" "+"negative eps of eps_last_1")
        #         continue
        #
        #     # earning growth rate (in percent)


            #egr_4 = 100 * ((eps_this / eps_last_4) - 1)
            #df.iloc[i, df.columns.get_loc('egr_4')] = round(egr_4, 2)

            # peg
            #peg_4 = close_this / egr_4
            #df.iloc[i, df.columns.get_loc('peg_4')] = round(peg_4, 2)
            #
            # # earning growth rate (in percent)
            # egr_1 = 100 * ((eps_this / eps_last_1) - 1)
            # df.iloc[i, df.columns.get_loc('egr_1')] = round(egr_1, 2)
            #
            # # peg
            # peg_1 = close_this / egr_1
            # df.iloc[i, df.columns.get_loc('peg_1')] = round(peg_1, 2)
            #
            # logging.info(__file__+" "+"egr_1 " + str(egr_1) + ", egr_4 " + str(egr_4))
            # logging.info(__file__+" "+"peg_1 " + str(peg_1) + ", peg_4 " + str(peg_4))

        # pass  #all codes in the year_quarter csv file has been updated. e.g /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_20181231.csv

        df_rst = finlib.Finlib().change_df_columns_order(df=df_rst, col_list_to_head=['code', 'name', 'end_date', 'peg_1', 'peg_4', 'egr_1', 'egr_4','trade_date','close'])
        b_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/peg"

        if not os.path.isdir(b_dir):
            os.mkdir(b_dir)

        out_f = b_dir+"/"+p+".csv"
        df_rst.to_csv(out_f, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "fundmental peg result saved to " + out_f + " , len " + str(df_rst.__len__()))

    pass


#update the csv dump_csv_q with inserting ps, peg columns. 'ps','peg_1','peg_4','egr_1','egr_4'
def calc_ps(debug=False):
    # fund_ps_csv = "/home/ryan/DATA/result/latest_fundamental_peg.csv"  #3505 lines
    # fund_ps_csv = "/home/ryan/DATA/result/fundamental_peg.csv"  #24573 lines

    #fund_ps_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_20181231.csv"

    fund_ps_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_"
    fund_ps_csv += str(year) + finlib.Finlib().get_quarter_date(quarter) + ".csv"

    if not os.path.isfile(dump_csv_d):
        logging.info(__file__+" "+"not found source file to get price to save (PS), aera, field etc. " + dump_csv_d)
        exit(1)

    if not os.path.isfile(dump_csv_q):
        logging.info(__file__+" "+"not found source file to get price to save (PS), aera, field etc. " + dump_csv_q)
        exit(1)

    if not os.path.isfile(fund_ps_csv):
        logging.info(__file__+" "+"not found source file to get price to save (PS), ps  " + fund_ps_csv)
        exit(1)

    #logging.info(__file__+" "+"loading "+dump_csv_d)
    df_base_d = pd.read_csv(dump_csv_d, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + dump_csv_d)

    #logging.info(__file__+" "+"loading " + dump_csv_q)
    df_base_q = pd.read_csv(dump_csv_q, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + dump_csv_q)

    #logging.info(__file__+" "+"loading " + fund_ps_csv)
    df_ps = pd.read_csv(fund_ps_csv, converters={'code': str})
    logging.info(__file__ + ": " + "loading " + fund_ps_csv)

    df_ps = df_ps[df_ps['year_quarter'] == str(year) + "_" + str(quarter)]

    cols_position = ['year_quarter', 'code', 'name', 'ps', 'pe', 'industry', 'area', 'npr', 'net_profits', 'peg_1', 'peg_4', 'egr_1', 'egr_4', 'roe', 'activity', 'avgturnover', 'strength', 'rateofreturn']

    cols_ps = ['ps', 'peg_1', 'peg_4', 'egr_1', 'egr_4', 'year_quarter']

    base_cols_d = df_base_d.columns.tolist()
    base_cols_q = df_base_q.columns.tolist()
    logging.info(__file__+" "+"preprocessing df_base_d and df_base_q")
    for co in ['ps', 'peg_1', 'peg_4', 'egr_1', 'egr_4']:
        if co in base_cols_d:
            df_base_d = df_base_d.drop(co, axis=1)  #remove the co in base if already exists

        if co in base_cols_q:
            df_base_q = df_base_q.drop(co, axis=1)

    cols_ps.insert(0, 'code')
    df_ps = df_ps[cols_ps]

    if debug:  #ryan debug
        df_base_d = df_base_d.query("code=='000651'").reset_index().drop('index', axis=1)
        df_base_q = df_base_q.query("code=='000651'").reset_index().drop('index', axis=1)
        df_ps = df_ps.query("code=='000651'").reset_index().drop('index', axis=1)


# df_result_d = pd.merge(df_base_d, df_ps, on='code', suffixes=('', '_df_ps'))
    df_result_d = pd.merge(df_base_d, df_ps, on=['code', 'year_quarter'], suffixes=('', '_df_ps'))
    # df_result_q = pd.merge(df_base_q, df_ps, on='code', suffixes=('', '_df_ps'))
    df_result_q = pd.merge(df_base_q, df_ps, on=['code', 'year_quarter'], suffixes=('', '_df_ps'))

    if 'level_0' in df_result_d.columns.tolist():
        df_result_d = df_result_d.drop('level_0', axis=1)
    if 'level_0' in df_result_q.columns.tolist():
        df_result_q = df_result_q.drop('level_0', axis=1)

    cols_result_d = df_result_d.columns.tolist()
    cols_result_q = df_result_q.columns.tolist()

    i = 0
    loop_len = cols_position.__len__()
    for co in cols_position:
        logging.info(__file__+" "+"remove d " + str(i) + " of " + str(loop_len))
        #logging.info('remove '+co)
        if co in cols_result_d:
            cols_result_d.remove(co)
            cols_result_d.insert(i, co)  #move the col position in cols
            i += 1

    i = 0
    for co in cols_position:
        logging.info(__file__+" "+"remove q " + str(i) + " of " + str(loop_len))
        #logging.info('remove '+co)
        if co in cols_result_q:
            cols_result_q.remove(co)
            cols_result_q.insert(i, co)
            i += 1

    df_result_d = df_result_d[cols_result_d]
    df_result_q = df_result_q[cols_result_q]

    df_result_d.to_csv(dump_csv_d, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "fundmental ps, peg inserted into " + dump_csv_d + " , len " + str(df_result_d.__len__()))

    df_result_q.to_csv(dump_csv_q, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "fundmental ps, peg inserted into " + dump_csv_q + " , len " + str(df_result_q.__len__()))
    return ()


def main():
    ########################
    #
    #########################

    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    the_latest_report_date = finlib.Finlib().get_report_publish_status()
    year = the_latest_report_date['completed_quarter_year']  # int 2018
    quarter = the_latest_report_date['completed_quarter_number']  # int 4

    #year = re.match("(\d{4})\d{4}", finlib.Finlib().get_year_month_quarter()['stable_report_perid']).group(1)
    #quarter = 4

    print("head year is " + str(year))

    print("main year is " + str(year))
    #exit()

    parser = OptionParser()

    parser.add_option("-f", "--fetch_data_all", action="store_true", dest="fetch_all_f", default=False, help="fetch all the quarterly fundatation history data")

    parser.add_option("-p", "--process_hist_data", action="store_true", dest="process_hist_data", default=False, help="fill the missing data with previous data")

    parser.add_option("-q", "--exam_quarterly", action="store_true", dest="exam_quarterly", default=False, help="exam quarterly fundational score")

    parser.add_option("-d", "--exam_daily", action="store_true", dest="exam_daily", default=False, help="exam today fundational score")

    parser.add_option("-a", "--area_rank_f", action="store_true", dest="area_rank_f", default=False, help="area_rank based on quarterly fundational score")

    parser.add_option("-i", "--industry_rank_f", action="store_true", dest="industry_rank_f", default=False, help="industry_rank based on quarterly fundational score")

    parser.add_option("-g", "--calc_peg", action="store_true", dest="calc_peg", default=False, help="calc PEG Ration. ")

    parser.add_option("-t", "--this_year_quarter", action="store_true", dest="this_year_quarter", default=False, help="filter out this year quarterly peg ")

    parser.add_option("-b", "--calc_ps", action="store_true", dest="calc_ps", default=False, help="calc price to sale (ps) data. ")

    parser.add_option("-c", "--calc_fund_2", action="store_true", dest="calc_fund_2", default=False, help="calc fund 2nd time ")

    parser.add_option("-e", "--debug", action="store_true", dest="debug", default=False, help="use debug, use instrument_A.csv.debug when -calc_preg only calc")

    parser.add_option("-m", "--update_get_market", action="store_true", dest="update_get_market_flag", default=False, help="update /home/ryan/DATA/pickle/market.csv")

    parser.add_option("-n", "--update_get_instrument", action="store_true", dest="update_get_instrument_flag", default=False, help="update /home/ryan/DATA/pickle/instrument.csv")

    parser.add_option("-s", "--update_get_security", action="store_true", dest="update_get_security_flag", default=False, help="update /home/ryan/DATA/pickle/security.csv")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all_f
    process_hist_data = options.process_hist_data
    exam_quarterly = options.exam_quarterly
    exam_daily = options.exam_daily
    industry_rank_f = options.industry_rank_f
    area_rank_f = options.area_rank_f
    calc_peg_f = options.calc_peg
    debug = options.debug
    this_year_quarter = options.this_year_quarter
    calc_ps_f = options.calc_ps
    calc_fund_2 = options.calc_fund_2
    update_get_market = options.update_get_market_flag
    update_get_security = options.update_get_security_flag
    update_get_instrument = options.update_get_instrument_flag
    force_run_f = options.force_run_f

    logging.info(__file__+" "+"fetch_all_f: " + str(fetch_all_f))
    logging.info(__file__+" "+"process_hist_data: " + str(process_hist_data))
    logging.info(__file__+" "+"exam_quarterly: " + str(exam_quarterly))
    logging.info(__file__+" "+"exam_daily: " + str(exam_daily))
    logging.info(__file__+" "+"industry_rank: " + str(industry_rank_f))
    logging.info(__file__+" "+"area_rank: " + str(area_rank_f))
    logging.info(__file__+" "+"calc_peg: " + str(calc_peg_f))
    logging.info(__file__+" "+"calc_ps: " + str(calc_ps_f))
    logging.info(__file__+" "+"this_year_quarter: " + str(this_year_quarter))
    logging.info(__file__+" "+"calc_fund_2: " + str(calc_fund_2))
    logging.info(__file__+" "+"update_get_market: " + str(update_get_market))
    logging.info(__file__+" "+"update_get_security: " + str(update_get_security))
    logging.info(__file__+" "+"update_get_instrument: " + str(update_get_instrument))
    logging.info(__file__+" "+"debug: " + str(debug))
    logging.info(__file__+" "+"force_run_f: " + str(force_run_f))

    global force_run_global
    force_run_global = False
    if force_run_f:
        force_run_global = True

#    year = 2018
#    quarter = 2

    if fetch_all_f:
        fetch_all()
    elif process_hist_data:
        _combine_all(year_end=year, quarter_end=quarter, debug=debug)
        refine_hist_data(renew=True, debug=debug)
    elif exam_quarterly:
        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
            print("not processing fundermental data at this month. ")
            #exit()

        df_basic = finlib.Finlib().get_today_stock_basic()

        all_hist = refine_hist_data(df=None, renew=False, debug=debug)
        df_other_six = all_hist[all_hist['year_quarter'] == str(year) + "_" + str(quarter)]

        #csv_jaqs_fund = "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"
        #df_jaqs_fund= pandas.read_csv(csv_jaqs_fund, converters={'code': str})

        #quarterly_fundamental_any(df_basic, df_other_six, df_jaqs_fund, year_quarter=str(year)+"_"+str(quarter), debug=debug)
        quarterly_fundamental_any(df_basic, year_quarter=str(year) + "_" + str(quarter), debug=debug)
    elif exam_daily:
        add_miss = False
        #add_miss=True # debug, use when debug a sepcial day.

        if add_miss:
            todayS = '2018-08-23'
        else:
            # todayS = datetime.today().strftime('%Y-%m-%d')

            # get_day_all(today) will be 404 when the script running at 3.00 PM. so using Day-1 data for reference.
            # otherwise need adjust the running cron later, say 5.00 PM etc.

            # last traiding day will be say 404 on Monday (-1 = Sunday)
            todayS = finlib.Finlib().get_last_trading_day()

            # yesterday = datetime.datetime.strptime(todayS, '%Y-%m-%d') - datetime.timedelta(1)
            # todayS = yesterday.strftime('%Y-%m-%d')

        today_fundamental_any(todayS=todayS)

    elif area_rank_f:
        area_rank()
    elif industry_rank_f:
        industry_rank()
    elif calc_peg_f:
        calc_peg(debug=debug)
    elif calc_ps_f:
        calc_ps(debug=debug)
    elif calc_fund_2:
        _quarterly_fundamental_any_2(debug=debug)
    elif this_year_quarter:
        #stable_report_perid = finlib.Finlib().get_year_month_quarter()['stable_report_perid'] #20171231
        b = finlib.Finlib().get_report_publish_status()  #20171231

        year = b['completed_quarter_year']
        quarter = b['completed_quarter_number']

        #quarter = 4
        peg_last_year(year=year, quarter=quarter)
    elif update_get_market:
        #updating "/home/ryan/DATA/pickle/market.csv")
        finlib.Finlib().get_market(force_update=True)
    elif update_get_security:
        #updating ("/home/ryan/DATA/pickle/security.csv")
        finlib.Finlib().get_security(force_update=True)

    elif update_get_instrument:
        #updating ("/home/ryan/DATA/pickle/instrument.csv")
        finlib.Finlib().get_instrument(force_update=True)


    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
