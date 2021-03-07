# coding: utf-8

import tushare as ts
import tushare.util.conns as ts_cs
import tushare.stock.trading as ts_stock_trading
import finlib_indicator
import talib
import pickle
import os
import os.path
import pandas as pd
import time
import numpy as np
import tabulate
import akshare as ak

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

import logging
import yaml
logging.basicConfig(filename='/home/ryan/del.log', filemode='a', format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
import warnings
import constant

from collections import deque
from io import StringIO

# warnings.filterwarnings("error")
warnings.filterwarnings("default")

# 2018.01.31  15:24, removed a lot DEL_ functions and committed to the git.


class NumpyMySQLConverter(mysql.connector.conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """
    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)


class Account:
    def __init__(self):
        self.initBalance = 10000
        self.stock_code
        self.stock_count
        self.balance


class Finlib:
    def load_all_jaqs(self):
        logging.info(__file__+" "+"load df basic requires lots of memory, > 1G will be consumed.")
        csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv"  # file 1.1G, lots of memory to loading >5G
        csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly.csv"  #

        if not os.path.exists(csv):
            logging.info(__file__+" "+"file not exists " + csv)
            return ()

        logging.info(__file__+" "+"reading " + csv)

        # df = pd.read_csv(csv, converters={i: str for i in range(20)})
        df = pd.read_csv(csv, converters={'ts_code': str, 'trade_date': str})
        return (df)

    '''
    def zzz_load_all_jaqs(self,debug=False, overwrite=False):
        #return() #ryan debug
        logging.info(__file__+" "+"consolidate jaqs to a df requires lots of memory, > 2G will be consumed.")


        output_csv = "/home/ryan/DATA/result/jaqs/jaqs_all.csv"
        output_pickle = "/home/ryan/DATA/result/jaqs/jaqs_all.pickle"

        path = "/home/ryan/DATA/DAY_JAQS"

        if debug:
            path = path+".dev"
            output_csv = "/home/ryan/DATA/result/jaqs.dev/jaqs_all.csv"

        if not os.path.isdir(path):
            logging.info('path not exist '+path)
            exit()

        #if self.is_cached(output_csv,day=7) and (not overwrite):
            #logging.info(__file__+" "+"load jaqs all from "+output_csv)
            #df_all_jaqs = pd.read_csv(output_csv)

        if self.is_cached(output_pickle, day=7) and (not overwrite):
            logging.info(__file__+" "+"load jaqs all from " + output_pickle)
            df_all_jaqs = pd.read_pickle(output_pickle)
            return(df_all_jaqs)

        allFiles = glob.glob(path + "/*.csv")

        logging.info(__file__+" "+"load_all_jaqs, reading files, 2G memory will be consumed, be paticent...")
        df_all_jaqs = pd.concat((pd.read_csv(f, converters={'code':str, 'trade_date':str}) for f in allFiles),sort=False)
        logging.info(__file__+" "+"generate df_all_jaqs which concatted from "+path+"/*.csv has done.")

        #logging.info(__file__+" "+"saving df_all_jaqs , "+output_csv)
        #df_all_jaqs.to_csv(output_csv, encoding='UTF-8', index=False)

        logging.info(__file__+" "+"saving df_all_jaqs , " + output_pickle)
        df_all_jaqs.to_pickle(output_pickle)

        return(df_all_jaqs)
    '''
    def load_all_ts_pro(self, debug=False, overwrite=False):
        # return ()  # ryan debug

        output_pickle = "/home/ryan/DATA/result/ts_all.pickle"

        logging.info(__file__+" "+"consolidate ts_pro to a df requires lots of memory, > 500M will be consumed.")

        path = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged"

        if debug:
            path = path + ".dev"

        if self.is_cached(output_pickle, day=7) and (not overwrite):
            logging.info(__file__+" "+"load tushare pro all from " + output_pickle)
            df_all_ts_pro = pd.read_pickle(output_pickle)
            return (df_all_ts_pro)

        if not os.path.isdir(path):
            os.mkdir(path)

        #allFiles = glob.glob(path + "/*.csv")
        allFiles = glob.glob(path + "/*201[5-9]*.csv")
        allFiles.extend(glob.glob(path + "/*202[0-9]*.csv"))
        logging.info(__file__+" "+"load_all_ts_pro, reading files, 500M memory will be consumed, be paticent...")

        df_all_ts_pro = pd.DataFrame()

        for f in allFiles:
            logging.info(__file__+" "+"reading " + f)
            df_tmp = pd.read_csv(f, converters={'end_date': str, 'audit_agency': str, 'audit_result': str, 'audit_sign': str})
            df_all_ts_pro = pd.concat([df_all_ts_pro, df_tmp], sort=False)

        # df_all_ts_pro = pd.concat((pd.read_csv(f, converters={'end_date':str}) for f in allFiles), sort=False) #faster but no debug ablity
        logging.info(__file__+" "+"generate df_all_ts_pro which concatted from " + path + "/*.csv has done.")

        logging.info(__file__+" "+"saving df_all_ts_pro to " + output_pickle)
        df_all_ts_pro.to_pickle(output_pickle)

        return (df_all_ts_pro)

    def is_non_zero_file(self, fpath):
        rnt = False

        if os.path.isfile(fpath):
            if os.path.getsize(fpath) > 0:
                rnt = True

        return rnt

    def measureValue(self, fenzi, fenmu):
        # the abs value bigger, the result bigger.
        # fenzi more bigger than fenmu, result is bigger.
        # fenzi more bigger, result is bigger.

        # np.log(np.e ** 3)  # 3.0
        # np.log2(2 ** 3)  # 3.0
        # np.log10(10 ** 3)  # 3.0

        abs_fenzi = abs(fenzi)
        abs_fenmu = abs(fenmu)
        abs_fenzi_min_fenmu = abs(fenzi - fenmu)

        if fenmu == 0.0:
            return 0

        # logging.info(__file__+" "+"log2 "+str(abs_fenzi - fenmu))
        # logging.info(__file__+" "+"log2 "+str((abs_fenzi / fenmu) + 1))

        rst = np.log10(abs_fenzi) * \
              np.log2(abs_fenzi_min_fenmu) * \
              np.log2((abs_fenzi * 1.0 / abs_fenmu) + 1)

        if fenzi < 0 or (fenzi - fenmu) < 0:
            rst = -1 * rst

        return rst

    def get_common_fund_df(self): #no longer work. tushare1.
        return(pd.read_csv("/home/ryan/DATA/result/fund_analysis.csv", converters={'code': str}))

    def generate_common_fund_df(self):
        dir = "/home/ryan/DATA/result"
        if not os.path.isdir(dir):
            os.mkdir(dir)

        to_csv = dir + "/fund_analysis.csv"


        df_exam_all = pd.DataFrame()


        # get pe
        # ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv
        # close: 		当日收盘价
        # turnover_rate: 		换手率（%）
        # turnover_rate_f: 		换手率（自由流通股）
        # dv_ratio: 股息率 （%）
        # total_mv: 总市值 （万元）
        # circ_mv: 流通市值（万元）

        df_basic = self.get_today_stock_basic()

        # get ROE, market cap

        rp = self.get_year_month_quarter()['stable_report_perid']
        rp_1 = self.get_year_month_quarter()['ann_date_2y_before']

        df_merge = self.regular_read_csv_to_stdard_df(
            data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + rp + ".csv")
        df_merge_1 = self.regular_read_csv_to_stdard_df(
            data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + rp_1 + ".csv")
        df_merge_sub = df_merge[['name',
                                 'code',
                                 'roe',  # 净资产收益率
                                 'roa',  # 总资产报酬率

                                 'total_profit',  # 利润总额
                                 'net_profit',  # 净利润
                                 'free_cashflow',  # 企业自由现金流量

                                 'total_revenue',  # 营业总收入
                                 'total_assets',  # 资产总计
                                 'total_liab',  # 负债合计

                                 'ebit',  # 息税前利润
                                 'ebitda',  # 息税折旧摊销前利润
                                 'netdebt',  # 净债务
                                 'fcff',  # 企业自由现金流量
                                 'fcfe',  # 股权自由现金流量


                                 ]]

        df_merge_sub_1 = df_merge_1[['name',
                                     'code',
                                     'roe',  # 净资产收益率
                                     'roa',  # 总资产报酬率

                                     'total_profit',  # 利润总额
                                     'net_profit',  # 净利润
                                     'free_cashflow',  # 企业自由现金流量

                                     'total_revenue',  # 营业总收入
                                     'total_assets',  # 资产总计
                                     'total_liab',  # 负债合计

                                     'ebit',  # 息税前利润
                                     'ebitda',  # 息税折旧摊销前利润
                                     'netdebt',  # 净债务
                                     'fcff',  # 企业自由现金流量
                                     'fcfe',  # 股权自由现金流量

                                     ]]

        df_exam_all = pd.merge(df_basic, df_merge_sub, on=['code'], how='inner', suffixes=('', '_x'))
        df_exam_all = pd.merge(df_exam_all, df_merge_sub_1, on=['code'], how='inner', suffixes=('', '_year1'))

        #600519 netdebt: -2.2e+09
        #df_exam_all['ev'] = df_exam_all['total_mv']+df_exam_all['netdebt']-df_exam_all['fcff']-df_exam_all['fcfe']
        df_exam_all['ev'] = df_exam_all['total_mv']*1e4+df_exam_all['netdebt']
        df_exam_all['ev_ebitda_ratio'] = df_exam_all['ev']/df_exam_all['ebitda']
        df_exam_all['ev_ebitda_ratio_rank'] =  df_exam_all['ev_ebitda_ratio'].rank(pct=True)
        logging.info(df_exam_all.sort_values(by='ev_ebitda_ratio_rank').head(5)[['code','name','ev_ebitda_ratio','ev_ebitda_ratio_rank']])

        #market cap/Net profit after tax
        df_exam_all['total_mv_net_profit_ratio'] = df_exam_all['total_mv']*1e4 / df_exam_all['net_profit']
        df_exam_all['total_mv_net_profit_ratio_rank'] =  df_exam_all['total_mv_net_profit_ratio'].rank(pct=True)  #
        logging.info(df_exam_all.sort_values(by='total_mv_net_profit_ratio_rank').head(5)[['code','name','total_mv_net_profit_ratio']])

        self.print_mt(df_exam_all)

        df_exam_all.to_csv(to_csv, encoding='UTF-8', index=False)
        logging.info("fund analysis result saved to "+to_csv+" , len "+str(df_exam_all.__len__()))

        return()


    #print maotai's data in format in annual statement.
    def print_mt(self,df):
        test_stock = df[df['code'] == 'SH600519'].iloc[0]
        for k in test_stock.keys():
            v = test_stock[k]
            if type(v) == int or type(v) == float:
                v =  f"{v:,}"
            logging.info( k+" --> " +str(v))


    def fetch_today_stock_basic_fund1(self,date_exam_day=None):
        if date_exam_day == None:
            date_exam_day = self.get_last_trading_day()

        csv_basic = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_" + date_exam_day + ".csv"  # get_stock_basics每天都会更新一次

        if not self.is_cached(csv_basic):
            logging.info(__file__ + " " + "Getting Basic, ts.get_stock_basics of " + date_exam_day)  # 获取沪深上市公司基本情况
            df_basic = ts.get_stock_basics()
            df_basic = df_basic.reset_index()
            df_basic.code = df_basic.code.astype(str)  # convert the code from numpy.int to string.
            df_basic.reset_index().to_csv(csv_basic, encoding='UTF-8', index=False)
            logging.info(__file__+" "+"Saved tushare.org daily fund to "+csv_basic+" , len "+str(df_basic.__len__()))



    #get ts stock basic. updated using tspro only.
    def get_today_stock_basic(self,date_exam_day=None):
        if date_exam_day == None:
            date_exam_day = self.get_last_trading_day()

        dir = "/home/ryan/DATA/result/basic_summary"
        csv = dir + "/basic_fund_fund2_" + date_exam_day + ".csv"

        df = pd.read_csv(csv, converters={'code': str})
        return(df)


    #merge df daily basic of ts and tspro
    def generate_today_fund1_fund2_stock_basic(self,date_exam_day=None):
        dir = "/home/ryan/DATA/result/basic_summary"
        if not os.path.isdir(dir):
            os.mkdir(dir)

        if date_exam_day == None:
            date_exam_day = self.get_last_trading_day()

        to_csv = dir + "/basic_fund_fund2_" + date_exam_day + ".csv"

        #df_daily_basic_1 = self.add_market_to_code(self.regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_" + date_exam_day + ".csv" )) #no longer work. tushare1.

        df_daily_info = self.regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/info_daily/info_" + date_exam_day + ".csv" )
        df_daily_info = self.ts_code_to_code(df_daily_info)
        df_pro_basic = self.regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/pro_basic.csv")
        df_pro_basic['on_market_days'] = df_pro_basic['list_date'].apply(lambda _d: (datetime.today() - datetime.strptime(str(_d), '%Y%m%d')).days)

        df_rtn = pd.merge(df_pro_basic, df_daily_info, on=['code'], how='left', suffixes=('', '_x')).reset_index().drop('index', axis=1)


       # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_20200820.csv
       # ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv
        df_daily_stocks_basic = self.regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_" + date_exam_day + ".csv")
        df_daily_stocks_basic['volume_ratio_perc_rank'] = df_daily_stocks_basic['volume_ratio'].rank(pct=True)  # 量比
        df_daily_stocks_basic['total_mv_perc_rank'] = df_daily_stocks_basic['total_mv'].rank(pct=True)  # 总市值 （万元）
        df_daily_stocks_basic['circ_mv_perc_rank'] = df_daily_stocks_basic['circ_mv'].rank(pct=True)  # 流通市值（万元）
        df_daily_stocks_basic['pe_perc_rank'] = df_daily_stocks_basic['pe'].rank(pct=True)  # 市盈率（总市值/净利润， 亏损的PE为空）
        df_daily_stocks_basic['pe_ttm_perc_rank'] = df_daily_stocks_basic['pe_ttm'].rank(pct=True)  # 市盈率（TTM，亏损的PE为空）
        df_daily_stocks_basic['ps_ttm_perc_rank'] = df_daily_stocks_basic['ps_ttm'].rank(pct=True)  # 市销率（TTM）
        df_daily_stocks_basic['turnover_rate_f_perc_rank'] = df_daily_stocks_basic['turnover_rate_f'].rank(pct=True)  # 换手率（自由流通股）
       # df_daily_stocks_basic['dv_ttm_perc_rank'] = df_daily_stocks_basic['dv_ttm'].rank(pct=True) #股息率（TTM）（%）

        df_rtn = pd.merge(df_rtn, df_daily_stocks_basic, on=['code'], how='left', suffixes=('', '_x')).reset_index().drop('index', axis=1)

        df_rtn = self.add_ts_code_to_column(df_rtn)
        df_rtn = df_rtn.reset_index().drop('index', axis=1)
        df_rtn.to_csv(to_csv, encoding='UTF-8', index=False)

        logging.info("df today basic generated, len "+str(df_rtn.__len__())+" , saved to "+to_csv)
        return

    def add_ts_code_to_column(self,df,code_col='code'):
        if 'ts_code' in df.columns:
            return(df)

        if df.__len__() == 0:
            return(df)

        df_tmp = df.copy(deep=True)

        code_fmt = self.get_code_format(df_tmp['code'].iloc[0])['format']

        if code_fmt == 'C2D6':
            df_a = self.remove_market_from_tscode(df_tmp)
            df_b = self.add_market_to_code(df_a, dot_f=True, tspro_format=True)
            df['ts_code'] = df_b['ts_code']
        elif code_fmt == 'D6':
            df_b = self.add_market_to_code(df, dot_f=True, tspro_format=True)
            df['ts_code'] = df_b['ts_code']
        else:
            logging.error("unknow code format "+str(df_tmp['code'].iloc[0]))

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1, inplace=False)

        df = self.adjust_column(df,['code','ts_code'])

        return(df)


    def df_filter(self, df):
        # code in df: the code must in format 'SH600xxx' etc

        df_basic = self.get_today_stock_basic()
        df_basic = df_basic[df_basic['on_market_days'] > 30]
        logging.info(__file__+" "+"after filter(timetomarket>30), df len " + str(df.__len__()))

        df = self.remove_garbage_macd_ma(df)
        logging.info(__file__+" "+"after remove macd ma garbage, df len " + str(df.__len__()))
        return df

    def get_year_month_quarter(self, year=None, month=None):
        dict_rtn = {}
        if (year == None) and (month == None):
            # logging.info('getting year and month of today')
            year = int(datetime.today().strftime('%Y'))
            month = int(datetime.today().strftime('%m'))
            # only return this field for today query
            # dict_rtn['report_publish_status'] = self.get_report_publish_status()

        tmp = self._get_quarter(month)
        quarter = tmp['quarter']
        ann_date = tmp['ann_date']
        ann_date = str(year) + ann_date  # 20180331, 20180630, 20180930, 20181231

        # get full period list
        full_period_list = []
        full_period_list_yearly = []

        if month > 3:
            full_period_list.append(str(year) + "0331")

        if month > 6:
            full_period_list.append(str(year) + "0630")

        if month > 9:
            full_period_list.append(str(year) + "0930")

        i = year
        while i >= 2017:
            i = i - 1
            full_period_list.append(str(i) + "0331")
            full_period_list.append(str(i) + "0630")
            full_period_list.append(str(i) + "0930")

            #Only count last year 1231 after March next year.
            if month <= 3 and  (year - i ==1):
                logging.info("ingore last year 1231 as current month less than 3. current month "+str(month))
            else:
                full_period_list_yearly.append(str(i) + "1231")
                full_period_list.append(str(i) + "1231")

        full_period_list.sort(reverse=True) #most recent year in head.
        full_period_list_yearly.sort(reverse=True)

        # get most recent report date
        m = month
        fetch_most_recent_report_perid = []

        # the peirod that all stock have report
        stable_report_perid = str(year - 1) + "1231"

        if m == 1 or m == 2 or m == 3:
            fetch_most_recent_report_perid.append(str(year - 1) + "1231")
            stable_report_perid = str(year - 2) + "1231"
        elif m == 4 or m == 5 or m == 6:
            fetch_most_recent_report_perid.append(str(year - 1) + "1231")
            fetch_most_recent_report_perid.append(str(year) + "0331")
        #elif m == 6:
        #    fetch_most_recent_report_perid.append(str(year) + "0331")
        #    pass
        elif m == 7 or m == 8 or m == 9:
            fetch_most_recent_report_perid.append(str(year) + "0630")
        #elif m == 9:
        #    fetch_most_recent_report_perid.append(str(year) + "0630")
        #    pass
        elif m == 10 or m == 11 or m == 12:
            # 第三季报在十月份
            fetch_most_recent_report_perid.append(str(year) + "0630")
            fetch_most_recent_report_perid.append(str(year) + "0930")
            pass

        # get previous 1Q ann_date
        day_1q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95)
        day_2q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 2)
        day_3q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 3)
        day_4q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 4)
        day_5q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 5)
        day_6q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 6)
        day_7q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 7)
        day_8q_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(95 * 8)

        day_1y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 1)
        day_2y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 2)
        day_3y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 3)
        day_4y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 4)
        day_5y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 5)
        day_6y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 6)
        day_7y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 7)
        day_8y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 8)
        day_9y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 9)
        day_10y_before = datetime.strptime(ann_date, '%Y%m%d') - timedelta(366 * 10)

        # ann_date_1q_before
        ann_date_1q_before = str(day_1q_before.strftime('%Y')) + self._get_quarter(day_1q_before.strftime('%m'))['ann_date']
        ann_date_2q_before = str(day_2q_before.strftime('%Y')) + self._get_quarter(day_2q_before.strftime('%m'))['ann_date']
        ann_date_3q_before = str(day_3q_before.strftime('%Y')) + self._get_quarter(day_3q_before.strftime('%m'))['ann_date']
        ann_date_4q_before = str(day_4q_before.strftime('%Y')) + self._get_quarter(day_4q_before.strftime('%m'))['ann_date']
        ann_date_5q_before = str(day_5q_before.strftime('%Y')) + self._get_quarter(day_5q_before.strftime('%m'))['ann_date']
        ann_date_6q_before = str(day_6q_before.strftime('%Y')) + self._get_quarter(day_6q_before.strftime('%m'))['ann_date']
        ann_date_7q_before = str(day_7q_before.strftime('%Y')) + self._get_quarter(day_7q_before.strftime('%m'))['ann_date']
        ann_date_8q_before = str(day_8q_before.strftime('%Y')) + self._get_quarter(day_8q_before.strftime('%m'))['ann_date']

        dict_rtn = {"year": year, "month": month, "quarter": quarter, 'ann_date': ann_date}

        dict_rtn['full_period_list'] = full_period_list
        dict_rtn['stable_report_perid'] = stable_report_perid
        dict_rtn['full_period_list_yearly'] = full_period_list_yearly
        dict_rtn['fetch_most_recent_report_perid'] = fetch_most_recent_report_perid

        dict_rtn['ann_date_1q_before'] = ann_date_1q_before
        dict_rtn['ann_date_2q_before'] = ann_date_2q_before
        dict_rtn['ann_date_3q_before'] = ann_date_3q_before
        dict_rtn['ann_date_4q_before'] = ann_date_4q_before
        dict_rtn['ann_date_5q_before'] = ann_date_5q_before
        dict_rtn['ann_date_6q_before'] = ann_date_6q_before
        dict_rtn['ann_date_7q_before'] = ann_date_7q_before
        dict_rtn['ann_date_8q_before'] = ann_date_8q_before
        '''
        ann_date_1y_before=str(day_1y_before.strftime('%Y'))+self._get_quarter(day_1y_before.strftime('%m'))['ann_date']
        ann_date_2y_before=str(day_2y_before.strftime('%Y'))+self._get_quarter(day_2y_before.strftime('%m'))['ann_date']
        ann_date_3y_before=str(day_3y_before.strftime('%Y'))+self._get_quarter(day_3y_before.strftime('%m'))['ann_date']
        ann_date_4y_before=str(day_4y_before.strftime('%Y'))+self._get_quarter(day_4y_before.strftime('%m'))['ann_date']
        ann_date_5y_before=str(day_5y_before.strftime('%Y'))+self._get_quarter(day_5y_before.strftime('%m'))['ann_date']
        ann_date_6y_before=str(day_6y_before.strftime('%Y'))+self._get_quarter(day_6y_before.strftime('%m'))['ann_date']
        ann_date_7y_before=str(day_7y_before.strftime('%Y'))+self._get_quarter(day_7y_before.strftime('%m'))['ann_date']
        ann_date_8y_before=str(day_8y_before.strftime('%Y'))+self._get_quarter(day_8y_before.strftime('%m'))['ann_date']
        ann_date_9y_before=str(day_9y_before.strftime('%Y'))+self._get_quarter(day_9y_before.strftime('%m'))['ann_date']
        ann_date_10y_before=str(day_10y_before.strftime('%Y'))+self._get_quarter(day_10y_before.strftime('%m'))['ann_date']
        '''

        ann_date_1y_before = str(day_1y_before.strftime('%Y')) + "1231"
        ann_date_2y_before = str(day_2y_before.strftime('%Y')) + "1231"
        ann_date_3y_before = str(day_3y_before.strftime('%Y')) + "1231"
        ann_date_4y_before = str(day_4y_before.strftime('%Y')) + "1231"
        ann_date_5y_before = str(day_5y_before.strftime('%Y')) + "1231"
        ann_date_6y_before = str(day_6y_before.strftime('%Y')) + "1231"
        ann_date_7y_before = str(day_7y_before.strftime('%Y')) + "1231"
        ann_date_8y_before = str(day_8y_before.strftime('%Y')) + "1231"
        ann_date_9y_before = str(day_9y_before.strftime('%Y')) + "1231"
        ann_date_10y_before = str(day_10y_before.strftime('%Y')) + "1231"

        dict_rtn['ann_date_1y_before'] = ann_date_1y_before
        dict_rtn['ann_date_2y_before'] = ann_date_2y_before
        dict_rtn['ann_date_3y_before'] = ann_date_3y_before
        dict_rtn['ann_date_4y_before'] = ann_date_4y_before
        dict_rtn['ann_date_5y_before'] = ann_date_5y_before
        dict_rtn['ann_date_6y_before'] = ann_date_6y_before
        dict_rtn['ann_date_7y_before'] = ann_date_7y_before
        dict_rtn['ann_date_8y_before'] = ann_date_8y_before
        dict_rtn['ann_date_9y_before'] = ann_date_9y_before
        dict_rtn['ann_date_10y_before'] = ann_date_10y_before

        # get last quarter
        last_quarter = quarter - 1
        last_quarter_year = year

        if last_quarter == 0:
            last_quarter_year = year - 1
            last_quarter = 4

        dict_rtn['last_quarter'] = {'year': last_quarter_year, 'quarter': last_quarter}

        return (dict_rtn)

    def _get_quarter(self, month):

        month = int(month)
        ann_date = ''

        if month >= 1 and month < 4:
            quarter = 1
            ann_date = '0331'
        elif month >= 4 and month < 7:
            quarter = 2
            ann_date = '0630'
        elif month >= 7 and month < 10:
            quarter = 3
            ann_date = '0930'
        elif month >= 10 and month <= 12:
            quarter = 4
            ann_date = '1231'

        return ({'quarter': quarter, 'ann_date': ann_date})

    def get_quarter_date(self, quarter):

        quarter = str(quarter)

        mark_date = "0000"

        if quarter == "4":
            mark_date = "1231"
        elif quarter == "3":
            mark_date = "0930"
        elif quarter == "2":
            mark_date = "0630"
        elif quarter == "1":
            mark_date = "0331"
        else:
            logging.info(__file__+" "+"unknow quarter " + quarter)

        return (mark_date)

    def get_price(self, code_m, date=None):  # code_m: SH600519
        if date is not None:
            if re.match("\d{4}-\d{2}-\d{2}", date):
                date = re.sub("-", "", date)
                date = str(date)
            logging.info(__file__+" "+"change date to "+date)

        # price = 0
        price = 10**10  # change price to a huge number, so will never buy this.
        price_csv = "/home/ryan/DATA/DAY_Global/AG/" + code_m + ".csv"
        logging.info(__file__+" "+"getting price. "+str(code_m)+ "  date "+str(date)+" source "+price_csv)
        if os.path.isfile(price_csv):
            #pd_tmp = pd.read_csv(price_csv, converters={'code': str}, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
            pd_tmp = self.regular_read_csv_to_stdard_df(price_csv)

            if pd_tmp.__len__() == 0:
                logging.info(__file__+" "+"Fatal error, file is empty " + price_csv)
                # exit(1)
                return price

            if re.match(r".*\d{4}-\d{2}-\d{2}.*", pd_tmp['date'].iloc[-1]):
                logging.fatal(__file__+" "+"date format expect yyyymmdd but actually yyyy-mm-dd, should read the csv by finlib.regular_read_csv_to_stdard_df, quit now")
                exit(0)

            if (date is not None):
                df_the_day = pd_tmp[pd_tmp['date'] <= date]

                if df_the_day.__len__() > 0:
                    actual_date = df_the_day.iloc[-1:]['date'].values[0]
                    actual_price = df_the_day.iloc[-1:]['close'].values[0]  # '11.8231'

                    if actual_date != date:
                        logging.info(__file__+" "+"request "+code_m+" "+date+", return "+actual_date)
                        pass
                    price = actual_price
                else:
                    logging.info(__file__+" "+"no record of " + code_m + " " + date)
            else:
                price = pd_tmp['close'][-1:].values[0]

            price = float(price)
        else:
            logging.info('FETAL ERROR, cannot get price, no such file ' + price_csv)
            # exit(1)
        logging.info(__file__+"  "+"price returned "+str(price))
        return price

    def get_market(self, force_update=False):
        # xapi = ts_cs.xapi_x()
        con_succ = False
        try:
            xapi = ts_cs.xapi()
            con_succ = True
        except:
            logging.info(__file__+" "+"except when getting ts_cs.xapi()")

        if con_succ == False:
            try:
                xapi = ts_cs.xapi_x()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.xapi_x()")

        if con_succ == False:
            try:
                xapi = ts_cs.api()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.api()")
                logging.info(__file__+" "+"retrying exhaused")

        market_csv = "/home/ryan/DATA/pickle/market.csv"
        # if os.path.isfile(market_csv) and force_update:
        #    logging.info(__file__+" "+"deleting "+market_csv)
        #    os.remove(market_csv)

        if self.is_cached(market_csv, 3) and (not force_update):
            df_market = pd.read_csv(market_csv, converters={'code': str})
        else:
            logging.info(__file__+" "+"fetching market")
            df_market = ts_stock_trading.get_markets(xapi)
            df_market.to_csv(market_csv, encoding='UTF-8', index=False)  # len 48
            logging.info(__file__+" "+"market saved to " + market_csv)

        return df_market

    def get_security(self, force_update=False):  # return 7709 records
        # xapi = ts_cs.xapi_x()
        # api = ts_cs.api()
        con_succ = False
        try:
            api = ts_cs.api()  # no errors
            con_succ = True
        except:
            logging.info(__file__+" "+"except when getting ts_cs.xapi()")  # AttributeError: 'TdxExHq_API' object has no attribute 'get_security_list'

        if con_succ == False:
            try:
                api = ts_cs.xapi()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.xapi_x()")  # AttributeError: 'TdxExHq_API' object has no attribute 'get_security_list'

        if con_succ == False:
            try:
                api = ts_cs.xapi_x()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.api()")
                logging.info(__file__+" "+"retrying exhaused")

                # Stock
        security_csv = "/home/ryan/DATA/pickle/security.csv"

        # if os.path.isfile(security_csv) and force_update:
        #    logging.info(__file__+" "+"removing file "+security_csv)
        #    os.remove(security_csv)

        if self.is_cached(security_csv, 3) and (not force_update):
            df_security = pd.read_csv(security_csv, converters={'code': str})
        else:
            # df_security = ts.get_security(api) # NOT FOUND 6000xxx in the map
            logging.info(__file__+" "+"fetching security")
            df_security = ts_stock_trading.get_security(api)  # ryan: add 2018 04 21
            df_security.to_csv(security_csv, encoding='UTF-8', index=False)  # len 7644
            logging.info(__file__+" "+"security saved to " + security_csv)

        return df_security

    def get_instrument(self, force_update=False):  # return 47000+ records
        xapi = ts_cs.xapi_x()
        # xapi = ts_cs.xapi()
        con_succ = False
        try:
            xapi = ts_cs.xapi()
            con_succ = True
        except:
            logging.info(__file__+" "+"except when getting ts_cs.xapi()")

        if con_succ == False:
            try:
                xapi = ts_cs.xapi_x()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.xapi_x()")

        if con_succ == False:
            try:
                xapi = ts_cs.api()
                con_succ = True
            except:
                logging.info(__file__+" "+"except when getting ts_cs.api()")
                logging.info(__file__+" "+"retrying exhaused")

                # Qi Huo, HK Stock, US Stock
        instrument_csv = "/home/ryan/DATA/pickle/instrument.csv"
        # if os.path.isfile(instrument_csv) and force_update:
        #    logging.info(__file__+" "+"deleting "+instrument_csv)
        #    os.remove(instrument_csv)

        if (not force_update) and self.is_cached(instrument_csv, 1):
            df_instrument = pd.read_csv(instrument_csv, converters={'code': str})
        else:
            logging.info(__file__+" "+"fetching instrument")
            df_instrument = ts_stock_trading.get_instrument(xapi)
            df_instrument.to_csv(instrument_csv, encoding='UTF-8', index=False)  # len 7644
            logging.info(__file__+" "+"instrument saved to " + instrument_csv)

        return df_instrument

    def _DEL_get_jaqs_field(self, ts_code, date=None):  # date: YYYYMMDD, code:600519, read from ~/DATA/DAY_JAQS/SH600519.csv
        # date : None, then return the latest record.

        code_in_number_only = re.match("(\d{6})\.(.*)", ts_code).group(1)
        market = re.match("(\d{6})\.(.*)", ts_code).group(2)

        self.append_market_to_code_single_dot(code=code_in_number_only)  # '600519.SH'
        codeInFmtMktCode = self.add_market_to_code_single(code=code_in_number_only)  # 'SH600519'
        self.add_market_to_code(df=pd.DataFrame({'code': code_in_number_only}, index=[0]), dot_f=True, tspro_format=True)  # 0  600519.SH

        f = "/home/ryan/DATA/DAY_JAQS/" + codeInFmtMktCode + '.csv'
        if not os.path.exists(f):
            logging.info('file not exist ' + f)
            return

        df = pd.read_csv(f, converters={'code': str, 'trade_date': str})

        if date == None:
            df = df.tail(1)
        else:
            date_Y_M_D = self.get_last_trading_day(date)
            date = datetime.strptime(date_Y_M_D, '%Y-%m-%d').strftime('%Y%m%d')
            df = df[df['trade_date'] == date]

            if df.__len__() == 0:
                logging.info('code ' + ts_code + ' has no record at date ' + date + ". Use latest known date.")
                df = df.tail(1)
            elif df.__len__() > 0:
                df = df.head(1)  # if multiple records, only use the 1st one.

        dict_rtn = {}
        dict_rtn['pe'] = df['pe'].values[0]
        dict_rtn['pe_ttm'] = df['pe_ttm'].values[0]
        dict_rtn['pb'] = df['pb'].values[0]
        dict_rtn['ps'] = df['ps'].values[0]
        dict_rtn['all'] = df.reset_index().drop('index', axis=1)

        return (dict_rtn)

    def renew_jaqs_api(self):
        api = DataApi(addr='tcp://data.quantos.org:8910')
        api.login("13651887669", "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1NTE1Mzg0NTQyNjgiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM2NTE4ODc2NjkifQ.MT6sg03zcLJprsx4NjsCbNqfIX0aYfycTyLZ4BsTh3c")
        return api

    def get_A_stock_instrment(self, today_df=None, debug=False, code_name_only=True):  # return 3515 records

        df = pd.DataFrame()

        if debug:
            instrument_csv = "/home/ryan/DATA/pickle/instrument_A.csv.debug"
        else:
            instrument_csv = "/home/ryan/DATA/pickle/instrument_A.csv"

        if os.path.isfile(instrument_csv):
            df = pd.read_csv(instrument_csv, converters={'code': str})
        else:
            print("file not exist. " + instrument_csv)
            exit()

        if code_name_only:
            df = df[['name', 'code']]

        df = df.drop_duplicates()

        return df

    def append_market_to_code_single_dot(self, code):
        code_S = code
        if re.match('^6', code):
            code_S = code + ".SH"
        elif re.match('^[0|3]', code):
            code_S = code + ".SZ"
        elif re.match('^[9]', code):  # B Gu
            # logging.info(("ignore B GU " + code))
            pass
        elif re.match('SH', code):  #
            code_S = code
        elif re.match('SZ', code):  #
            code_S = code
        else:
            pass
            #logging.info(("Fatal: UNKNOWN CODE " + code))
        return code_S

    def add_market_to_code_single(self, code, dot_f=False, tspro_format=False):
        code_S = code

        dot = ''

        if dot_f == True:
            dot = '.'

        if re.match('^6', code):
            code_S = "SH" + code
            code_S2 = code + dot + 'SH'
        elif re.match('^[0|3]', code):
            code_S = "SZ" + code
            code_S2 = code + dot + 'SZ'
        elif re.match('^[9]', code):  # B Gu
            pass
            #logging.info(("ignore B GU " + code))
        elif re.match('^SH', code):  #
            code_number = re.match('^SH(.*)', code).group(1)
            code_S = code
            code_S2 = code_number + dot + 'SH'
        elif re.match('^SZ', code):  #
            code_number = re.match('^SZ(.*)', code).group(1)
            code_S = code
            code_S2 = code_number + dot + 'SZ'
        else:
            pass
            #logging.info(("Fatal: UNKNOWN CODE " + code))

        if tspro_format:
            return(code_S2)
        else:
            return(code_S)

    def add_market_to_code(self, df, dot_f=False, tspro_format=False):

        # tspro_format : 600000.SH
        df.code = df.code.astype(str)

        dot = ''

        if dot_f == True:
            dot = '.'

        # support the column name in df is 'code'
        for index, row in df.iterrows():
            # code = str(row['code'])
            code = row['code']
            # print(row)
            if re.match('^6', code):
                code_S = "SH" + dot + code
                code_S2 = code + dot + "SH"
            elif re.match('^[0|3]', code):
                code_S = "SZ" + dot + code
                code_S2 = code + dot + "SZ"
            elif re.match('^[9]', code):  # B Gu
                #logging.info(("ignore B GU " + code))
                continue
            elif re.match('^SH', code):  #
                code_S = code
                code_number = re.match('^SH(.*)', code).group(1)  # 600519
                code_S2 = code_number + dot + 'SH'
            elif re.match('^SZ', code):  #
                code_S = code
                code_number = re.match('^SZ(.*)', code).group(1)  # 600519
                code_S2 = code_number + dot + 'SZ'
            else:
                #logging.info(("Fatal: UNKNOWN CODE " + code))
                continue

            if tspro_format:
                df.at[index, 'ts_code'] = code_S2
            else:
                df.at[index, 'code'] = code_S

        return df

    def remove_market_from_tscode(self, df):
        # rename col name from ts_code to code
        # support the column name in df is 'ts_code'

        collist = df.columns.values

        for i in range(collist.__len__()):
            if collist[i] == 'ts_code':
                collist[i] = 'code'

        df.columns = collist  # apply the new columns name to df. rename columns

        for index, row in df.iterrows():
            code = row['code']
            code = code.replace(".", "")
            code = code.replace("SH", "")
            code = code.replace("SZ", "")
            df.at[index, 'code'] = code

        return df

    def remove_df_columns(self, df, col_name_regex):
        # col_name_regex example: "name_.*"
        new_cols_list = [i for i in list(df.columns) if not re.match(col_name_regex, i)]
        return (df[new_cols_list])

    def change_df_columns_order(self, df, col_list_to_head):
        # col_list_to_head example: ['code', 'name', 'year_quarter']
        new_cols_list = [i for i in list(df.columns) if not i in col_list_to_head]
        new_cols_list = col_list_to_head + new_cols_list
        return (df[new_cols_list])

    def load_ts_pro_basic_quarterly(self):
        df_result = pd.DataFrame()

        dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly"
        file_list = []
        file_list = glob.glob(dir + "/basic_*.csv")  # basic_20181231.csv
        for f in file_list:
            f = os.path.abspath(f)
            print("loading " + f)
            # df = pd.read_csv(f, converters={i: str for i in range(1000)} )
            df = pd.read_csv(f, converters={i: str for i in ['ts_code', 'trade_date']})
            df_result = df_result.append(df)

        return (df_result)

    def ts_code_to_code(self, df):
        # rename col name from ts_code  to code
        # change code format from 600000.SH to SH600000
        collist = df.columns.values
        logging.info("converting ts_code to code, lines to be converted "+str(df.__len__()))

        for i in range(collist.__len__()):
            if collist[i] == 'ts_code':
                collist[i] = 'code'

        df.columns = collist  # apply the new columns name to df. rename columns

        for index, row in df.iterrows():
            code = row['code']
            #print("code "+code)
            try:
                dcode = re.match('(\d{6})\.(.*)', code).group(1)  # group(1):600000,
                mkt = re.match('(\d{6})\.(.*)', code).group(2)  # group(2):
                df.at[index, 'code'] = mkt + dcode
            except:
                logging.warning("expection convert ts_code to code, ts_code "+code)


        return df

    # usage example
    # todayS = finlib.Finlib().get_last_trading_day(datetime.today().strftime('%Y-%m-%d'))
    # todayS = finlib.Finlib().get_last_trading_day(datetime.today().strftime('%Y%m%d'))
    #
    # todayS = datetime.strptime(todayS, '%Y-%m-%d').strftime('%Y%m%d') #last trading day. eg. 20181202-->20181130

    def get_last_trading_day(self, date=None, debug=True):

        if date is None:

            hour = datetime.today().hour

            # A market trading time, (0.00 to 15:00) new data not generated. so give yesterday's.
            if hour < 15:
                yesterday = datetime.today() - timedelta(1)
                todayS = yesterday.strftime('%Y%m%d')
                exam_date = todayS
                date = todayS
            else:  # (15.01 -- 23.59)
                todayS = datetime.today().strftime('%Y%m%d')
                exam_date = todayS
                date = todayS

        tmp = re.match("^(\d{4})(\d{2})(\d{2})$", date)

        if tmp:
            yyyy = tmp.group(1)
            mm = tmp.group(2)
            dd = tmp.group(3)
            date = yyyy + mm + dd

        exam_date = todayS = date

        this_year = datetime.today().strftime("%Y") #2020
        csv_f = "/home/ryan/DATA/pickle/trading_day_"+this_year+".csv"

        if not os.path.isfile(csv_f):
            a = self.get_ag_trading_day()
        else:
            a = pandas.read_csv(csv_f)

        b = a[a['cal_date'] == int(todayS)]

        if len(b) == 0:
            print("no record!!!")
            print("csv_f " + csv_f)
            print("todayS " + todayS)

        tdy_idx = a[a['cal_date'] == int(todayS)].index.values[0]

        if a.at[tdy_idx, "is_open"] == 0:
            if debug:
                logging.info(__file__+" "+"Today " + todayS + " is not a trading day, checking previous days")
            tdy_idx = a[a['cal_date'] == int(todayS)].index.values[0]
            for i in range(tdy_idx, 0, -1):
                if a.at[i, "is_open"] == 1:
                    exam_date = str(a.at[i, "cal_date"])
                    if debug:
                        logging.info(__file__+" "+"Day " + exam_date + " is a trading day.")
                    break

        return str(exam_date)

    def is_a_trading_day_ag(self, dateS):

        this_year = datetime.today().strftime("%Y") #2020
        csv_f = "/home/ryan/DATA/pickle/trading_day_"+this_year+".csv"
        a = pandas.read_csv(csv_f)

        tdy_idx = a[a['cal_date'] == int(dateS)].index.values[0]

        if a.at[tdy_idx, "is_open"] == 0:
            logging.info(__file__+" "+"Date " + dateS + " is not a trading day")
            rst = False

        else:
            rst = True

        return (rst)

    def get_last_trading_day_us(self, date=None):

        todayS = datetime.today().strftime('%Y-%m-%d')

        if date is None:

            hour = datetime.today().hour

            # A market trading time, (21.30 to 4:00) new data not generated. so give yesterday's.
            if hour < 4 or hour > 21:  # in markets
                last_trading_day_us = datetime.strptime(todayS, '%Y-%m-%d') - timedelta(2)
                last_trading_day_us = last_trading_day_us.strftime('%Y-%m-%d')
            else:  # (4.01 -- 23.59) not in market
                last_trading_day_us = datetime.strptime(todayS, '%Y-%m-%d') - timedelta(1)
                last_trading_day_us = last_trading_day_us.strftime('%Y-%m-%d')

        return last_trading_day_us

    def get_ag_trading_day(self):
        this_year = datetime.today().strftime("%Y")
        csvf = "/home/ryan/DATA/pickle/trading_day_" +this_year  + ".csv"
        df_trade_cal = ts.pro_api().trade_cal(exchange='SSE', start_date='19980101', end_date=this_year+'1231')
        df_trade_cal.to_csv(csvf, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "trade_cal saved to " + csvf + " , len " + str(df_trade_cal.__len__()))
        return df_trade_cal

    ### calculate Tecnical indicator for given df.
    # Moved from t_daily_pattern_Hit_Price_Volume.py
    # debug :  flag of debug
    # forex : flag of forex
    # df:  the dataframe has ohlcv
    #
    # Return:
    #
    def calc(self, max_exam_day, opt, df, df_52_week, outputF, outputF_today, exam_date, live_trading=False):
        try:
            debug = opt['debug']
            forex = opt['forex']
            bool_calc_std_mean = opt['bool_calc_std_mean']
            bool_perc_std_mean = opt['bool_perc_std_mean']
            bool_talib_pattern = opt['bool_talib_pattern']
            bool_pv_hit = opt['bool_pv_hit']
            bool_p_mfi_div = opt['bool_p_mfi_div']
            bool_p_rsi_div = opt['bool_p_rsi_div']
            bool_p_natr_div = opt['bool_p_natr_div']
            bool_p_tema_div = opt['bool_p_tema_div']
            bool_p_trima_div = opt['bool_p_trima_div']

            bool_p_adx_div = opt['bool_p_adx_div']
            bool_p_adxr_div = opt['bool_p_adxr_div']
            bool_p_apo_div = opt['bool_p_apo_div']
            bool_p_aroon_div = opt['bool_p_aroon_div']
            bool_p_aroonosc_div = opt['bool_p_aroonosc_div']
            bool_p_bop_div = opt['bool_p_bop_div']
            bool_p_cci_div = opt['bool_p_cci_div']
            bool_p_cmo_div = opt['bool_p_cmo_div']
            bool_p_dx_div = opt['bool_p_dx_div']
            bool_p_minusdi_div = opt['bool_p_minusdi_div']
            bool_p_minusdm_div = opt['bool_p_minusdm_div']
            bool_p_mom_div = opt['bool_p_mom_div']
            bool_p_plusdi_div = opt['bool_p_plusdi_div']
            bool_p_plusdm_div = opt['bool_p_plusdm_div']
            bool_p_ppo_div = opt['bool_p_ppo_div']
            bool_p_roc_div = opt['bool_p_roc_div']
            bool_p_rocp_div = opt['bool_p_rocp_div']
            bool_p_rocr_div = opt['bool_p_rocr_div']
            bool_p_rocr100_div = opt['bool_p_rocr100_div']
            bool_p_trix_div = opt['bool_p_trix_div']
            bool_p_ultosc_div = opt['bool_p_ultosc_div']
            bool_p_willr_div = opt['bool_p_willr_div']
            bool_p_macd_div = opt['bool_p_macd_div']
            bool_p_macdext_div = opt['bool_p_macdext_div']
            bool_p_macdfix_div = opt['bool_p_macdfix_div']

            bool_p_ad_div = opt['bool_p_ad_div']
            bool_p_adosc_div = opt['bool_p_adosc_div']
            bool_p_obv_div = opt['bool_p_obv_div']

            bool_p_avgprice_div = opt['bool_p_avgprice_div']
            bool_p_medprice_div = opt['bool_p_medprice_div']
            bool_p_typprice_div = opt['bool_p_typprice_div']
            bool_p_wclprice_div = opt['bool_p_wclprice_div']

            bool_p_htdcperiod_div = opt['bool_p_htdcperiod_div']
            bool_p_htdcphase_div = opt['bool_p_htdcphase_div']
            bool_p_htphasor_div = opt['bool_p_htphasor_div']
            bool_p_htsine_div = opt['bool_p_htsine_div']
            bool_p_httrendmode_div = opt['bool_p_httrendmode_div']

            bool_p_beta_div = opt['bool_p_beta_div']
            bool_p_correl_div = opt['bool_p_correl_div']
            bool_p_linearreg_div = opt['bool_p_linearreg_div']
            bool_p_linearregangle_div = opt['bool_p_linearregangle_div']
            bool_p_linearregintercept_div = opt['bool_p_linearregintercept_div']
            bool_p_linearregslope_div = opt['bool_p_linearregslope_div']
            bool_p_stddev_div = opt['bool_p_stddev_div']
            bool_p_tsf_div = opt['bool_p_tsf_div']
            bool_p_var_div = opt['bool_p_var_div']

            bool_p_wma_div = opt['bool_p_wma_div']
            bool_p_t3_div = opt['bool_p_t3_div']
            bool_p_sma_div = opt['bool_p_sma_div']
            bool_p_sarext_div = opt['bool_p_sarext_div']
            bool_p_sar_div = opt['bool_p_sar_div']
            bool_p_midprice_div = opt['bool_p_midprice_div']
            bool_p_midpoint_div = opt['bool_p_midpoint_div']
            # bool_p_mavp_div =opt['bool_p_mavp_div']
            bool_p_mama_div = opt['bool_p_mama_div']
            bool_p_ma_div = opt['bool_p_ma_div']
            bool_p_kama_div = opt['bool_p_kama_div']
            bool_p_httrendline_div = opt['bool_p_httrendline_div']
            bool_p_ema_div = opt['bool_p_ema_div']
            bool_p_dema_div = opt['bool_p_dema_div']
            bool_p_bbands_div = opt['bool_p_bbands_div']

            df_result = pd.DataFrame(columns=('date', 'code', 'op', 'op_rsn', 'op_strength', 'close_p'))  # today's hit
            i_result = 0

            df = pd.DataFrame(['na'] * df.__len__(), columns=['op']).join(df)  #
            df = pd.DataFrame(['pv_ignore'] * df.__len__(), columns=['op_rsn']).join(df)  #
            df = pd.DataFrame([''] * df.__len__(), columns=['op_strength']).join(df)  #
            code = str(df.iloc[1, df.columns.get_loc('code')])

            date = df.iloc[:, df.columns.get_loc('date')]
            o = df.iloc[:, df.columns.get_loc('open')]
            h = df.iloc[:, df.columns.get_loc('high')]
            l = df.iloc[:, df.columns.get_loc('low')]
            c = df.iloc[:, df.columns.get_loc('close')]
            vol = df.iloc[:, df.columns.get_loc('volume')]  # volume
            # amnt=df.iloc[:,df.columns.get_loc('amnt')]  #amount
            # tnv=df.iloc[:,df.columns.get_loc('tnv')]  #turnoverratio

            if df[-1:]['close'].values[0] == 0 or df[-1:]['open'].values[0] == 0 or df[-1:]['volume'].values[0] == 0:
                logging.info(__file__+" "+"ignore as the close price/open price/volume is 0")
                return (df, df_result)

            last_record_time = datetime.now()
            ###############################
            # loop #1 , get std, mean
            ###############################
            if bool_calc_std_mean:

                time_loop_1 = datetime.now()
                # if debug:
                logging.info(str(time_loop_1) + " " + code + " loop 1(std,mean) start ")
                last_record_time = time_loop_1

                # new_value_df = pd.DataFrame([0]*df.__len__(),columns=['c_mean_10D']) #
                # df = new_value_df.join(df)  #

                # 期望找到价格变化不大
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['std_15D_c'])  #
                df = new_value_df.join(df)  #

                # 期望找到成交量变化不大
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['std_15D_vol'])  #
                df = new_value_df.join(df)  #

                # close price percent score in all times, 期望找到 价格在底部
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_c'])
                df = new_value_df.join(df)  # the inserted column on the head

                # 期望找到成交量在底部
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_vol'])  #
                df = new_value_df.join(df)  #

                # price break sigma
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['c_brk_sig'])
                df = df.join(new_value_df)
                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['vol_brk_sig'])
                df = df.join(new_value_df)

                # new_value_df = pd.DataFrame([''] * df.__len__(), columns=['op_strength']);
                # df = df.join(new_value_df)

                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['c_mean_15D'])
                df = new_value_df.join(df)  # the inserted column on the head

                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['vol_mean_15D'])
                df = new_value_df.join(df)  # the inserted column on the head

                # pre_days = 220  # ryan modified from 21 to 220, using a year range for comparation
                # check n days's statistic, include today. When verify, need change Date n-1 value.

                for i in range(df.__len__() - max_exam_day, df.__len__() + 1):
                    #for i in range(1, df.__len__() + 1):
                    # c_perc = stats.percentileofscore(c, df.iloc[i]['close']) / 100
                    # df.iloc[i, df.columns.get_loc('c_perc')] = c_perc
                    # print "loop " + str(i)

                    # if debug:
                    #    print "loop #1, " + str(i) + " of " + str(df.__len__() + 1)

                    #start_day = i - pre_days
                    #if start_day < 0:
                    #    start_day = 0

                    # if debug:
                    #    if  df.iloc[i-1, df.columns.get_loc('date')] == "2015-11-26":
                    #        logging.info(1)

                    # previous pre_days, include today
                    c_prev = c[i - 253:i]
                    vol_prev = vol[i - 253:i]

                    # the script runs after a day close, make decision based on the day close price.
                    # the decision is going to be executed on today's close price at the next day's market opening.

                    # today close at the position of the previous (15 days <-- include today)
                    perc_c = stats.percentileofscore(c_prev, df.iloc[i - 1]['close']) / 100
                    perc_vol = stats.percentileofscore(vol_prev, df.iloc[i - 1]['volume']) / 100

                    df.iloc[i - 1, df.columns.get_loc('perc_c')] = round(perc_c, 1)  # 0,0.1, 0.2...1
                    df.iloc[i - 1, df.columns.get_loc('perc_vol')] = round(perc_vol, 1)

                    c_mean_15D = c_prev.mean()
                    vol_mean_15D = vol_prev.mean()
                    df.iloc[i - 1, df.columns.get_loc('c_mean_15D')] = round(c_mean_15D, 1)  # 0,0.1, 0.2...1
                    df.iloc[i - 1, df.columns.get_loc('vol_mean_15D')] = round(vol_mean_15D, 1)

                    std_15D_c = c_prev.std()
                    if np.isnan(std_15D_c): std_15D_c = 0

                    std_15D_vol = vol_prev.std()
                    if np.isnan(std_15D_vol): std_15D_vol = 0

                    df.iloc[i - 1, df.columns.get_loc('std_15D_c')] = round(std_15D_c, 2)
                    df.iloc[i - 1, df.columns.get_loc('std_15D_vol')] = round(std_15D_vol, 0)

                    if std_15D_c == 0:
                        df.iloc[i - 1, df.columns.get_loc('c_brk_sig')] = 0
                    else:
                        df.iloc[i - 1, df.columns.get_loc('c_brk_sig')] = round((df.iloc[i - 1]['close'] - c_mean_15D) * 1.0 / std_15D_c, 1)

                    if std_15D_vol == 0:
                        df.iloc[i - 1, df.columns.get_loc('vol_brk_sig')] = 0
                    else:
                        df.iloc[i - 1, df.columns.get_loc('vol_brk_sig')] = round((df.iloc[i - 1]['volume'] - vol_mean_15D) * 1.0 / std_15D_vol, 1)

                    strength = abs(df.iloc[i - 1, df.columns.get_loc('vol_brk_sig')]) + \
                               abs(df.iloc[i - 1, df.columns.get_loc('c_brk_sig')])

                    df.iloc[i - 1, df.columns.get_loc('op_strength')] += str(strength)

                    # this is in for loop 1

            ###############################
            # loop #2 , get percent score (0-1) in loop #1 ( std, mean)
            ##############################

            if bool_perc_std_mean:
                time_loop_2 = datetime.now()
                # if debug:
                logging.info(str(time_loop_2) + " " + code + " loop 2(perc score in loop1) started. Last loop took " + str(time_loop_2 - last_record_time))
                last_record_time = time_loop_2

                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_std_15D_c'])  #
                df = new_value_df.join(df)  #

                new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_std_15D_vol'])  #
                df = new_value_df.join(df)  #

                #pre_days =   # ryan modified from 21 to 220, using year range for comparation

                for i in range(df.__len__() - max_exam_day, df.__len__()):
                    # if debug:
                    #    print "loop #2, " + str(i) + " of " + str(df.__len__() + 1)

                    #start_day = i - pre_days
                    #if start_day < 0:
                    #    #start_day = 0
                    #    continue

                    df.iloc[i, df.columns.get_loc('perc_std_15D_c')] = round(stats.percentileofscore(df.iloc[(i - 253):i]['std_15D_c'], df.iloc[i]['std_15D_c']) / 100, 1)
                    df.iloc[i, df.columns.get_loc('perc_std_15D_vol')] = round(stats.percentileofscore(df.iloc[(i - 253):i]['std_15D_vol'], df.iloc[i]['std_15D_vol']) / 100, 1)

            ###############################
            # loop #2.5, the most dropp and most up stock
            ##############################

            if bool_pv_hit:
                time_loop_2_5 = datetime.now()
                # if debug:
                logging.info(str(time_loop_2_5) + " " + code + " loop 2.5 (most price change) started. Last loop took " + str(time_loop_2_5 - last_record_time))
                last_record_time = time_loop_2_5

                df_loop_2_5 = df_52_week[int(df_52_week.__len__() / 2):]  #half year
                # df_loop_2_5 = df_52_week[-30:]  # ryan, debug, 30 day, more chance to be hitted.

                df_loop_2_5 = df_loop_2_5.reset_index().drop('index', axis=1)

                new_value_df = pd.DataFrame([0] * df_loop_2_5.__len__(), columns=['price_change_perc'])  # today_close - yesterday_close
                df_loop_2_5 = new_value_df.join(df_loop_2_5)  #

                closeP = str(df_loop_2_5[-1:]['close'].values[0])

                for i in range(df_loop_2_5.__len__() - max_exam_day, df_loop_2_5.__len__()):
                    yesterday_close = df_loop_2_5.iloc[i - 1]['close']

                    today_close = df_loop_2_5.iloc[i]['close']

                    delta_perc = (today_close - yesterday_close) * 1.0 / yesterday_close * 100
                    delta_perc = round(delta_perc, 2)

                    df_loop_2_5.iloc[i, df_loop_2_5.columns.get_loc('price_change_perc')] = delta_perc

                if df_loop_2_5['price_change_perc'].max() == df_loop_2_5['price_change_perc'][-1:].values[0]:
                    logging.info(__file__+" "+"code " + str(code) + " hit the max daily increase in last " + str(df_loop_2_5.__len__()) + " days")
                    df_result.loc[i_result] = [df_loop_2_5['date'][-1:].values[0], code, 'S', code + "_max_daily_increase", 1, closeP]
                    i_result += 1
                elif df_loop_2_5['price_change_perc'].min() == df_loop_2_5['price_change_perc'][-1:].values[0]:
                    logging.info(__file__+" "+"code " + str(code) + " hit the max daily decrease in last " + str(df_loop_2_5.__len__()) + " days")
                    df_result.loc[i_result] = [df_loop_2_5['date'][-1:].values[0], code, 'B', code + "_max_daily_decrease", 1, closeP]
                    i_result += 1

                # check price gap between yesterdy_close and today_open
                today_high = df_loop_2_5.iloc[-1:]['high'].values[0]
                today_open = df_loop_2_5.iloc[-1:]['open'].values[0]
                today_low = df_loop_2_5.iloc[-1:]['low'].values[0]
                today_close = df_loop_2_5.iloc[-1:]['close'].values[0]

                yesterday_high = df_loop_2_5.iloc[-2:-1]['high'].values[0]
                yesterday_open = df_loop_2_5.iloc[-2:-1]['open'].values[0]
                yesterday_low = df_loop_2_5.iloc[-2:-1]['low'].values[0]
                yesterday_close = df_loop_2_5.iloc[-2:-1]['close'].values[0]

                # if yesterday_close > 0 and (today_open < yesterday_close) and (today_open > today_close) : #and (round(today_open,1) == round(today_high,1))
                if yesterday_close > 0 and (today_open < yesterday_close) and (yesterday_close > today_close) and (today_high < yesterday_close):  # and (round(today_open,1) == round(today_high,1))
                    logging.info(__file__+" "+"code " + str(code) + " hit decrease gap ")
                    op_strength = round((yesterday_close - today_open) * 100.0 / yesterday_close, 2)
                    df_result.loc[i_result] = [df_loop_2_5['date'][-1:].values[0], code, 'B', code + "_decrease_gap", op_strength, closeP]
                    i_result += 1

                # if yesterday_close > 0 and (today_open > yesterday_close) and (today_open < today_close): #and (today_open == today_high)
                if yesterday_close > 0 and (today_open > yesterday_close) and (yesterday_close < today_close) and (today_low > yesterday_close):  # and (today_open == today_high)
                    logging.info(__file__+" "+"code " + str(code) + " hit increase gap ")
                    op_strength = round((today_open - yesterday_close) * 100.0 / yesterday_close, 2)
                    df_result.loc[i_result] = [df_loop_2_5['date'][-1:].values[0], code, 'S', code + "_increase_gap", op_strength, closeP]
                    i_result += 1

            ###############################
            # Loop #3 talib pattern
            ###############################

            if bool_talib_pattern:

                time_loop_3 = datetime.now()
                # if debug:
                logging.info(str(time_loop_3) + " " + code + " loop 3(talib ptn) started. Last loop took " + str(time_loop_3 - last_record_time))
                last_record_time = time_loop_3

                pattern = talib.get_function_groups()['Pattern Recognition']

                p_cnt = 0

                for p in pattern:
                    p_cnt += 1

                    if debug and p_cnt > 300:
                        logging.info(__file__+" "+"in debug mode, break talib pattern after 300 times running.")
                        break

                    cmd = "talib." + p + "(o.values, h.values, l.values, c.values)"
                    # print cmd
                    tmp = eval(cmd)

                    start_rec = 1
                    if live_trading:
                        start_rec = tmp.__len__()

                    for j in range(tmp.__len__() + 1 - max_exam_day, tmp.__len__() + 1):
                        # if debug:
                        #    print "loop #3. ptn " + str(p) + ". ptnCnt: "+ str(p_cnt) + ' of ' + str(pattern.__len__())+". bt record on ptn:"+ str(j)+' of '+ str(tmp.__len__() + 1)

                        date = df.iloc[j - 1, df.columns.get_loc('date')]
                        code = str(df.iloc[j - 1, df.columns.get_loc('code')])
                        closeP = df.iloc[j - 1, df.columns.get_loc('close')]

                        if tmp[j - 1] == 0:
                            # print "no talib op, ptn "+p
                            pass
                        elif tmp[j - 1] > 0:  # 100
                            # print "talib buy op, ptn "+p

                            df.iloc[j - 1, df.columns.get_loc('op')] += ";B"
                            df.iloc[j - 1, df.columns.get_loc('op_rsn')] += ";" + str(code) + "_B_talib_" + p
                            df.iloc[j - 1, df.columns.get_loc('op_strength')] += ",1"  # talib strength always be 0.1

                            if exam_date == df.iloc[j - 1, df.columns.get_loc('date')]:
                                df_result.loc[i_result] = [date, code, 'B', code + "_B_talib_" + p, 1, closeP]
                                i_result += 1

                        elif tmp[j - 1] < 0:  # -100
                            # print "talib sell op, ptn "+p
                            df.iloc[j - 1, df.columns.get_loc('op')] += ";S"
                            df.iloc[j - 1, df.columns.get_loc('op_rsn')] += ";" + str(code) + "_S_talib_" + p
                            df.iloc[j - 1, df.columns.get_loc('op_strength')] += ",-1"  # talib strength always be 0.1

                            if exam_date == df.iloc[j - 1, df.columns.get_loc('date')]:
                                df_result.loc[i_result] = [date, code, 'S', code + "_S_talib_" + p, 1, closeP]
                                i_result += 1

            ###############################
            # loop #3.5 , 52 weeks price analyze
            ###############################

            if bool_pv_hit:  # share same switch with pv_hit
                time_loop_3_5 = datetime.now()
                # if debug:
                logging.info(str(time_loop_3_5) + " " + code + " loop 3.5(52 weeks price analyze) started. Last loop took " + str(time_loop_3_5 - last_record_time))
                last_record_time = time_loop_3_5

                exam_date_in_df = df_52_week.iloc[-1].date

                max_close_index = df_52_week['close'].idxmax()
                min_close_index = df_52_week['close'].idxmin()

                max_close_df = df_52_week.loc[max_close_index]
                min_close_df = df_52_week.loc[min_close_index]

                df_last_row = df_52_week[-1:]

                # 52week price
                if (df_last_row['close'].values[0] - min_close_df['close']) < 0.02 * min_close_df['close']:  # 2% near the lowest price
                    date_min_c = min_close_df['date']
                    time_delta = datetime.strptime(df_last_row['date'].values[0], '%Y%m%d') - datetime.strptime(date_min_c, '%Y%m%d')
                    time_delta = time_delta.days

                    if time_delta >= 0:
                        logging.info(code + ", today price ( " + \
                                     df_last_row['date'].values[0] + ", " + \
                                     str(df_last_row['close'].values[0]) + \
                                     ") approach 52 weeks low (" + \
                                     date_min_c + "," + str(min_close_df['close']) + "), " + \
                                     str(time_delta) + " days ago")

                        df_result.loc[i_result] = [df_last_row['date'].values[0], code, 'B', code + "_B_pvbreak_lp_year", time_delta, df_last_row['close'].values[0]]
                        i_result += 1

                if (max_close_df['close'] - df_last_row['close'].values[0]) < 0.02 * max_close_df['close']:  # 2% near the highest price
                    # if tmp_a/max_close_df['close']  > 0.9: #90% near the highest price
                    date_max_c = max_close_df['date']
                    time_delta = datetime.strptime(df_last_row['date'].values[0], '%Y%m%d') - datetime.strptime(date_max_c, '%Y%m%d')
                    time_delta = time_delta.days

                    if time_delta >= 0:
                        logging.info(code + ", today price( " + \
                                     df_last_row['date'].values[0] + ", " + \
                                     str(df_last_row['close'].values[0]) + \
                                     ") approach 52 weeks high (" + \
                                     date_max_c + "," + str(max_close_df['close']) + "), " + \
                                     str(time_delta) + " days ago")

                        df_result.loc[i_result] = [df_last_row['date'].values[0], code, 'S', code + "_S_pvbreak_hp_year", time_delta, df_last_row['close'].values[0]]
                        i_result += 1

                # 52week volume
                max_vol_df = df_52_week.loc[df_52_week['volume'].idxmax()]
                min_vol_df = df_52_week.loc[df_52_week['volume'].idxmin()]

                # if tmp_c < 0.1: #10% near the lowest vol
                if (df_last_row['volume'].values[0] - min_vol_df['volume']) < 0.03 * min_vol_df['volume']:  # 3% near the lowest vol
                    date_min_v = min_vol_df['date']
                    time_delta = datetime.strptime(df_last_row['date'].values[0], '%Y-%m-%d') - datetime.strptime(date_min_v, '%Y-%m-%d')
                    time_delta = time_delta.days

                    if time_delta > 0:
                        logging.info(code + ", today volume( " + \
                                     df_last_row['date'].values[0] + ", " + \
                                     str(df_last_row['volume'].values[0]) + \
                                     ") approach 52 weeks low (" + \
                                     date_min_v + "," + str(min_vol_df['volume']) + "), " + \
                                     str(time_delta)) + " days ago"

                        df_result.loc[i_result] = [date, code, 'B', code + "_B_pvbreak_lv_year", time_delta, df_last_row['close'].values[0]]
                        i_result += 1

                # if tmp_a/max_vol_df['volume'] > 0.9: #90% near the highest vol
                if (max_vol_df['volume'] - df_last_row['volume'].values[0]) < 0.03 * max_vol_df['volume']:
                    date_max_v = max_vol_df['date']
                    time_delta = datetime.strptime(df_last_row['date'].values[0], '%Y-%m-%d') - datetime.strptime(date_max_v, '%Y-%m-%d')
                    time_delta = time_delta.days

                    if time_delta > 0:
                        logging.info(code + ", today volume( " + \
                                     df_last_row['date'].values[0] + ", " + \
                                     str(df_last_row['volume'].values[0]) + \
                                     ") approach 52 weeks high (" + \
                                     date_max_v + "," + str(max_vol_df['volume']) + "), " + \
                                     str(time_delta)) + " days ago"

                        df_result.loc[i_result] = [date, code, 'S', code + "_S_pvbreak_hv_year", time_delta, df_last_row['close'].values[0]]
                        i_result += 1

            ###############################
            # loop #4 , price, volume analyze
            ###############################

            if bool_pv_hit:
                time_loop_4 = datetime.now()
                # if debug:
                logging.info(str(time_loop_4) + " " + code + " loop 4(PV Analyze) started. Last loop took " + str(time_loop_4 - last_record_time))
                last_record_time = time_loop_4

                pre_days = 7  # Decide today's P/V status, based on last 5 days' the P/V position.
                threhold = round(0.7 * pre_days, 0)

                start_rec = 1
                if live_trading:
                    start_rec = df.__len__()

                df = pd.DataFrame([''] * df.__len__(), columns=['vol_pos']).join(df)  #
                df = pd.DataFrame([''] * df.__len__(), columns=['5D_vol_vlt']).join(df)  #
                df = pd.DataFrame([''] * df.__len__(), columns=['c_pos']).join(df)  #
                df = pd.DataFrame([''] * df.__len__(), columns=['5D_c_vlt']).join(df)  #

                for i in range(df.__len__() - pre_days, df.__len__() + 1):
                    # if debug:
                    #   print "loop #4, " + str(i) + " of " + str(df.__len__() + 1)
                    cnt_vol_vlt_low = 0
                    cnt_vol_vlt_high = 0
                    cnt_vol_pos_low = 0
                    cnt_vol_pos_high = 0

                    cnt_c_vlt_low = 0
                    cnt_c_vlt_high = 0

                    cnt_c_pos_low = 0
                    cnt_c_pos_high = 0

                    #start_day = i - pre_days

                    #if start_day < 0:
                    #    start_day = 0

                    # previous pre_days vol, include today
                    vol_prev = df["perc_vol"][i - 253:i]
                    vol_std_prev = df["perc_std_15D_vol"][i - 253:i]

                    # previous pre_days close, include today
                    c_prev = df["perc_c"][i - 253:i]
                    c_std_prev = df["perc_std_15D_c"][i - 253:i]

                    # previous op
                    op_pre = df['op'][:i]  # not last 7 days, check all
                    op_list = op_pre[op_pre != '']

                    val_pre_op = 'na'
                    pre_op = ''
                    pre_opn = 0
                    this_buy_num = 0
                    this_sell_num = 0

                    if op_list.__len__() > 0:
                        idx_pre_op = op_list.index[-1]  # index of latest operation in previous
                        val_pre_op = op_pre[idx_pre_op]  # value of latest op, in 'B0','S0','pB', 'B1','S1' etc.
                        # print "matched previous op "+val_pre_op

                    op_match = re.match("([B|S])(\d+)", val_pre_op)
                    if op_match:
                        pre_op = op_match.group(1)  # in B, S
                        pre_opn = op_match.group(2)  # in 0, 1, 2..

                    # print "previous op "+ pre_op
                    # print "previous op num " + str(pre_opn)

                    if (pre_op == 'B'):
                        this_sell_num = 0
                        this_buy_num = int(pre_opn) + 1

                    if (pre_op == 'S'):
                        this_buy_num = 0
                        this_sell_num = int(pre_opn) + 1

                    # print "this_buy_num "+str(this_buy_num)
                    # print "this_sell_num "+str(this_sell_num)

                    # if i>20:
                    # print 1

                    # vol std
                    for i2 in vol_std_prev:
                        if i2 <= 0.15 and i2 > 0:
                            cnt_vol_vlt_low += 1
                        elif i2 >= 0.85:
                            cnt_vol_vlt_high += 1

                    if (cnt_vol_vlt_low >= threhold) and (cnt_vol_vlt_high < 1):  # more than 3 out of 5
                        df.iloc[i - 1, df.columns.get_loc('5D_vol_vlt')] = 'v_vlt_l'
                    elif (cnt_vol_vlt_high >= threhold) and (cnt_vol_vlt_low < 1):
                        df.iloc[i - 1, df.columns.get_loc('5D_vol_vlt')] = 'v_vlt_h'

                    # vol
                    for i2 in vol_prev:
                        if i2 <= 0.15 and i2 > 0:
                            cnt_vol_pos_low += 1
                        elif i2 >= 0.85:
                            cnt_vol_pos_high += 1

                    if (cnt_vol_pos_low >= threhold) and (cnt_vol_pos_high < 1):  # more than 3 out of 5
                        df.iloc[i - 1, df.columns.get_loc('vol_pos')] = "v_pos_l"
                    elif (cnt_vol_pos_high >= threhold) and (cnt_vol_pos_low < 1):
                        df.iloc[i - 1, df.columns.get_loc('vol_pos')] = "v_pos_h"

                    # close std
                    for i2 in c_std_prev:
                        if i2 <= 0.15 and i2 > 0:
                            cnt_c_vlt_low += 1
                        elif i2 >= 0.85:
                            cnt_c_vlt_high += 1

                    if (cnt_c_vlt_low >= threhold) and (cnt_c_vlt_high < 1):  # more than 3 out of 5. No vlt_high
                        df.iloc[i - 1, df.columns.get_loc('5D_c_vlt')] = 'c_vlt_l'
                    elif (cnt_c_vlt_high >= threhold) and (cnt_c_vlt_low < 1):
                        df.iloc[i - 1, df.columns.get_loc('5D_c_vlt')] = 'c_vlt_h'

                    # close
                    for i2 in c_prev:
                        if i2 <= 0.15 and i2 > 0:
                            cnt_c_pos_low += 1
                        elif i2 >= 0.85:
                            cnt_c_pos_high += 1

                    if (cnt_c_pos_low >= threhold) and (cnt_c_pos_high < 1):  # more than 3 out of 5
                        df.iloc[i - 1, df.columns.get_loc('c_pos')] = "c_pos_l"
                    elif (cnt_c_pos_high >= threhold) and (cnt_c_pos_low < 1):
                        df.iloc[i - 1, df.columns.get_loc('c_pos')] = "c_pos_h"

                    # 根据当天收盘价统计，假设第二天开盘价格==第一天收盘价格, 所以为第二天<<开盘>>时的价格的操作建议.
                    # logical start: buy
                    code = str(df.iloc[i - 1, df.columns.get_loc('code')])

                    # code_match = re.match('S[H|Z](\d+)', code)
                    # if code_match:
                    #    pass
                    # code = code_match.group(1)
                    # else:
                    #    pass
                    # logging.info(__file__+" "+"wrong code"+code)
                    # exit(1)

                    time = str(df.iloc[i - 1, df.columns.get_loc('date')])
                    close_p = str(df.iloc[i - 1, df.columns.get_loc('close')])  # suppose close(today) == open(next_day)

                    vol_pos = df.iloc[i - 1, df.columns.get_loc('vol_pos')]
                    vol_vlt = df.iloc[i - 1, df.columns.get_loc('5D_vol_vlt')]
                    c_pos = df.iloc[i - 1, df.columns.get_loc('c_pos')]
                    c_vlt = df.iloc[i - 1, df.columns.get_loc('5D_c_vlt')]

                    vol_break = df.iloc[i - 1, df.columns.get_loc('vol_brk_sig')]
                    c_break = df.iloc[i - 1, df.columns.get_loc('c_brk_sig')]
                    op_strength = df.iloc[i - 1, df.columns.get_loc('op_strength')]

                    if forex:  # forex doesn't have vol data. otherwise forex will never hit the ptn
                        vol_vlt = 'v_vlt_l'
                        vol_pos = 'v_pos_l'
                        vol_break = 2

                    # Buy 1, 成交量在低位或者不变，并且价格在低位或者不变，并且成交量或者价格突破
                    if (((vol_vlt == 'v_vlt_l') or (vol_pos == 'v_pos_l')) and (c_pos == 'c_pos_l')
                            # and (( c_vlt == 'c_vlt_l') or (c_pos == 'c_pos_l'))
                            # and (not c_pos == 'c_pos_h')
                            and ((vol_break >= 1.5) or (c_break >= 1.5) or (vol_break <= -1.5) or (c_break <= -1.5))):
                        reason = code + "_B_pvbreak_lp_lv_v_or_c_up_or_dn_brk"  # price low, vol low, vol/c up/down break
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';B' + str(this_buy_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"Buy "+str(this_buy_num)+ " "+code+" "+time+" "+ close_p+" "+reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'B', reason, op_strength, close_p]
                            i_result += 1

                    # Buy 2. 无量上涨. 价格底部, 成交量不变或者低，价格突破 <--- Buy
                    if (((vol_vlt == 'v_vlt_l') or (vol_pos == 'v_pos_l')) and (c_pos == 'c_pos_l')
                            # and (( c_vlt == 'c_vlt_l') or (c_pos == 'c_pos_l'))
                            # and (not c_pos == 'c_pos_h')
                            and (c_break >= 1)):
                        reason = code + "_B_pvbreak_lp_lv_p_up_brk"  # price low, vol low, price up break.
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';B' + str(this_buy_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"Buy "+str(this_buy_num)+ " " + code + " " + time + " " + close_p + " " + reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'B', reason, op_strength, close_p]
                            i_result += 1

                    # Buy 3 价格底部，成交量突破上涨 <-- 之前是跌的 初期buy
                    if ((c_pos == 'c_pos_l') and (vol_break >= 1.4)):
                        reason = code + "_B_pvbreak_lp_v_up_brk"  # price low, vol up break
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';B' + str(this_buy_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"Buy "+str(this_buy_num)+ " " + code + " " + time + " " + close_p + " " + reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'B', reason, op_strength, close_p]
                            i_result += 1

                    # Sell 1 价格顶部，成绩量突破上涨 <--  之前是涨的 末期sell
                    if (((c_pos == 'c_pos_h')) and (vol_break >= 1.5)):
                        reason = code + "_S_pvbreak_hp_v_up_brk"  # high price, vol up break
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';S' + str(this_sell_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"Sell "+str(this_sell_num) + " "+ code + " " + time + " " + close_p + " " + reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'S', reason, op_strength, close_p]
                            i_result += 1

                    # Sell 2, 放量下跌 <---- 之前是涨的， Sell
                    if ((c_pos == 'c_pos_h') and (c_break <= -1) and (vol_break > 1)):
                        reason = code + "_S_pvbreak_hp_p_dn_brk_v_up_brk"  # price high, price down break, vol up break.
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';S' + str(this_sell_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"Sell " +str(this_sell_num)+ " "+ code + " " + time + " " + close_p + " " + reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'S', reason, op_strength, close_p]
                            i_result += 1

                    # Potential Buy 2, 放量下跌 <---- 之前是跌的，Buy， 右侧交易
                    if ((c_pos == 'c_pos_l') and (c_break <= -1) and (vol_break > 1)):
                        reason = code + "_B_pvbreak_lp_p_dn_brk_v_up_brk"  # price low, price down break, vol up break
                        df.iloc[i - 1, df.columns.get_loc('op')] += ';B' + str(this_buy_num)
                        df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                        # logging.info(__file__+" "+"B "+str(this_buy_num)+ " " + code + " " + time + " " + close_p + " " + reason)
                        if exam_date == time:
                            df_result.loc[i_result] = [time, code, 'B', reason, op_strength, close_p]
                            i_result += 1
        except:
            traceback.print_exception(*sys.exc_info())

        ###############################
        # loop #5 , div of price and (mfi, rsi, natr,  )
        ###############################

        time_loop_5 = datetime.now()
        # if debug:
        logging.info(str(time_loop_5) + " " + code + " loop 5 (DIV) started . Last loop took " \
                     + str(time_loop_5 - last_record_time))

        last_record_time = time_loop_5

        # if bool_p_mfi_div  or bool_p_rsi_div or bool_p_natr_div:

        n_mfi_period = 14  # calculate 14_period_MFI
        n_compare = 15  # Evaluate last 15 days divergence signal. The Buy/Sell sig MAX appearance is one.(buy 1,sell 1)
        # each day has its price and MFI_14, compare 15 days data.
        # e.g., get 15 day max price and MFI_14.

        n_rsi_period = 14
        n_natr_period = 14
        n_tema_period = 30
        n_trima_period = 30
        loop_num = target = ''
        target_period = 0

        # pre_days = n_mfi_period + n_compare + 1
        code = str(df.iloc[1, df.columns.get_loc('code')])
        p_cnt = 0

        target = "pv"  #for pv_div
        (df_result, i_result, df) = self.calc_div(loop_num=loop_num, code=code, target=target, \
                                                  target_period=max_exam_day, \
                                                  comparing_window=253, df=df, \
                                                  df_result=df_result, i_result=i_result, \
                                                  exam_date=exam_date, debug=debug, live_trading=live_trading)


        for b in ('bool_p_mfi_div', 'bool_p_rsi_div', 'bool_p_natr_div', 'bool_p_tema_div', 'bool_p_trima_div', \
                  'bool_p_adx_div', 'bool_p_adxr_div', 'bool_p_apo_div', 'bool_p_aroon_div', 'bool_p_aroonosc_div', \
                  'bool_p_bop_div', 'bool_p_cci_div', 'bool_p_cmo_div', 'bool_p_dx_div', 'bool_p_minusdi_div', \
                  'bool_p_minusdm_div', 'bool_p_mom_div', 'bool_p_plusdi_div', 'bool_p_plusdm_div', 'bool_p_ppo_div', \
                  'bool_p_roc_div', 'bool_p_rocp_div', 'bool_p_rocr_div', 'bool_p_rocr100_div', 'bool_p_trix_div', \
                  'bool_p_ultosc_div', 'bool_p_willr_div', 'bool_p_macd_div', 'bool_p_macdext_div',
                  'bool_p_macdfix_div', \
                  'bool_p_ad_div', 'bool_p_adosc_div', 'bool_p_obv_div', 'bool_p_avgprice_div', 'bool_p_medprice_div', \
                  'bool_p_typprice_div', 'bool_p_wclprice_div', 'bool_p_htdcperiod_div', 'bool_p_htdcphase_div',
                  'bool_p_htphasor_div', \
                  'bool_p_htsine_div', 'bool_p_httrendmode_div', 'bool_p_beta_div', 'bool_p_correl_div',
                  'bool_p_linearreg_div', \
                  'bool_p_linearregangle_div', 'bool_p_linearregintercept_div', 'bool_p_linearregslope_div',
                  'bool_p_stddev_div', 'bool_p_tsf_div', \
                  'bool_p_var_div', 'bool_p_wma_div', 'bool_p_t3_div', 'bool_p_sma_div', 'bool_p_sarext_div', \
                  'bool_p_sar_div', 'bool_p_midprice_div', 'bool_p_midpoint_div', 'bool_p_mavp_div', 'bool_p_mama_div', \
                  'bool_p_ma_div', 'bool_p_kama_div', 'bool_p_httrendline_div', 'bool_p_ema_div', 'bool_p_dema_div', \
                  'bool_p_bbands_div'
                  ):

            if debug and p_cnt > 300:
                logging.info(__file__+" "+"in debug mode, break talib indicator div after 300 times running.")
                break

            if (b == 'bool_p_mfi_div' and eval(b) == True):
                loop_num = '5'
                target = 'mfi'
                target_period = n_mfi_period
            elif (b == 'bool_p_rsi_div' and eval(b) == True):
                loop_num = '6'
                target = 'rsi'
                target_period = n_rsi_period
            elif (b == 'bool_p_natr_div' and eval(b) == True):
                loop_num = '7'
                target = 'natr'
                target_period = n_natr_period
            elif (b == 'bool_p_tema_div' and eval(b) == True):
                loop_num = '8'
                target = 'tema'
                target_period = n_tema_period
            elif (b == 'bool_p_trima_div' and eval(b) == True):
                loop_num = '9'
                target = 'trima'
                target_period = n_trima_period

            elif (b == 'bool_p_adx_div' and eval(b) == True):
                loop_num = '10'
                target = 'adx'
            elif (b == 'bool_p_adxr_div' and eval(b) == True):
                loop_num = '11'
                target = 'adxr'
            elif (b == 'bool_p_apo_div' and eval(b) == True):
                loop_num = '12'
                target = 'apo'
            elif (b == 'bool_p_aroon_div' and eval(b) == True):
                loop_num = '13'
                target = 'aroon'
            elif (b == 'bool_p_aroonosc_div' and eval(b) == True):
                loop_num = '14'
                target = 'aroonosc'
            elif (b == 'bool_p_bop_div' and eval(b) == True):
                loop_num = '15'
                target = 'bop'
            elif (b == 'bool_p_cci_div' and eval(b) == True):
                loop_num = '16'
                target = 'cci'
            elif (b == 'bool_p_cmo_div' and eval(b) == True):
                loop_num = '17'
                target = 'cmo'
            elif (b == 'bool_p_dx_div' and eval(b) == True):
                loop_num = '18'
                target = 'dx'
            elif (b == 'bool_p_minusdi_div' and eval(b) == True):
                loop_num = '19'
                target = 'minusdi'
            elif (b == 'bool_p_minusdm_div' and eval(b) == True):
                loop_num = '20'
                target = 'minusdm'
            elif (b == 'bool_p_mom_div' and eval(b) == True):
                loop_num = '21'
                target = 'mom'
            elif (b == 'bool_p_plusdi_div' and eval(b) == True):
                loop_num = '22'
                target = 'plusdi'
            elif (b == 'bool_p_plusdm_div' and eval(b) == True):
                loop_num = '23'
                target = 'plusdm'
            elif (b == 'bool_p_ppo_div' and eval(b) == True):
                loop_num = '24'
                target = 'ppo'
            elif (b == 'bool_p_roc_div' and eval(b) == True):
                loop_num = '25'
                target = 'roc'
            elif (b == 'bool_p_rocp_div' and eval(b) == True):
                loop_num = '26'
                target = 'rocp'
            elif (b == 'bool_p_rocr_div' and eval(b) == True):
                loop_num = '27'
                target = 'rocr'
            elif (b == 'bool_p_rocr100_div' and eval(b) == True):
                loop_num = '28'
                target = 'rocr100'
            elif (b == 'bool_p_trix_div' and eval(b) == True):
                loop_num = '29'
                target = 'trix'
            elif (b == 'bool_p_ultosc_div' and eval(b) == True):
                loop_num = '30'
                target = 'ultosc'
            elif (b == 'bool_p_willr_div' and eval(b) == True):
                loop_num = '31'
                target = 'willr'
            elif (b == 'bool_p_macd_div' and eval(b) == True):
                loop_num = '32'
                target = 'macd'
            elif (b == 'bool_p_macdext_div' and eval(b) == True):
                loop_num = '33'
                target = 'macdext'
            elif (b == 'bool_p_macdfix_div' and eval(b) == True):
                loop_num = '34'
                target = 'macdfix'

            elif (b == 'bool_p_ad_div' and eval(b) == True):
                loop_num = '35'
                target = 'ad'
            elif (b == 'bool_p_adosc_div' and eval(b) == True):
                loop_num = '36'
                target = 'adosc'
            elif (b == 'bool_p_obv_div' and eval(b) == True):
                loop_num = '37'
                target = 'obv'

            elif (b == 'bool_p_avgprice_div' and eval(b) == True):
                loop_num = '38'
                target = 'avgprice'
            elif (b == 'bool_p_medprice_div' and eval(b) == True):
                loop_num = '39'
                target = 'medprice'
            elif (b == 'bool_p_typprice_div' and eval(b) == True):
                loop_num = '40'
                target = 'typprice'
            elif (b == 'bool_p_wclprice_div' and eval(b) == True):
                loop_num = '41'
                target = 'wclprice'

            elif (b == 'bool_p_htdcperiod_div' and eval(b) == True):
                loop_num = '42'
                target = 'ht_dcperiod'
            elif (b == 'bool_p_htdcphase_div' and eval(b) == True):
                loop_num = '43'
                target = 'ht_dcphase'
            elif (b == 'bool_p_htphasor_div' and eval(b) == True):
                loop_num = '44'
                target = 'ht_phasor'
            elif (b == 'bool_p_htsine_div' and eval(b) == True):
                loop_num = '45'
                target = 'ht_sine'
            elif (b == 'bool_p_httrendmode_div' and eval(b) == True):
                loop_num = '46'
                target = 'ht_trendmode'

            elif (b == 'bool_p_beta_div' and eval(b) == True):
                loop_num = '47'
                target = 'beta'
            elif (b == 'bool_p_correl_div' and eval(b) == True):
                loop_num = '48'
                target = 'correl'
            elif (b == 'bool_p_linearreg_div' and eval(b) == True):
                loop_num = '49'
                target = 'linearreg'
            elif (b == 'bool_p_linearregangle_div' and eval(b) == True):
                loop_num = '50'
                target = 'linearreg_angle'
            elif (b == 'bool_p_linearregintercept_div' and eval(b) == True):
                loop_num = '51'
                target = 'linearreg_intercept'
            elif (b == 'bool_p_linearregslope_div' and eval(b) == True):
                loop_num = '52'
                target = 'linearreg_slope'
            elif (b == 'bool_p_stddev_div' and eval(b) == True):
                loop_num = '53'
                target = 'stddev'
            elif (b == 'bool_p_tsf_div' and eval(b) == True):
                loop_num = '54'
                target = 'tsf'
            elif (b == 'bool_p_var_div' and eval(b) == True):
                loop_num = '55'
                target = 'var'

            elif (b == 'bool_p_wma_div' and eval(b) == True):
                loop_num = '56'
                target = 'wma'
            elif (b == 'bool_p_t3_div' and eval(b) == True):
                loop_num = '57'
                target = 't3'
            elif (b == 'bool_p_sma_div' and eval(b) == True):
                loop_num = '58'
                target = 'sma'
            elif (b == 'bool_p_sarext_div' and eval(b) == True):
                loop_num = '59'
                target = 'sarext'
            elif (b == 'bool_p_sar_div' and eval(b) == True):
                loop_num = '60'
                target = 'sar'
            elif (b == 'bool_p_midprice_div' and eval(b) == True):
                loop_num = '61'
                target = 'midprice'
            elif (b == 'bool_p_midpoint_div' and eval(b) == True):
                loop_num = '62'
                target = 'midpoint'
            # elif  (b == 'bool_p_mavp_div' and  eval(b) == True) :    loop_num='63'; target = 'mavp';
            elif (b == 'bool_p_mama_div' and eval(b) == True):
                loop_num = '64'
                target = 'mama'
            elif (b == 'bool_p_ma_div' and eval(b) == True):
                loop_num = '65'
                target = 'ma'
            elif (b == 'bool_p_kama_div' and eval(b) == True):
                loop_num = '66'
                target = 'kama'
            elif (b == 'bool_p_httrendline_div' and eval(b) == True):
                loop_num = '67'
                target = 'ht_trendline'
            elif (b == 'bool_p_ema_div' and eval(b) == True):
                loop_num = '68'
                target = 'ema'
            elif (b == 'bool_p_dema_div' and eval(b) == True):
                loop_num = '69'
                target = 'dema'
            elif (b == 'bool_p_bbands_div' and eval(b) == True):
                loop_num = '70'
                target = 'bbands'

            if not target == '':
                # print "targe is "+target

                p_cnt += 1

                # df, df_result saves to file right after they calculated, \
                # so no need to warriy about the multi-proc, especially in function calls
                # ABOVE STATEMENT IS NOT TRUE, df and df_result saved in this function later !! 20180202
                # print "loop 5, running target " +target
                last_record_time_loop_5 = datetime.now()

                #(df_result, i_result, df) = self.calc_div(loop_num=loop_num, code=code, target=target, \
                #                                          target_period=target_period, \
                #                                          comparing_window=n_compare, df=df, \
                #                                          df_result=df_result, i_result=i_result, \
                #                                          exam_date=exam_date, debug=debug, live_trading=live_trading)

                if debug:
                    logging.info((code + " loop 5 target " + str(last_record_time) + " took ") + str(
                        datetime.now() - last_record_time_loop_5) + \
                          ", loop 5 took " + str(datetime.now() - last_record_time))

                # reset target
                target = ''
                loop_num = ''

                # print ("loop 5 start at " + str(time_loop_5))
                # last_record_time = time_loop_5

        logging.info(str(datetime.now()) + " " + code + " loop 5 (DIV) completed . Last loop took " \
                     + str(datetime.now() - last_record_time))

        #################################################
        ######### Verify Completed, now save result TO CSV
        #################################################
        # save to csv
        df_result = df_result.drop_duplicates().reset_index()
        if i_result > 0:  # Today (Exam_day) has B/S signal
            df_result.to_csv(outputF_today, index=False)
            logging.info(str(datetime.now()) + " Save " + code + " " + exam_date + ",  B/S signal to " + outputF_today)
        else:
            logging.info((code + " " + exam_date + ", no B/S signal."))

        cols = df.columns.tolist()

        cols_exp = [
            'date',
            'code',
            'op',
            'op_rsn',
            'op_strength',
            'c',
            'o',
            'vol',
            'vol_pos',
            '5D_vol_vlt',
            'c_pos',
            '5D_c_vlt',
            'vol_brk_sig',
            'c_brk_sig',
            'o',
            'h',
            'l',
            'c',
            'tnv',
            'std_15D_vol',
            "perc_std_15D_vol",
            "perc_vol",
            'std_15D_c',
            "perc_std_15D_c",
            "perc_c",
            'c_mean_15D',
            "vol_mean_15D",
            "std_15D_c",
            'std_15D_vol',
            'price_change_perc',
        ]

        final_cols = list(set(cols) & set(cols_exp))

        df = df[final_cols]
        df.to_csv(outputF, encoding='UTF-8', index=False)
        logging.info(str(datetime.now()) + " save to file " + outputF)  # 'outputF': "/home/ryan/DATA/tmp/pv/AG/" + file,
        return (df, df_result)

        # if debug:
        # df.to_csv("/home/ryan/DATA/DAY_dev/price_volume.csv",index=False)
        # print "debug done, /home/ryan/DATA/DAY_dev/price_volume.csv"
        # exit

    # else:
    #     df.to_csv(outputF,index=False)
    # print "save to file "+outputF

    # general function to calculate the divergency, eg, price-mfi, price-rsi div.
    def calc_div(self, loop_num, code, target, target_period=14, comparing_window=15, df='', \
                 df_result='', i_result='', exam_date='', debug=False, live_trading=False):

        #pre_days = target_period + comparing_window + 1
        target = str(target).lower()
        # print ("target is "+target)

        start_rec = 1
        end_rec = df.__len__() + 1

        if df.__len__() < target_period + comparing_window:
            return (df_result, i_result, df)

        if live_trading:
            start_rec = df.__len__()

        for i in range(df.__len__() - target_period, df.__len__() + 1):
            # if debug:
            # print "loop #"+str(loop_num)+", "+ str(i) + " of " + str(df.__len__() + 1)
            # if i == 14
            #    pass

            #start_day = i - pre_days
            #if start_day < 10:
            #    #start_day = 0
            #    continue

            #ds_n_days = df.iloc[start_day:i]  #
            ds_n_days = df.iloc[i - comparing_window:i]  #

            open = np.array(ds_n_days['open'], dtype=float)
            high = np.array(ds_n_days['high'], dtype=float)
            low = np.array(ds_n_days['low'], dtype=float)
            close = np.array(ds_n_days['close'], dtype=float)
            volume = np.array(ds_n_days['volume'], dtype=float)

            use_shared_eval = True
            if True:
                if target == 'mfi':
                    cmd = "talib." + target.upper() + "(high, low, close, volume, timeperiod=target_period)"
                elif target == 'rsi':
                    cmd = "talib." + target.upper() + "(close, timeperiod=target_period)"
                elif target == 'natr':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=target_period)"
                elif target == 'tema':
                    cmd = "talib." + target.upper() + "(close, timeperiod=target_period)"
                elif target == 'trima':
                    cmd = "talib." + target.upper() + "(close, timeperiod=target_period)"

                elif target == 'adx':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=14)"
                elif target == 'adxr':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=14)"
                elif target == 'apo':
                    cmd = "talib." + target.upper() + "(close, fastperiod=12, slowperiod=26, matype=0)"
                elif target == 'aroon':
                    cmd = "talib.AROON(high, low, timeperiod=14)"
                    aroondown, aroonup = eval(cmd)
                    target_n_days = aroondown
                    use_shared_eval = False

                elif target == 'aroonosc':
                    cmd = "talib." + target.upper() + "(high, low, timeperiod=14)"
                elif target == 'bop':
                    cmd = "talib." + target.upper() + "(open, high, low, close)"
                elif target == 'cci':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=14)"
                elif target == 'cmo':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"
                elif target == 'dx':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=14)"
                elif target == 'minusdi':
                    cmd = "talib.MINUS_DI(high, low, close, timeperiod=14)"
                elif target == 'minusdm':
                    cmd = "talib.MINUS_DM(high, low, timeperiod=14)"
                elif target == 'mom':
                    cmd = "talib." + target.upper() + "(close, timeperiod=10)"
                elif target == 'plusdi':
                    cmd = "talib.PLUS_DI(high, low, close, timeperiod=14)"
                elif target == 'plusdm':
                    cmd = "talib.PLUS_DM(high, low, timeperiod=14)"
                elif target == 'ppo':
                    cmd = "talib." + target.upper() + "(close, fastperiod=12, slowperiod=26, matype=0)"
                elif target == 'roc':
                    cmd = "talib." + target.upper() + "(close, timeperiod=10)"
                elif target == 'rocp':
                    cmd = "talib." + target.upper() + "(close, timeperiod=10)"
                elif target == 'rocr':
                    cmd = "talib." + target.upper() + "(close, timeperiod=10)"
                elif target == 'rocr100':
                    cmd = "talib." + target.upper() + "(close, timeperiod=10)"
                elif target == 'trix':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"
                elif target == 'ultosc':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)"
                elif target == 'willr':
                    cmd = "talib." + target.upper() + "(high, low, close, timeperiod=14)"

                elif target == 'macd':
                    cmd = "talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)"
                    macd, macdsignal, macdhist = eval(cmd)
                    target_n_days = macd
                    use_shared_eval = False

                elif target == 'macdext':
                    cmd = "talib.MACDEXT(close, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)"
                    macd, macdsignal, macdhist = eval(cmd)
                    target_n_days = macd
                    use_shared_eval = False

                elif target == 'macdfix':
                    cmd = "talib.MACDFIX(close, signalperiod=9)"
                    macd, macdsignal, macdhist = eval(cmd)
                    target_n_days = macd
                    use_shared_eval = False

                elif target == 'ad':
                    cmd = "talib." + target.upper() + "(high, low, close, volume)"

                elif target == 'adosc':
                    cmd = "talib." + target.upper() + "(high, low, close, volume, fastperiod=3, slowperiod=10)"

                elif target == 'obv':
                    cmd = "talib." + target.upper() + "(close, volume)"

                elif target == 'avgprice':
                    cmd = "talib." + target.upper() + "(open, high, low, close)"

                elif target == 'medprice':
                    cmd = "talib." + target.upper() + "(high, low)"

                elif target == 'typprice':
                    cmd = "talib." + target.upper() + "(high, low, close)"

                elif target == 'wclprice':
                    cmd = "talib." + target.upper() + "(high, low, close)"

                elif target == 'ht_dcperiod':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'ht_dcphase':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'ht_phasor':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'ht_sine':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'ht_trendmode':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'beta':
                    cmd = "talib." + target.upper() + "(high, low, timeperiod=5)"

                elif target == 'correl':
                    cmd = "talib." + target.upper() + "(high, low, timeperiod=30)"

                elif target == 'linearreg':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                elif target == 'linearreg_angle':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                elif target == 'linearreg_intercept':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                elif target == 'linearreg_slope':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                elif target == 'stddev':
                    cmd = "talib." + target.upper() + "(close, timeperiod=5, nbdev=1)"

                elif target == 'tsf':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                elif target == 'var':
                    cmd = "talib." + target.upper() + "(close, timeperiod=5, nbdev=1)"

                elif target == 'wma':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"

                elif target == 't3':
                    cmd = "talib." + target.upper() + "(close, timeperiod=5, vfactor=0)"

                elif target == 'sma':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"

                elif target == 'sarext':
                    cmd = "talib." + target.upper() + "(high, low, startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)"

                elif target == 'sar':
                    cmd = "talib." + target.upper() + "(high, low, acceleration=0, maximum=0)"

                elif target == 'midprice':
                    cmd = "talib." + target.upper() + "(high, low, timeperiod=14)"

                elif target == 'midpoint':
                    cmd = "talib." + target.upper() + "(close, timeperiod=14)"

                # elif target == 'mavp':
                #    logging.info('mavp is not implemented yet')
                #    return
                # cmd = "talib." + target.upper() + "(close, periods=np.array([5.0,7.0]), minperiod=2, maxperiod=30, matype=0)"
                # talib.MAVP(np.array([ 8.31,  8.5]), periods=np.array([5.0,7.0]), minperiod=2, maxperiod=30, matype=0)
                # array([ nan,  nan])

                # talib.MAVP(np.array([ 8.31,  8.5 ,  8.53]), periods=np.array([5.0,7.0]), minperiod=2, maxperiod=30, matype=0)
                # Exception: input lengths are different

                elif target == 'mama':
                    # cmd = "talib." + target.upper() + "(close, fastlimit=0, slowlimit=0)" #Exception: TA_MAMA function failed with error code 2: Bad Parameter (TA_BAD_PARAM)

                    cmd = "talib." + target.upper() + "(close)"
                    mama, fama = eval(cmd)
                    target_n_days = mama
                    use_shared_eval = False

                elif target == 'ma':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30, matype=0)"

                elif target == 'kama':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"

                elif target == 'ht_trendline':
                    cmd = "talib." + target.upper() + "(close)"

                elif target == 'ema':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"

                elif target == 'dema':
                    cmd = "talib." + target.upper() + "(close, timeperiod=30)"

                elif target == 'bbands':
                    cmd = "talib." + target.upper() + "(close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)"
                    upperband, middleband, lowerband = eval(cmd)
                    target_n_days = middleband
                    use_shared_eval = False
                elif target == 'pv':
                    target_n_days = ds_n_days['volume']

                else:
                    logging.info(__file__+" "+"Unknown target, die at finlib.py.")
                    exit(0)

            #if use_shared_eval:
            if target != 'pv':
                if debug:
                    logging.info(__file__+" "+"running " + cmd)
                target_n_days = eval(cmd)

            # target_n_days = talib.MFI(high, low, close, volume, timeperiod=target_period)

            target_n_days = np.array(target_n_days)
            target_n_days_no_nan = target_n_days[np.logical_not(np.isnan(target_n_days))]  # remove nan

            if target_n_days_no_nan.__len__() < 1:
                # if debug:
                #    logging.info(__file__+" "+"zero size "+target+"_n_days_no_nan")
                # return [0, ds_n_days.index[-1], code]
                continue

            if str(type(target_n_days[-1])) == "<type 'numpy.ndarray'>":
                # print "target " + target + " target_n_days[-1] is an array, I don't know how to handle this."
                # logging.info(str(target_n_days[-1]))
                continue

            close_max = target_max_close = close_min = target_min_close = close[0]
            target_max = target_min = close_max_target = close_min_target = target_n_days[0]

            time = ds_n_days['date'][-1:].values[0]
            close_p = ds_n_days['close'][-1:].values[0]

            for j in range(target_n_days.__len__()):

                # logging.info(str(type(target_n_days[j])))

                # if type(target_n_days[j]) == "<type 'numpy.ndarray'>":
                #    print "target " + target + " target_n_days[j] is an array, j is " + str(j)
                #    logging.info(str(target_n_days[j]))
                #    continue

                # if not type(target_n_days[j]) == "<type 'numpy.float64'>":
                #    print "target "+target +" target_n_days[j] is not single value, j is " + str(j)
                #    logging.info(str(target_n_days[j]))
                #    continue

                if np.isnan(target_n_days[j]):  # math.isnan give exception: TypeError: only length-1 arrays can be converted to Python scalars
                    continue

                if close[j] >= close_max:
                    close_max = close[j]  # max close
                    close_max_target = target_n_days[j]  # rsi value of the day_max_close

                if close[j] <= close_min:
                    close_min = close[j]
                    close_min_target = target_n_days[j]

                if target_n_days[j] >= target_max:
                    target_max = target_n_days[j]  # max rtarget_n_dayssi value, real
                    target_max_close = close[j]  # close value of the day_rsi_max

                if target_n_days[j] <= target_min:
                    target_min = target_n_days[j]
                    target_min_close = close[j]

            if (close[-1] >= close_max) and ((target_n_days[-1] - 0.99 * target_max) < 0):  # close_max_target == target_n_days[-1]
                if target_max_close == 0 or target_max == 0:
                    logging.info(__file__+" "+"target_max_close or target_max is zero.  Avoid the div by zero error.")
                    continue
                expected_target = target_max * close_max * 1.0 / target_max_close  # should be great then actual mfi
                op_strength = (expected_target - target_n_days[-1]) * 1.0 / target_max

                reason = code + "_S_" + target + "_div"  #

                df.iloc[i - 1, df.columns.get_loc('op')] += ";S"
                df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                df.iloc[i - 1, df.columns.get_loc('op_strength')] += "," + str(op_strength)  #
                # logging.info(__file__+" "+"code: " + str(code) + " Date:" + time \
                #      + " Sell Sig on "+ target +"_"+  str(target_period) + " divergence")

                if exam_date == time:
                    df_result.loc[i_result] = [time, code, 'S', reason, op_strength, close_p]
                    i_result += 1

            elif (close[-1] <= close_min) and (target_n_days[-1] - 1.01 * target_min) > 0:

                if target_min_close == 0 or target_min == 0:
                    logging.info(__file__+" "+"target_min_close or target_min is zero.  Avoid the div by zero error.")
                    continue

                expected_target = target_min * close[-1] * 1.0 / target_min_close  # should higher than today actual mfi
                op_strength = (target_n_days[-1] - expected_target) * 1.0 / target_min
                reason = code + "_B_" + target + "_div"  #

                df.iloc[i - 1, df.columns.get_loc('op')] += ";B"
                df.iloc[i - 1, df.columns.get_loc('op_rsn')] += ";" + reason
                df.iloc[i - 1, df.columns.get_loc('op_strength')] += "," + str(op_strength)

                # logging.info(__file__+" "+"code: " + str(code) + " Date:" + time \
                #      + " Buy Sig on "+target+"_" + str(target_period) + " divergence")

                if exam_date == time:
                    df_result.loc[i_result] = [time, code, 'B', reason, op_strength, close_p]
                    i_result += 1

            else:
                pass
                # delta = 0
                # return [0, ds_n_days.index[-1], code, delta]

        return (df_result, i_result, df)

    def create_or_update_ptn_perf_db_record(self, df, dict, code, day_cnt, cursor, cnx, db_tbl):

        for ptn_code in list(dict.keys()):
            ptn_dict = re.match("(.*)_(.*)", ptn_code).group(1)
            code_dict = re.match("(.*)_(.*)", ptn_code).group(2)

            if ptn_dict == 'pv_ignore':
                continue
                pass

            if code_dict != code:  # each process in pool only update her code in globe dict
                continue

            logging.info(ptn_code)

            # select the records
            # if forex:
            #    tbl="pattern_perf_forex"
            # else:
            #    tbl="pattern_perf"

            select_ptn_perf = ("SELECT * FROM `" + db_tbl + "` WHERE pattern=\'" + ptn_dict + "\'")
            logging.info(__file__+" "+"select_ptn_perf " + select_ptn_perf)
            cursor.execute(select_ptn_perf)  # mysql.connector.errors.InterfaceError: 2013: Lost connection to MySQL server during query
            record = cursor.fetchall()

            if (record.__len__() == 0):
                # no history record in the db, insert to tbl.
                # add_s_p_perf = ("INSERT INTO " + db_tbl + \

                add_s_p_perf = ("INSERT INTO " + db_tbl + \
                                " (stockID, pattern, date_s, date_e, trading_days, buy_signal_cnt, sell_signal_cnt, \
                                1mea,1med,1min,1max,1var,1skw,1kur,1uc,1dc, \
                                2mea,2med,2min,2max,2var,2skw,2kur,2uc,2dc, \
                                3mea,3med,3min,3max,3var,3skw,3kur,3uc,3dc, \
                                5mea,5med,5min,5max,5var,5skw,5kur,5uc,5dc, \
                                7mea,7med,7min,7max,7var,7skw,7kur,7uc,7dc, \
                                10mea,10med,10min,10max,10var,10skw,10kur,10uc,10dc, \
                                15mea,15med,15min,15max,15var,15skw,15kur,15uc,15dc, \
                                20mea,20med,20min,20max,20var,20skw,20kur,20uc,20dc, \
                                30mea,30med,30min,30max,30var,30skw,30kur,30uc,30dc, \
                                60mea,60med,60min,60max,60var,60skw,60kur,60uc,60dc, \
                                120mea,120med,120min,120max,120var,120skw,120kur,120uc,120dc, \
                                240mea,240med,240min,240max,240var,240skw,240kur,240uc,240dc \
                                ) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                         %s,%s,%s,%s,%s,%s,%s,%s,%s)"                                                                                                                                                                                                                                                                                    )

                tm_list = ['1', '2', '3', '5', '7', '10', '15', '20', '30', '60', '120', '240']  # RYAN RESUME
                tm_data_sql = []
                for tm in tm_list:
                    tm = str(tm)

                    for j in ['_mean', '_median', '_min', '_max', '_variance', '_skewness', '_kurtosis', '_upcnt', '_dncnt']:
                        cmd = 'dict[ptn_code][\'' + tm + j + '\']'
                        a = eval(cmd)

                        if str(a) == 'nan':
                            a = 0

                        tm_data_sql.append(a)
                        # a= str(eval(cmd))
                        # if a == 'nan':
                        #    a='0'
                        # tm_data_sql += str(a) + ","

                # tm_data_sql = tm_data_sql[:-1] #remove tail comma
                data_s_p_perf = (code_dict, ptn_dict, df['date'][0:1].values[0], df['date'][-1:].values[0], \
                                 day_cnt, dict[ptn_code]["buy_signal_cnt"], dict[ptn_code]["sell_signal_cnt"], \
                                 ) + tuple(tm_data_sql)
                # logging.info(__file__+" "+"add_s_p_perf "+add_s_p_perf)
                # logging.info(data_s_p_perf)
                cursor.execute(add_s_p_perf, data_s_p_perf)
                cnx.commit()
                logging.info(__file__+" "+"created new record, " + db_tbl + ", " + ptn_dict)
                pass  # END OF INSERT

            if (record.__len__() == 1):
                # read the history record, then update it to tbl_target(debug_zzz by default)

                (h_ID, h_stockID, h_pattern, h_date_s, h_date_e, h_trading_days, h_buy_signal_cnt, h_sell_signal_cnt, \
                 h_1mea, h_1med, h_1min, h_1max, h_1var, h_1skw, h_1kur, h_1uc, h_1dc, \
                 h_2mea, h_2med, h_2min, h_2max, h_2var, h_2skw, h_2kur, h_2uc, h_2dc, \
                 h_3mea, h_3med, h_3min, h_3max, h_3var, h_3skw, h_3kur, h_3uc, h_3dc, \
                 h_5mea, h_5med, h_5min, h_5max, h_5var, h_5skw, h_5kur, h_5uc, h_5dc, \
                 h_7mea, h_7med, h_7min, h_7max, h_7var, h_7skw, h_7kur, h_7uc, h_7dc, \
                 h_10mea, h_10med, h_10min, h_10max, h_10var, h_10skw, h_10kur, h_10uc, h_10dc, \
                 h_15mea, h_15med, h_15min, h_15max, h_15var, h_15skw, h_15kur, h_15uc, h_15dc, \
                 h_20mea, h_20med, h_20min, h_20max, h_20var, h_20skw, h_20kur, h_20uc, h_20dc, \
                 h_30mea, h_30med, h_30min, h_30max, h_30var, h_30skw, h_30kur, h_30uc, h_30dc, \
                 h_60mea, h_60med, h_60min, h_60max, h_60var, h_60skw, h_60kur, h_60uc, h_60dc, \
                 h_120mea, h_120med, h_120min, h_120max, h_120var, h_120skw, h_120kur, h_120uc, h_120dc, \
                 h_240mea, h_240med, h_240min, h_240max, h_240var, h_240skw, h_240kur, h_240uc, h_240dc \
                 ) = record[0]

                if h_buy_signal_cnt is None:
                    h_buy_signal_cnt = 0

                if h_sell_signal_cnt is None:
                    h_sell_signal_cnt = 0

                logging.info(__file__+" "+"update(merge) record, " + db_tbl + ", " + h_pattern)

                # if('XAUUSD_B_talib_CDLSEPARATINGLINES' == h_pattern):#debug
                #    pass

                st_dict = {'_mean': 'mea', '_median': "med", '_min': 'min', '_max': 'max', '_variance': 'var', '_skewness': 'skw', '_kurtosis': 'kur', '_upcnt': 'uc', '_dncnt': 'dc'}

                update_ptn_perf = ("UPDATE `" + db_tbl + "`  "
                                   "SET stockID = %(stockID)s, pattern = %(pattern)s, "
                                   " date_s = %(date_s)s, date_e = %(date_e)s, trading_days = %(trading_days)s,"
                                   " buy_signal_cnt = %(buy_signal_cnt)s, sell_signal_cnt = %(sell_signal_cnt)s, pattern = %(pattern)s, "
                                   " 1mea = %(1mea)s, 1med = %(1med)s, 1min = %(1min)s, 1max = %(1max)s, 1var = %(1var)s, 1skw = %(1skw)s, 1kur = %(1kur)s, 1uc = %(1uc)s, 1dc = %(1dc)s, "
                                   " 2mea = %(2mea)s, 2med = %(2med)s, 2min = %(2min)s, 2max = %(2max)s, 2var = %(2var)s, 2skw = %(2skw)s, 2kur = %(2kur)s, 2uc = %(2uc)s, 2dc = %(2dc)s, "
                                   " 3mea = %(3mea)s, 3med = %(3med)s, 3min = %(3min)s, 3max = %(3max)s, 3var = %(3var)s, 3skw = %(3skw)s, 3kur = %(3kur)s, 3uc = %(3uc)s, 3dc = %(3dc)s, "
                                   " 5mea = %(5mea)s, 5med = %(5med)s, 5min = %(5min)s, 5max = %(5max)s, 5var = %(5var)s, 5skw = %(5skw)s, 5kur = %(5kur)s, 5uc = %(5uc)s, 5dc = %(5dc)s, "
                                   " 7mea = %(7mea)s, 7med = %(7med)s, 7min = %(7min)s, 7max = %(7max)s, 7var = %(7var)s, 7skw = %(7skw)s, 7kur = %(7kur)s, 7uc = %(7uc)s, 7dc = %(7dc)s, "
                                   " 10mea = %(10mea)s, 10med = %(10med)s, 10min = %(10min)s, 10max = %(10max)s, 10var = %(10var)s, 10skw = %(10skw)s, 10kur = %(10kur)s, 10uc = %(10uc)s, 10dc = %(10dc)s, "
                                   " 15mea = %(15mea)s, 15med = %(15med)s, 15min = %(15min)s, 15max = %(15max)s, 15var = %(15var)s, 15skw = %(15skw)s, 15kur = %(15kur)s, 15uc = %(15uc)s, 15dc = %(15dc)s, "
                                   " 20mea = %(20mea)s, 20med = %(20med)s, 20min = %(20min)s, 20max = %(20max)s, 20var = %(20var)s, 20skw = %(20skw)s, 20kur = %(20kur)s, 20uc = %(20uc)s, 20dc = %(20dc)s, "
                                   " 30mea = %(30mea)s, 30med = %(30med)s, 30min = %(30min)s, 30max = %(30max)s, 30var = %(30var)s, 30skw = %(30skw)s, 30kur = %(30kur)s, 30uc = %(30uc)s, 30dc = %(30dc)s, "
                                   " 60mea = %(60mea)s, 60med = %(60med)s, 60min = %(60min)s, 60max = %(60max)s, 60var = %(60var)s, 60skw = %(60skw)s, 60kur = %(60kur)s, 60uc = %(60uc)s, 60dc = %(60dc)s, "
                                   " 120mea = %(120mea)s, 120med = %(120med)s, 120min = %(120min)s, 120max = %(120max)s, 120var = %(120var)s, 120skw = %(120skw)s, 120kur = %(120kur)s, 120uc = %(120uc)s, 120dc = %(120dc)s, "
                                   " 240mea = %(240mea)s, 240med = %(240med)s, 240min = %(240min)s, 240max = %(240max)s, 240var = %(240var)s, 240skw = %(240skw)s, 240kur = %(240kur)s, 240uc = %(240uc)s, 240dc = %(240dc)s "
                                   "WHERE pattern=%(pattern)s")

                data_ptn_perf = {}
                # data_ptn_perf = {'ID'}
                data_ptn_perf['stockID'] = code_dict
                data_ptn_perf['pattern'] = ptn_dict

                if df['date'][0:1].values[0] < h_date_s:
                    data_ptn_perf['date_s'] = str(df['date'][0:1].values[0])
                else:
                    data_ptn_perf['date_s'] = h_date_s

                if df['date'][-1:].values[0] > h_date_e:
                    data_ptn_perf['date_e'] = str(df['date'][-1:].values[0])
                else:
                    data_ptn_perf['date_e'] = h_date_e

                data_ptn_perf['trading_days'] = day_cnt + h_trading_days
                data_ptn_perf['buy_signal_cnt'] = h_buy_signal_cnt + dict[ptn_code]["buy_signal_cnt"]
                data_ptn_perf['sell_signal_cnt'] = h_sell_signal_cnt + dict[ptn_code]["sell_signal_cnt"]

                new_ptn_hit_cnt = dict[ptn_code]["buy_signal_cnt"] + dict[ptn_code]["sell_signal_cnt"]
                his_ptn_hit_cnt = h_buy_signal_cnt + h_sell_signal_cnt

                tm_list = ['1', '2', '3', '5', '7', '10', '15', '20', '30', '60', '120', '240']  # RYAN RESUME
                tm_data_sql = []
                for tm in tm_list:
                    tm = str(tm)

                    # up_cnt
                    his_uc = eval('h_' + tm + st_dict['_upcnt'])
                    this_uc = eval('dict[ptn_code][\'' + tm + '_upcnt' + '\']')

                    if str(his_uc) == 'nan' or str(his_uc) == '' or (his_uc is None):
                        his_uc = 0

                    if str(this_uc) == 'nan' or str(this_uc) == '' or (this_uc is None):
                        this_uc = 0

                    data_ptn_perf[tm + st_dict['_upcnt']] = his_uc + this_uc

                    # dn_cnt
                    his_dc = eval('h_' + tm + st_dict['_dncnt'])
                    this_dc = eval('dict[ptn_code][\'' + tm + '_dncnt' + '\']')

                    if str(his_dc) == 'nan' or str(his_dc) == '' or (his_dc is None):
                        his_dc = 0

                    if str(this_dc) == 'nan' or str(this_dc) == '' or (this_dc is None):
                        this_dc = 0

                    data_ptn_perf[tm + st_dict['_dncnt']] = his_dc + this_dc

                    # for j in ['_mean', '_median', '_min', '_max', '_variance', '_skewness', '_kurtosis', '_upcnt','_dncnt']:
                    for j in ['_mean', '_median', '_min', '_max', '_variance', '_skewness', '_kurtosis']:
                        cmd = 'dict[ptn_code][\'' + tm + j + '\']'

                        a = eval(cmd)

                        if str(a) == 'nan' or str(a) == '':
                            a = 0

                        history_value = eval('h_' + tm + st_dict[j])  # 'h_1mea'

                        try:
                            avg_value = (a * new_ptn_hit_cnt + history_value * his_ptn_hit_cnt) * 1.0 / (new_ptn_hit_cnt + his_ptn_hit_cnt)
                        except:
                            logging.info(sys.exc_info()[0])

                        data_ptn_perf[tm + st_dict[j]] = avg_value  # data_ptn_perf['1mea']=0.00018

                        # tm_data_sql.append(a)
                        # a= str(eval(cmd))
                        # if a == 'nan':
                        #    a='0'
                        # tm_data_sql += str(a) + ","
                logging.info(update_ptn_perf)
                cursor.execute(update_ptn_perf, data_ptn_perf)
                cnx.commit()

    def is_on_market(self, ts_code, date, basic_df):
        # basic_df passed from invoker, to avoid load csv everytime.
        # self.get_ts_field(ts_code = ts_code, field = )

        if basic_df is None:
            basic_df = self.get_today_stock_basic()


        list_date_df = basic_df.query("ts_code==\'" + ts_code + "\'")

        if not list_date_df.empty:
            list_date = list_date_df['list_date'].iloc[0]
            year = re.match("(\d{4})\d{2}\d{2}", str(list_date)).group(1)
            earlist_report_period = year + "1231"
            if date < earlist_report_period:
                # logging.info(__file__+" "+"stock has not been on market. "+ts_code + " , "+date+" . Earliest on market report "+earlist_report_period)
                return (False)
            else:
                # logging.info(__file__+" "+"stock has been on market. "+ts_code + " , "+date+" . Earliest on market report "+earlist_report_period)
                return (True)
        else:
            logging.info(__file__+" "+"do not have on-market date for code " + ts_code)
            return (False)

    def file_verify(self, file_path, day=3, hide_pass=False, print_len=True):

        rem = re.match("(.*\/)\*\.(.*)", file_path)

        if rem:
            root_dir = rem.group(1)
            file_ext = rem.group(2)

            allFiles = glob.glob(root_dir + "/*." + file_ext)

            for f in allFiles:
                self._file_verify(f, day=day, hide_pass=hide_pass, print_len=print_len)

        else:
            self._file_verify(file_path, day=day, hide_pass=hide_pass, print_len=print_len)

    def _file_verify(self, file_path, day=3, hide_pass=False, print_len=True):
        # print(". "+file_path)

        if not os.path.exists(file_path):
            print("exist F, update F, " + file_path)
            return ({"exist": False, "update": False})

        flen = "na"

        if print_len and re.match(".*\.csv", file_path):

            try:
                flen = str(pd.read_csv(file_path, encoding="utf-8", dtype=str).__len__())
            except:
                print("exception when reading : " + file_path)
                print(sys.exc_info())
                # sys.exc_clear() #not supported by python3
                # print(sys.exc_traceback)

        string_expected_not_update_or_not = ""

        rem = re.match(".*_(\d{4}_\d)\.csv", file_path)  # fundamental_peg_2018_4.csv
        if rem:
            file_content_date = rem.group(1)
            year = self.get_report_publish_status()['completed_quarter_year']  # '2018
            quarter = self.get_report_publish_status()['completed_quarter_number']  # '3'

            if (file_content_date < year + "_" + quarter):
                # don't expect the file updated in 3 days
                string_expected_not_update_or_not = " expected"
                pass
            else:
                string_expected_not_update_or_not = " unexpected"

        rem = re.match(".*_(\d{8})\.csv", file_path)
        if rem:
            file_content_date = rem.group(1)
            # d = self.get_report_publish_status()['completed_year_rpt_date']
            d = self.get_year_month_quarter()['fetch_most_recent_report_perid'][0]

            if (file_content_date < d):
                # don't expect the file updated in 3 days
                string_expected_not_update_or_not = " expected"
                pass
            else:
                string_expected_not_update_or_not = " unexpected"

        file_time = datetime.fromtimestamp(os.path.getctime(file_path))
        current_time = datetime.now()
        file_age = (current_time - file_time).total_seconds()

        # if file_age > 86400:
        if file_age > day * 24 * 3600:
            if hide_pass and string_expected_not_update_or_not == " expected":
                pass
            else:
                print("exist T, update F" + string_expected_not_update_or_not + ", len " + flen + " " + file_path)

            return ({"exist": True, "update": False})
        else:
            if not hide_pass:
                print("exist T, update T, len " + flen + " " + file_path)
            return ({"exist": True, "update": True})

    def is_cached(self, file_path, day=1):
        '''
        copied from /home/ryan/anaconda2/lib/python2.7/site-packages/finsymbols/symbol_helper.py
        Checks if the file cached is still valid
        '''

        if not os.path.exists(file_path):
            return False


        #print(os.stat(file_path).st_size)
        if os.stat(file_path).st_size <= 300: #not work
            return False

        file_time = datetime.fromtimestamp(os.path.getctime(file_path))
        current_time = datetime.now()
        file_age = (current_time - file_time).total_seconds()

        # if file_age > 86400:
        if file_age > day * 24 * 3600:
            return False
        else:
            return True

    def get_code_format(self, code_input):
        rem_D6DotC2 = re.match("(\d{6})\.(.*)", code_input)  # 600519.SH
        rem_C2D6 = re.match("([a-zA-Z]{2})(\d{6})", code_input)  # SH600519
        rem_D6 = re.match("^(\d{6})$", code_input)  # 600519

        if rem_D6DotC2:
            code = rem_D6DotC2.group(1)
            mkt = rem_D6DotC2.group(2)
            code_format = "D6.C2"

        if rem_C2D6:
            code = rem_C2D6.group(2)
            mkt = rem_C2D6.group(1)
            code_format = "C2D6"

        if rem_D6:
            code = rem_D6.group(1)
            mkt = 'NA'
            code_format = "D6"

        dict = {'code': code, 'mkt': mkt, 'format': code_format}

        dict['D6.C2'] = dict['code'] + "." + dict['mkt']
        dict['C2D6'] = dict['mkt'] + dict['code']
        dict['D6'] = dict['code']

        return (dict)

    def get_report_publish_status(self):
        tmp = self.get_year_month_quarter()
        m = tmp['month']

        this_year = tmp['year']
        last_year = tmp['year'] - 1
        last_two_year = tmp['year'] - 2

        rtn = {}
        lst = []
        rtn['period_to_be_checked_lst'] = lst
        rtn['process_fund_or_not'] = True

        if m == 1 or m == 2 or m == 3:
            # 年报：明年1月中旬起至4月底要公布完毕。每年1月1日——4月30日。
            # ann_date_1q_before = ann_date_1y_before

            rtn['year_report'] = 'publishing'
            rtn['quarter_1_report'] = 'not_start'
            rtn['half_year_report'] = 'not_start'
            rtn['quarter_3_report'] = 'not_start'

            rtn['completed_year_rpt_date'] = str(last_two_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(last_year) + "0630"
            rtn['completed_quarter_date'] = str(last_year) + "0930"

            rtn['completed_quarter_year'] = str(last_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "3"

            lst.append(tmp['ann_date_1y_before'])

        elif m == 4:
            # 即第一季报在四月份
            # 一季报：4月底要公布完毕。每年4月1日——4月30日
            #  年报：明年1月中旬起至4月底要公布完毕。每年1月1日——4月30日。
            # 1q_before =0331, 1y_before= 1231

            rtn['year_report'] = 'publishing'
            rtn['quarter_1_report'] = 'publishing'
            rtn['half_year_report'] = 'not_start'
            rtn['quarter_3_report'] = 'not_start'

            lst.append(tmp['ann_date_1q_before'])
            lst.append(tmp['ann_date_1y_before'])

            # rtn['completed_year_rpt_date']= str(last_two_year)+"1231"
            # at month 4th, we have mixed data of this year and last year. choose using this year.
            # suppose most company have published last year and this Q1 in Apri.
            rtn['completed_year_rpt_date'] = str(last_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(last_year) + "0630"
            rtn['completed_quarter_date'] = str(this_year) + "0331"

            rtn['completed_quarter_year'] = str(last_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "4"

        elif m == 5 or m == 6:
            rtn['year_report'] = 'published'
            rtn['quarter_1_report'] = 'published'
            rtn['half_year_report'] = 'not_start'
            rtn['quarter_3_report'] = 'not_start'

            rtn['completed_year_rpt_date'] = str(last_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(last_year) + "0630"
            rtn['completed_quarter_date'] = str(this_year) + "0331"

            rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "1"
            #rtn['process_fund_or_not'] = False #alway fetch

            # lst.append(tmp['ann_date_1q_before']) #<<< better be empty? comment is to empty.

        elif m == 7 or m == 8:
            # 半年报：7月起至8月底公布完毕。每年7月1日——8月30日。
            # 中期报告由上市公司在半年度结束后两个月内完成（即七、八月份）
            rtn['year_report'] = 'published'
            rtn['quarter_1_report'] = 'published'
            rtn['half_year_report'] = 'publishing'
            rtn['quarter_3_report'] = 'not_start'

            lst.append(tmp['ann_date_1q_before'])

            rtn['completed_year_rpt_date'] = str(last_year) + "1231"

            if m == 7:
                rtn['completed_half_year_rpt_date'] = str(last_year) + "0630"
                rtn['completed_quarter_date'] = str(this_year) + "0331"
                rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
                rtn['completed_quarter_number'] = "1"

            elif m == 8:
                rtn['completed_half_year_rpt_date'] = str(this_year) + "0630"
                rtn['completed_quarter_date'] = str(this_year) + "0630"
                rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
                rtn['completed_quarter_number'] = "2"  # even not all stocks have Q2 report this time.

        elif m == 9:
            rtn['year_report'] = 'published'
            rtn['quarter_1_report'] = 'published'
            rtn['half_year_report'] = 'published'
            rtn['quarter_3_report'] = 'not_start'

            # lst.append(tmp['ann_date_1q_before']) #<<< better be empty? comment is to empty.

            rtn['completed_year_rpt_date'] = str(last_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(this_year) + "0630"
            rtn['completed_quarter_date'] = str(this_year) + "0630"

            rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "2"
            #rtn['process_fund_or_not'] = False  # always fetch.

        elif m == 10:
            # 第三季报在十月份  每年10月1日——10月31日
            rtn['year_report'] = 'published'
            rtn['quarter_1_report'] = 'published'
            rtn['half_year_report'] = 'published'
            rtn['quarter_3_report'] = 'publishing'

            lst.append(tmp['ann_date_1q_before'])

            rtn['completed_year_rpt_date'] = str(last_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(this_year) + "0630"
            rtn['completed_quarter_date'] = str(this_year) + "0630"
            rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "2"

        elif m == 11 or m == 12:
            rtn['year_report'] = 'published'
            rtn['quarter_1_report'] = 'published'
            rtn['half_year_report'] = 'published'
            rtn['quarter_3_report'] = 'published'

            # lst.append(tmp['ann_date_1q_before']) #<<< better be empty? comment is to empty.

            rtn['completed_year_rpt_date'] = str(last_year) + "1231"
            rtn['completed_half_year_rpt_date'] = str(this_year) + "0630"
            rtn['completed_quarter_date'] = str(this_year) + "0930"
            rtn['completed_quarter_year'] = str(this_year)  # using by t_daily_fundamentals.py
            rtn['completed_quarter_number'] = "3"
            #rtn['process_fund_or_not'] = False #aways fetch

        #rtn['process_fund_or_not'] = True  # ryan debug. should be remove on production

        return (rtn)

    def prime_stock_list(self):
        csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step6/multiple_years_score_selected.csv'
        logging.info(__file__+" "+"loading , " + csv)
        if (os.path.isfile(csv)) and os.stat(csv).st_size >= 10:  # > 10 bytes
            df = pd.read_csv(csv, encoding="utf-8")
        else:
            logging.error("no such file " + csv)
            exit()
        return(df)

    def remove_garbage(self, df, code_field_name='code', code_format='C2D6',b_m_score=-1,n_year=5):
        df = self._remove_garbage_must(df,b_m_score,n_year)
        df = self._remove_garbage_rpt_s1(df, code_field_name, code_format)
        df = self._remove_garbage_by_profit_on_market_days_st(df)
        return(df)


    def evaluate_by_ps_pe_pb(self):
        df_exam_all = self.get_common_fund_df()
        df_exam_all['evaluated_by'] = None

        # on_market_long years: PB
        df_exam_all.loc[df_exam_all['on_market_days'] > 10 * 365, ['evaluated_by']] = 'PE,PB'

        df_exam_all.loc[(df_exam_all['on_market_days'] < 10 * 365)&(df_exam_all['on_market_days'] > 5 * 365), ['evaluated_by']] = 'PE'

        # PS : currently unprofitable companies
        df_exam_all.loc[(df_exam_all['on_market_days'] < 5 * 365) & (df_exam_all['net_profit'] < 0), ['evaluated_by']] = 'PS'


        df_exam_all.loc[(df_exam_all['on_market_days'] < 5 * 365) & (df_exam_all['net_profit'] > 0), ['evaluated_by']] = 'PE,PS'

        # PB: banks and insurance companies, QuanShang.
        df_exam_all.loc[df_exam_all['industry'].str.contains("银行|证券|多元金融|保险"), ['evaluated_by']] = 'PB'
        return(df_exam_all)

    def _remove_garbage_by_profit_on_market_days_st(self,df):
        df_exam_all = self.get_common_fund_df()

        # on market 5 years, nagtive profit 2 years
        df_gar_1 = df_exam_all[(df_exam_all['net_profit'] < 0) & (df_exam_all['net_profit_year1'] < 0) & (
                    df_exam_all['on_market_days'] > 5 * 365)]
        df = self._df_sub_by_code(df=df,df_sub=df_gar_1, byreason=constant.NAG_PROFIT_RECENT_2_YEARS)

        # on market 5 years, debit more than assets
        df_gar_2 = df_exam_all[
            (df_exam_all['on_market_days'] > 5 * 365) & (df_exam_all['total_liab'] > df_exam_all['total_assets'])]
        df = self._df_sub_by_code(df=df, df_sub=df_gar_2, byreason=constant.DEBIT_GT_ASSETS)

        # ST
        df_gar_3 = df_exam_all[df_exam_all['name'].str.contains('ST')]
        df = self._df_sub_by_code(df=df, df_sub=df_gar_3, byreason=constant.ST_STOCK)

        return(df)

    def _remove_garbage_must(self, df, b_m_score=-1,n_year=5):
        if 'ts_code' in df.columns:
            ts_code_fmt = True
            df = self.ts_code_to_code(df)
        else:
            ts_code_fmt = False

        df = self._remove_garbage_beneish_low_rate(df,m_score=b_m_score)
        df = self._remove_garbage_change_named_stock(df,n_year=n_year)
        df = self._remove_garbage_none_standard_audit_statement(df,n_year=n_year)
        df = self._remove_garbage_high_pledge_ration(df,statistic_ratio_threshold=50, detail_ratio_sum_threshold=70)
        df = self._remove_garbage_low_roe_pe(df, market='AG', roe_pe_ratio_threshold=0.5)


        #remove koudi, this affected the fundermental_2.py step6.
        # df = self._remove_garbage_ma_up_koudi_gt_5(df, reason=constant.MA5_UP_KOUDI_DISTANCE_GT_5)

        if ts_code_fmt:
            df = self.remove_market_from_tscode(df)
            df = self.add_market_to_code(df=df,dot_f=True,tspro_format=True)

            if 'code' in df.columns:
                df = df.drop('code', axis=1)

        return(df)


    def _remove_garbage_rpt_s1(self, df, code_field_name, code_format):
        # code_field_name in code, ts_code
        # code_format in "D6.C2", "C2D6"
        if 'ts_code' in df.columns:
            ts_code_fmt = True
            df = self.ts_code_to_code(df)
        else:
            ts_code_fmt = False

        df_init_len = df.__len__()

        if "level_0" in df.columns:
            df = df.drop('level_0', axis=1)

        if "index" in df.columns:
            df = df.drop('index', axis=1)

        if "Unnamed: 0" in df.columns:
            df = df.drop('Unnamed: 0', axis=1)

        stable_report_perid = self.get_year_month_quarter()['stable_report_perid']

        f = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step1/rpt_" + stable_report_perid + ".csv"

        if not os.path.exists(f):
            logging.warning("remove garbage fail, file not exists "+f)
            return(df)

        df_garbage = pd.read_csv(f, converters={'end_date': str})
        df_garbage = df_garbage[['stopProcess', 'ts_code', 'name', 'end_date']]
        df_garbage = df_garbage[df_garbage['stopProcess'] == 1].reset_index().drop('index', axis=1)
        df_garbage = self.ts_code_to_code(df=df_garbage)

        df = self._df_sub_by_code(df=df, df_sub=df_garbage, byreason=constant.STOP_PROCESS)

        if ts_code_fmt:
            df = self.remove_market_from_tscode(df)
            df = self.add_market_to_code(df=df,dot_f=True,tspro_format=True)
            df = df.rename(columns={"code": "ts_code"}, inplace=False)

        df = df.reset_index().drop('index', axis=1)
        return (df)

        #
        # garbage_cnt = df_garbage.__len__()
        #
        # # determin input df code format
        #
        # for i in range(garbage_cnt):
        #     garbage_ts_code = df_garbage.iloc[i]
        #     garbage_ts_code = garbage_ts_code['ts_code']  # 600519.SH
        #
        #     dict = self.get_code_format(garbage_ts_code)
        #
        #     code_target = dict[code_format]
        #     df = df[df[code_field_name] != code_target]



    #input: df['code',...]
    #output: df that without the low rated benish score
    def _remove_garbage_beneish_low_rate(self, df, m_score):
        beneish_csv = '/home/ryan/DATA/result/ag_beneish.csv'
        # ts_code,name,ann_date,M_8v,M_5v,DSRI,GMI,AQI,SGI,DEPI,SGAI,TATA,LVGI
        df_gar = pd.read_csv(beneish_csv, converters={'ann_date': str})
        df_gar = df_gar[df_gar['M_8v'] >= m_score]

        if df.__len__()==0:
            logging.warning("empty df")
            return(df)

        if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
            df = self.add_market_to_code(df)

        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.BENEISH_LOW_RATE)

        return(df)


    #input: df['code',...]
    def _remove_garbage_change_named_stock(self, df, n_year=5):

        if df.__len__()==0:
            logging.warning("empty df")
            return(df)


        csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/changed_name_stocks.csv'
        df_gar = pd.read_csv(csv, converters={'start_date': str})


        df_gar = df_gar[df_gar['start_date'] > (datetime.today() - timedelta(days=n_year * 365)).strftime("%Y%m%d")]
        df_gar = df_gar[df_gar['change_reason'] != "其他"]
        df_gar = self.ts_code_to_code(df_gar)
        df_gar = pd.DataFrame(df_gar['code'].drop_duplicates()).reset_index().drop('index', axis=1)
        logging.info("Shares changed name within " + str(n_year) + " years, len " + str(df_gar.__len__()))

        if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
            df = self.add_market_to_code(df)

        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.STOCK_CHANGED_NAME)

        return(df)


    #input: df['code',...]
    def _remove_garbage_ma_up_koudi_gt_5(self,df, reason=constant.MA5_UP_KOUDI_DISTANCE_GT_5):
        if df.__len__()==0:
            logging.warning("empty df")
            return(df)

        df_gar = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.MA5_UP_KOUDI_DISTANCE_GT_5)
        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=reason)

        return(df)

    # roe_pe_ratio_threshold the higher the striker
    def _remove_garbage_low_roe_pe(self, df, market='AG', roe_pe_ratio_threshold=1):
        df = self.get_roe_div_pe(market=market)
        df_gar = df[df['pe_ttm'] <= 0]
        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.NAGTIVE_OR_ZERO_PE)
        
        df_gar = df[df['roe_pe'] < roe_pe_ratio_threshold]
        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.LOW_ROE_PE_RATIO)
        return(df)




    def _remove_garbage_none_standard_audit_statement(self, df, n_year=5):
        if df.__len__()==0:
            logging.warning("empty df")
            return(df)

        csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_audit.csv'
        df_gar = pd.read_csv(csv, converters={'ann_date': str})

        df_gar = df_gar[df_gar['ann_date'] > (datetime.today() - timedelta(days=n_year * 365)).strftime("%Y%m%d")]
        df_gar = df_gar[df_gar['audit_result'] != "标准无保留意见"]
        df_gar = self.ts_code_to_code(df_gar)
        df_gar = pd.DataFrame(df_gar['code'].drop_duplicates()).reset_index().drop('index', axis=1)
        logging.info("none standard audit statement " + str(n_year) + " years, len " + str(df_gar.__len__()))

        if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
            df = self.add_market_to_code(df)

        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.NONE_STANDARD_AUDIT_REPORT)

        return(df)


    def _remove_garbage_high_pledge_ration(self, df, statistic_ratio_threshold=50, detail_ratio_sum_threshold=50):
        if df.__len__()==0:
            logging.warning("empty df")
            return(df)

        csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/pledge/pledge_stat.csv'
        csv_detail = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/pledge/pledge_detail.csv'
        df_gar = pd.read_csv(csv, converters={'end_date': str})
        df_gar_detail = pd.read_csv(csv_detail)

        df_gar = df_gar[df_gar['pledge_ratio'] >= statistic_ratio_threshold]
        df_gar_detail = df_gar_detail[df_gar_detail['p_total_ratio_sum'] >= detail_ratio_sum_threshold]

        # df_gar = self.ts_code_to_code(df_gar)
        # df_gar = pd.DataFrame(df_gar['code'].drop_duplicates()).reset_index().drop('index', axis=1)
        # df_gar = pd.DataFrame(df_gar['code'].drop_duplicates()).reset_index().drop('index', axis=1)
        logging.info("pledge static: pledge_ration >= " + str(statistic_ratio_threshold) + ", len " + str(df_gar.__len__()))
        logging.info("pledge detail: p_total_ratio_sum >= " + str(detail_ratio_sum_threshold) + ", len " + str(df_gar_detail.__len__()))

        if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
            df = self.add_market_to_code(df)

        df = self._df_sub_by_code(df=df, df_sub=df_gar, byreason=constant.PLEDGE_STATISTIC_RATIO_GT_THRESHOLD)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_detail, byreason=constant.PLEDGE_DETAIL_RATIO_SUM_GT_THRESHOLD)

        return(df)


    def remove_garbage_macd_ma(self,df):
        if df.empty:
            logging.info("df is empty, not to remove macd ma garbage")
            return(df)

        df_gar_1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.CLOSE_UNDER_SMA60, period='D')
        df_gar_2 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.DIF_LT_0, period='D')
        df_gar_3 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.SIG_LT_0, period='D')
        df_gar_4 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.DIF_LT_SIG, period='D')
        df_gar_5 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.SMA21_UNDER_SMA60, period='D')

        df_gar_6 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.VERY_STONG_DOWN_TREND,
                                                                               market='ag', selected=False)
        df_gar_7 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.SELL_MUST, period='D',
                                                                               market='ag')

        if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
            df = self.add_market_to_code(df)

        df = self._df_sub_by_code(df=df, df_sub=df_gar_1,byreason=constant.CLOSE_UNDER_SMA60)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_2,byreason=constant.DIF_LT_0)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_3,byreason=constant.SIG_LT_0)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_4,byreason=constant.DIF_LT_SIG)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_5,byreason=constant.SMA21_UNDER_SMA60)

        df = self._df_sub_by_code(df=df, df_sub=df_gar_6,byreason=constant.VERY_STONG_DOWN_TREND)
        df = self._df_sub_by_code(df=df, df_sub=df_gar_7,byreason=constant.SELL_MUST)
        return(df)


    # def remove_garbage_junxian_barstyle(self,df):
    #     dir = '/home/ryan/DATA/result'
    #     file = dir+'./ag_junxian_barstyle_very_strong_down_trend.csv'
    #
    #
    #     df_gar_1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.CLOSE_UNDER_SMA60, period='D')
    #     df_gar_2 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.DIF_LT_0, period='D')
    #     df_gar_3 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.SIG_LT_0, period='D')
    #     df_gar_4 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.DIF_LT_SIG, period='D')
    #     df_gar_5 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.SMA21_UNDER_SMA60, period='D')
    #
    #     if self.get_code_format(code_input=df['code'].iloc[0])['format'] == 'D6':
    #         df = self.add_market_to_code(df)
    #
    #     df = self._df_sub_by_code(df=df, df_sub=df_gar_1,byreason=constant.CLOSE_UNDER_SMA60)
    #     df = self._df_sub_by_code(df=df, df_sub=df_gar_2,byreason=constant.DIF_LT_0)
    #     df = self._df_sub_by_code(df=df, df_sub=df_gar_3,byreason=constant.SIG_LT_0)
    #     df = self._df_sub_by_code(df=df, df_sub=df_gar_4,byreason=constant.DIF_LT_SIG)
    #     df = self._df_sub_by_code(df=df, df_sub=df_gar_5,byreason=constant.SMA21_UNDER_SMA60)
    #     return(df)
    #


    def _df_sub_by_code(self,df,df_sub,byreason=''):
        if df.__len__() == 0:
            logging.info("input df is empty, not able to deduct df_sub.")
            return(df)

        df_init_len = df.__len__()

        s_all = df['code'].drop_duplicates().reset_index().drop('index', axis=1)['code']
        s_sub = df_sub['code'].drop_duplicates().reset_index().drop('index', axis=1)['code']

        s_rst = s_all[~s_all.isin(s_sub)]  # remove s_sub from s_all

        df = df[df['code'].isin(s_rst)].drop_duplicates()

        if 'level_0' in df.columns:
            df = df.drop('level_0', axis=1)

        df = df.reset_index().drop('index', axis=1)

        df_len = df.__len__()
        logging.info(str(df_init_len)+"->"+str(df_len)+", "+str(df_init_len - df_len) + " shares were removed by "+byreason)
        return(df)


    # convert daily df to monthly df with resample/reshape
    def daily_to_monthly_bar(self, df_daily):

        df_daily = self.regular_column_names(df_daily)

        #df_daily['date'] = pd.to_datetime(df_daily['date'], format="%Y-%m-%d")#ryan commented 20201120
        df_daily['date'] = pd.to_datetime(df_daily['date'], format="%Y%m%d") #ryan added 20201120

        # df_daily = df_daily.reset_index().set_index('date')

        code = df_daily['code'].iloc[0]

        logic_sp500_index = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }

        logic_us = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }


        #### /home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv
        # code,date,close,open,high,low,pre_close,change,pct_chg,volume,amount

        logic_ag_index = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',

            'pre_close': 'last',
            'change': 'mean',
            'pct_chg': 'mean',
        }

        #### /home/ryan/DATA/DAY_Global/AG/*.csv
        logic_ag = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',
            'tnv': 'mean',
        }

        if code == 'SP500' or code =='NASDAQ100':
            logic=logic_sp500_index
        elif ('pct_chg' in df_daily.columns):
            logic = logic_ag_index
            logging.info("found pct_chg in columns, suppose this is AG_INDEX. ")
        elif (re.match(r'S[H|Z]\d{6}',code)):
            logic = logic_ag
            logging.info("detected AG Indivial stocks, code "+code)

        elif (re.match(r'^\w+',code)):
            logic = logic_us
            logging.info("detected US Indivial stocks, code "+code)
        else:
            logic = logic_ag
            logging.info("Not found pct_chg in columns, suppose this is AG Indivial stocks. ")


#https://stackoverflow.com/questions/34597926/converting-daily-stock-data-to-weekly-based-via-pandas-in-python
        #-2 to Friday 2020-11-20.  ignore to next Sunday 2020-11-22.  -6 to Monday 2020-11-16.
        # df_weekly = df_daily.resample('W', on='date',loffset=pd.offsets.timedelta(days=-2)).apply(logic).reset_index()
        df_weekly = df_daily.resample('W', on='date',loffset=timedelta(days=-2)).apply(logic).reset_index()

        # did not get it work.   FutureWarning: 'loffset' in .resample() and in Grouper() is deprecated.
        # from pandas.tseries.frequencies import to_offset
        # df_weekly = df_daily.resample('W', on='date').apply(logic).reset_index()
        # df_weekly.index = df_weekly.index.to_timestamp() + to_offset("-2D")

        df_weekly['code'] = code
        df_weekly  = self.change_df_columns_order(df=df_weekly, col_list_to_head=['code'])
        # print("\n\n df_daily 2")
        # print(df_daily.iloc[-1])
        #
        # print("\n\n df_weekly")
        # print(df_weekly.iloc[-1])

        #ignore to last day of the month. 2020-11-30
        df_monthly = df_daily.resample('M', on='date').apply(logic).reset_index()
        df_monthly['code'] = code
        df_monthly  = self.change_df_columns_order(df=df_monthly, col_list_to_head=['code'])

        # print("\n\n df_monthly")
        # print(df_monthly.iloc[-1])

        rtn = {
            'df_weekly': df_weekly,
            'df_monthly': df_monthly,
        }

        return (rtn)

    # rename colums c->close, o->open...,  to compliant with stockstats

    # [HK/00001.csv] close,code,datetime,high,low,name,open,p_change,vol
    # [US/AAPL.csv]  code,datetime,open,high,low,close,vol,name
    # [AG/SH600519.csv] 代码,时间,开盘价,最高价,最低价,收盘价,成交量(股),成交额(元),换手率

    def regular_column_names(self, df):
        if 'o' in df.columns:
            df.rename(columns={"o": "open"}, inplace=True)

        if 'h' in df.columns:
            df.rename(columns={"h": "high"}, inplace=True)

        if 'c' in df.columns:
            df.rename(columns={"c": "close"}, inplace=True)

        if 'l' in df.columns:
            df.rename(columns={"l": "low"}, inplace=True)

        if 'vol' in df.columns:
            df.rename(columns={"vol": "volume"}, inplace=True)

        if 'amt' in df.columns:
            df.rename(columns={"amt": "amount"}, inplace=True)

        if 'datetime' in df.columns:
            df.rename(columns={"datetime": "date"}, inplace=True)

        if 'Code' in df.columns:
            df.rename(columns={"Code": "code"}, inplace=True)
        if 'Date' in df.columns:
            df.rename(columns={"Date": "date"}, inplace=True)
        if 'Open' in df.columns:
            df.rename(columns={"Open": "open"}, inplace=True)
        if 'High' in df.columns:
            df.rename(columns={"High": "high"}, inplace=True)
        if 'Low' in df.columns:
            df.rename(columns={"Low": "low"}, inplace=True)
        if 'Close' in df.columns:
            df.rename(columns={"Close": "close"}, inplace=True)
        if 'Volume' in df.columns:
            df.rename(columns={"Volume": "volume"}, inplace=True)

        return (df)

    def regular_df_date_to_ymd(self, df):
        if 'date' not in df.columns:
            logging.fatal(__file__+" "+"No cloumn date in df")
            logging.warning(__file__+" "+str(df.head(2)))
            #self.pprint(df.head(2))

            #exit(0)
            return (df)

        if df.__len__() == 0:
            return ()

        if str(df['date'].iloc[0]).count("-") == 2:
            df['date'] = df['date'].apply(lambda _d: datetime.strptime(str(_d), '%Y-%m-%d'))
            df['date'] = df['date'].apply(lambda _d: _d.strftime('%Y%m%d'))
        elif str(df['date'].iloc[0]).count("-") == 0:
            df['date'] = df['date'].apply(lambda _d: str(_d))
        else:
            logging.fatal(__file__+" "+"unknown date format " + str(df['date'].iloc[0]))
            exit(0)

        return (df)

    def zzz_kdj(self, csv_f, market='AG'):
        # https://raw.githubusercontent.com/Abhay64/KDJ-Indicator/master/KDJ_Indicator.py

        df = pd.DataFrame()
        df_result = pd.DataFrame()
        result = {'K': [], 'D': [], 'J': []}

        if market == 'AG':
            # df = pd.read_csv('/home/ryan/DATA/DAY_Global/AG/SH600519.csv', converters={'code': str}, header=None, skiprows=1,
            #                          names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
            df = pd.read_csv(csv_f, converters={'code': str}, header=None, skiprows=1, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
        else:
            pass
        '''
        %K = (Current Close - Lowest Low)/(Highest High - Lowest Low) * 100
        %D = 3-day SMA of %K

        Lowest Low = lowest low for the look-back period
        Highest High = highest high for the look-back period
        %K is multiplied by 100 to move the decimal point two places
        '''

        # converting from UNIX timestamp to normal
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
        array_date = np.array(df['date'])
        array_close = np.array(df['close'])
        array_open = np.array(df['open'])
        array_high = np.array(df['high'])
        array_low = np.array(df['low'])
        array_volume = np.array(df['volume'])
        # print("High Array size", array_high.size)
        # print("Low Array size", array_low.size)
        # print("Open Array size", array_open.size)
        # print("Close Array size", array_close.size)
        y = 0
        z = 0
        # kperiods are 14 array start from 0 index
        kperiods = 13
        array_highest = []
        for x in range(0, array_high.size - kperiods):
            z = array_high[y]
            for j in range(0, kperiods):
                if (z < array_high[y + 1]):
                    z = array_high[y + 1]
                y = y + 1
            # creating list highest of k periods
            array_highest.append(z)
            y = y - (kperiods - 1)
        # print("Highest array size", len(array_highest))
        # print(array_highest)
        y = 0
        z = 0
        array_lowest = []
        for x in range(0, array_low.size - kperiods):
            z = array_low[y]
            for j in range(0, kperiods):
                if (z > array_low[y + 1]):
                    z = array_low[y + 1]
                y = y + 1
            # creating list lowest of k periods
            array_lowest.append(z)
            y = y - (kperiods - 1)
        # print(len(array_lowest))
        # print(array_lowest)

        # KDJ (K line, D line, J line)
        Kvalue = []
        for x in range(kperiods, array_close.size):
            k = ((array_close[x] - array_lowest[x - kperiods]) * 100 / (array_highest[x - kperiods] - array_lowest[x - kperiods]))
            Kvalue.append(k)
        # print(len(Kvalue))
        # print(Kvalue)
        y = 0
        # dperiods for calculate d values
        dperiods = 3
        Dvalue = [None, None]
        mean = 0
        for x in range(0, len(Kvalue) - dperiods + 1):
            sum = 0
            for j in range(0, dperiods):
                sum = Kvalue[y] + sum
                y = y + 1
            mean = sum / dperiods
            # d values for %d line
            Dvalue.append(mean)
            y = y - (dperiods - 1)
        # print(len(Dvalue))
        # print(Dvalue)
        Jvalue = [None, None]
        for x in range(0, len(Dvalue) - dperiods + 1):
            j = (Dvalue[x + 2] * 3) - (Kvalue[x + 2] * 2)
            # j values for %j line
            Jvalue.append(j)
        # print(len(Jvalue))
        # print(Jvalue)

        result['K'] = Kvalue
        result['D'] = Dvalue
        result['J'] = Jvalue

        df_kdj = pd.DataFrame(result).reset_index().drop('index', axis=1)
        df_data = df[kperiods:].reset_index().drop('index', axis=1)

        df_result = df_data.join(df_kdj, how='outer')

        return (df_result)

    def price_hit_cnt(self, df, price, cri_hit=0.01):

        h_cnt = df.loc[(df['high'] <= (1 + cri_hit) * price) & (df['high'] >= (1 - cri_hit) * price)].__len__()

        l_cnt = df.loc[(df['low'] <= (1 + cri_hit) * price) & (df['low'] >= (1 - cri_hit) * price)].__len__()

        o_cnt = df.loc[(df['open'] <= (1 + cri_hit) * price) & (df['open'] >= (1 - cri_hit) * price)].__len__()

        c_cnt = df.loc[(df['close'] <= (1 + cri_hit) * price) & (df['close'] >= (1 - cri_hit) * price)].__len__()

        # debug. The low,open,high will not show on the plot
        # print(df.loc[(df['close'] <= (1+cri_hit) * price) & ( df['close'] >= (1-cri_hit) * price) ])

        rtn = {
            'sum_cnt': h_cnt + l_cnt + o_cnt + c_cnt,
            'price_benchmark': price,
            'cri_hit': cri_hit,
            'h_cnt': h_cnt,
            'l_cnt': l_cnt,
            'o_cnt': o_cnt,
            'c_cnt': c_cnt,
        }
        return (rtn)

    # verify if current price hit any value of fibo series

    # cri_hit: how many time price pxx hitted. e.g p23=10, then find 10*.099 < Cnt([open|close|high|low]) < 10*.1.01
    def fibonocci(self, df, cri_percent=5, cri_hit=0.01):

        y_axis = np.array(df['close'])
        x_axis = np.array(df['date'])

        min = np.min(y_axis)
        max = np.max(y_axis)
        delta = (max - min) / 100

        # Fibonacci 23.6, 38.2, 50, 61.8, 100
        p00 = min
        p23 = min + 23.6 * delta
        p38 = min + 38.2 * delta
        p50 = min + 50 * delta
        p61 = min + 61.8 * delta
        p100 = max

        p00_cnt = self.price_hit_cnt(df, p00, cri_hit)
        p23_cnt = self.price_hit_cnt(df, p23, cri_hit)
        p38_cnt = self.price_hit_cnt(df, p38, cri_hit)
        p50_cnt = self.price_hit_cnt(df, p50, cri_hit)
        p61_cnt = self.price_hit_cnt(df, p61, cri_hit)
        p100_cnt = self.price_hit_cnt(df, p100, cri_hit)

        cur_price = y_axis[-1]
        cur_percent = (cur_price - min) / delta

        hit = True  # hit the buy condition or intersting condition

        d100 = cur_percent - 100
        d61 = cur_percent - 61.8
        d50 = cur_percent - 50
        d38 = cur_percent - 38.2
        d23 = cur_percent - 23.6
        d00 = cur_percent - 0

        closest = "NA"
        current_hit_cnt = 0
        long_enter_price = cur_price * .98  #the price that we suggest to buy in
        long_take_profit_price = 0  #buy tp
        long_stop_lost_price = 0  #buy sl

        if d100 > 0 and d100 < cri_percent:  #should hit this. as cur_price will not exceed the max.
            closest = "100"
            current_hit_cnt = p100_cnt
            #print("distance passed max " + str(round(d100, 0)))

        elif d61 > 0 and d61 < cri_percent:
            closest = "61"
            current_hit_cnt = p61_cnt
            #print("distance passed 61.8% less than " + str(round(d61, 0)))
            long_take_profit_price = p100 - (5 * delta)
            long_stop_lost_price = p61 - (5 * delta)

        elif d50 > 0 and d50 < cri_percent:
            closest = "50"
            current_hit_cnt = p50_cnt
            #print("distance passed 50% less than " + str(round(d50, 0)))
            long_take_profit_price = p61 - (5 * delta)
            long_stop_lost_price = p50 - (5 * delta)

        elif d38 > 0 and d38 < cri_percent:
            closest = "38"
            current_hit_cnt = p38_cnt
            #print("distance passed 38.2% less than " + str(round(d38, 0)))
            long_take_profit_price = p50 - (5 * delta)
            long_stop_lost_price = p38 - (5 * delta)

        elif d23 > 0 and d23 < cri_percent:
            closest = "23"
            current_hit_cnt = p23_cnt
            #print("distance passed 23.6% less than " + str(round(d23, 0)))
            long_take_profit_price = p38 - (5 * delta)
            long_stop_lost_price = p23 - (5 * delta)

        elif d00 > 0 and d00 < cri_percent:  #cur_price near the all min.
            closest = "00"
            current_hit_cnt = p00_cnt
            #print("distance passed min less than " + str(round(d00, 0)))
            long_take_profit_price = p23 - (5 * delta)
            long_stop_lost_price = p00 - (5 * delta)
        else:
            hit = False

        rtn = {
            "hit": hit,  # True of False
            "closest": closest,  # closest taget Fibocinno number
            "current_hit_cnt": current_hit_cnt,  # how many times this price was hit by OHLC.
            "pri_cur": cur_price,
            "per_cur": round(cur_percent, 2),  # current percent in Fibo, if hit, 0 < per_cur -closet  < cri_percent
            "p_max": max,
            "p_min": min,
            "date_max": np.max(x_axis),
            "date_min": np.min(x_axis),
            "p100": round(p100, 1),  # price of 100%
            "p61": round(p61, 1),
            "p50": round(p50, 1),
            "p38": round(p38, 1),
            "p23": round(p23, 1),
            "p00": round(p00, 1),
            "p100_cnt": p100_cnt,
            "p61_cnt": p61_cnt,
            "p50_cnt": p50_cnt,
            "p38_cnt": p38_cnt,
            "p23_cnt": p23_cnt,
            "p00_cnt": p00_cnt,
            "d100": round(d100, 1),  # distance to 100%
            "d61": round(d61, 1),
            "d50": round(d50, 1),
            "d38": round(d38, 1),
            "d23": round(d23, 1),
            "d00": round(d00, 1),
            "long_enter_price": round(long_enter_price, 2),
            "long_take_profit_price": round(long_take_profit_price, 2),
            "long_stop_lost_price": round(long_stop_lost_price, 2),
            "one_percent_delta": round(delta, 2),
            "long_take_profit_percent": round((long_take_profit_price - long_enter_price) * 100 / long_enter_price, 1),
            "long_stop_lost_percent": round((long_stop_lost_price - long_enter_price) * 100 / long_enter_price, 1),
        }

        return (rtn)

    def get_stock_data_info(self, market, code):
        rtn = {'valid': False, 'updated': False, 'csv': None}
        code = str(code)

        data_base = '/home/ryan/DATA/DAY_Global'
        last_trading_day_Ymd = self.get_last_trading_day(debug=False)
        last_trading_day_Y_m_d = datetime.strptime(last_trading_day_Ymd, "%Y%m%d").strftime("%Y-%m-%d")

        date_col_name = 'date'

        if market == 'CN':
            data_csv = data_base + "/AG/" + code + ".csv"
        elif market == "CN_INDEX":
            data_csv = data_base + "/AG_INDEX/" + code + ".csv"
            date_col_name = 'trade_date'  #19901219
        elif market == 'US':
            data_csv = data_base + "/" + market + "/" + code + ".csv"
            date_col_name = 'datetime'
            last_trading_day = self.get_last_trading_day_us()
        elif market == 'US_INDEX':
            data_csv = data_base + "/" + market + "/" + code + ".csv"
            date_col_name = 'Date'
            last_trading_day = self.get_last_trading_day_us()
        elif market == 'HK':
            data_csv = data_base + "/HK/" + code + ".csv"
            date_col_name = 'datetime'

        if not os.path.isfile(data_csv):
            logging.warning(__file__+" "+"warn: data file doesn't exist. " + data_csv)
            return (rtn)
        else:
            rtn['exist'] = True
            rtn['csv'] = data_csv

        if market == 'CN':
            date_col_name = 'date'
            df = pd.read_csv(data_csv, names=['code', date_col_name, 'o', 'h', 'l', 'c', 'vol', 'amt', 'exchage_rate'])
        else:
            df = pd.read_csv(data_csv)

        last_day_in_csv = self.rgular_date_to_ymd(str(df[date_col_name].iloc[-1:].values[0]))

        rtn['last_day_in_csv'] = last_day_in_csv
        rtn['expected_update_date'] = last_trading_day_Ymd

        if last_day_in_csv == last_trading_day_Ymd:
            rtn['updated'] = True
        else:
            logging.warning(__file__+" "+"out-of-date, expected date " + str(last_trading_day_Y_m_d) + ". date in csv " + str(last_day_in_csv) + " " + data_csv)

        pass
        return (rtn)

    '''
    rst['CN_INDEX']
    Out[5]: 
            code   name
    0  000001.SH   上证综指
    1  000300.SH  沪深300
    2  000905.SH  中证500
    '''
    def load_select(self):
        select_csv = "/home/ryan/tushare_ryan/select.yml"

        rst = {}

        with open(select_csv) as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
            file.close()

        for market in cfg.keys():
            rst[market] = pd.DataFrame(columns=['code', 'name'])

            for code_name_dict in cfg[market]:
                for code in code_name_dict.keys():
                    rst[market] = rst[market].append({'code': code, 'name': code_name_dict[code]}, ignore_index=True)
        return (rst)

    #convert YYYY-MM-DD to YYYYMMDD
    def regular_date_to_ymd(self, dateStr):
        if (dateStr.count("-") == 2):
            dateStr = datetime.strptime(dateStr, '%Y-%m-%d').strftime('%Y%m%d')
        elif (dateStr.count("-") != 0):
            logging.fatal(__file__+" "+"unknow date format, " + str(dateStr))
            exit(0)
        return (dateStr)

    #regular df to format: code, name, open,high,low,close,volume
    def regular_read_csv_to_stdard_df(self, data_csv,add_market=False,exit_if_not_exist=True):
        logging.info("loading "+data_csv)
        base_dir = "/home/ryan/DATA/DAY_Global"
        base_dir_fund2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"
        data_csv = str(data_csv)
        rtn_df = pd.DataFrame()
        data_csv_fp = os.path.abspath(data_csv)
        dir = os.path.dirname(data_csv_fp)

        if not os.path.isfile(data_csv_fp):
            logging.fatal(__file__+" "+"file not exist. " + data_csv_fp)
            if exit_if_not_exist:
                exit(0)
            else:
                return("FILE_NOT_EXIT")

        if dir == base_dir + "/AG":
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, header=None, skiprows=1, names=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'tnv'])
        elif dir in [base_dir + "/stooq/US_INDEX", base_dir + "/stooq/US"]:
            #DOW.csv  SP500.csv, AAPL.csv
            #add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir == base_dir + "/US":
            #add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir == base_dir + "/HK":
            #add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")

        elif dir == base_dir + "/AG_INDEX":
            rtn_df = pd.read_csv(data_csv_fp, skiprows=1, header=None, names=['code', 'date', 'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'], converters={'code': str, 'date': str}, encoding="utf-8")

        # elif dir == base_dir + "/stooq/US_INDEX":
        #     rtn_df = pd.read_csv(data_csv_fp, skiprows=1, header=None, names=['date','open','high','low','close','volume'], converters={'date': str}, encoding="utf-8")
        #     _code = os.path.basename(data_csv_fp).replace(".csv",'').upper()
        #     rtn_df['code']=_code

        elif dir == base_dir_fund2 + "/source/basic_daily":
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp,converters={'ts_code': str, 'trade_date': str}, encoding="utf-8"))

        elif dir == base_dir_fund2 + "/source/basic_quarterly":
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp, encoding="utf-8"))

        elif dir == base_dir_fund2 + "/source/market":
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp, converters={'ts_code': str, 'list_date': str}, encoding="utf-8"))

        elif dir == base_dir_fund2 + "/source":
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp, converters={'ts_code': str, 'trade_date':str}, encoding="utf-8"))

        elif dir == base_dir_fund2 + "/merged":
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp, encoding="utf-8"))


        elif dir == base_dir_fund2 + "/peg":
            rtn_df = pd.read_csv(data_csv_fp, encoding="utf-8")


        elif dir in ["/home/ryan/DATA/pickle/daily_update_source", "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/info_daily"]:
            rtn_df = self.ts_code_to_code(pd.read_csv(data_csv_fp, converters={'ts_code': str, 'trade_date': str},encoding="utf-8"))

        elif data_csv_fp =="/home/ryan/DATA/pickle/instrument_A.csv":
            add_market = True
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'list_date': str},  encoding="utf-8")

        elif dir.__contains__("/home/ryan/DATA/result"):
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir.__contains__("/home/ryan/DATA"):
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        else:
            logging.fatal(__file__+" "+"unknown path file " + data_csv_fp)
            exit(0)

        if rtn_df.__len__() > 0:
            rtn_df = self.regular_column_names(rtn_df)
            rtn_df = self.regular_df_date_to_ymd(rtn_df)

            if add_market:
                rtn_df = self.add_market_to_code(rtn_df)

            if 'Unnamed: 0' in rtn_df.columns:
                rtn_df = rtn_df.drop('Unnamed: 0', axis=1)

            rtn_df['code'] = rtn_df['code'].apply(lambda _d: str(_d).upper())

        return (rtn_df)

    def regular_read_akshare_to_stdard_df(self, data_csv,add_market=True):
        base_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/akshare/source"
        data_csv = str(data_csv)
        rtn_df = pd.DataFrame()

        data_csv_fp = os.path.abspath(data_csv)
        dir = os.path.dirname(data_csv_fp)

        if not os.path.isfile(data_csv_fp):
            logging.fatal(__file__+" "+"file not exist. " + data_csv_fp)
            exit(0)

        rtn_df = pd.read_csv(data_csv_fp, encoding="utf-8",converters={i: str for i in range(100)})

        _t = 'Unnamed: 0'
        if _t in rtn_df.columns:
            rtn_df = rtn_df.drop(_t, axis=1)


        fname_dict = {
        "SName":"name",
        "SCode":"code",
        "板块":"Plate",
        "板块代码": "Section code",
        "板块名称": "Section name",
        "超大单净流入-净额": "Super large single net inflow-net",
        "超大单净流入-净占比": "Super large single net inflow-net proportion",
        "城     市": "city",
        "成交额": "Turnover",
        "成交量": "Volume",
        "持股比例": "Shareholding ratio",
        "持股比例增幅": "Increase in shareholding ratio",
        "持有数": "Number of holdings",
        "达到平仓线比例(%)": "Proportion of reaching the liquidation line (%)",
        "达到预警线未达平仓线比例(%)": "Proportion of reaching the warning line and not reaching the liquidation line (%)",
        "大单净流入-净额": "Large order net inflow-net",
        "大单净流入-净占比": "Large order net inflow-net proportion",
        "代码": "code",
        "当年累计净利润_累计净利润": "Cumulative net profit for the year_cumulative net profit",
        "当年累计净利润_同比增长": "Cumulative net profit for the year _ year-on-year growth",
        "当年累计营业收入_累计营业收入": "Cumulative operating income for the year_cumulative operating income",
        "当年累计营业收入_同比增长": "Cumulative operating income for the year _ year-on-year growth",
        "当前价": "Current price",
        "当月净利润_环比增长": "Net profit for the month _ month-on-month growth",
        "当月净利润_净利润": "Net profit for the month_net profit",
        "当月净利润_同比增长": "Net profit for the month _ year-on-year growth",
        "当月营业收入_环比增长": "Operating income for the month _ month-on-month growth",
        "当月营业收入_同比增长": "Operating income for the month _ year-on-year growth",
        "当月营业收入_营业收入": "Current month operating income_operating income",
        "等级": "grade",
        "地区": "Area",
        "对应值": "Corresponding value",
        "分红次数": "Dividend times",
        "分析师": "Analyst",
        "供应商、客户和消费者权益责任": "Supplier, customer, and consumer rights responsibilities",
        "公告日期": "Announcement date",
        "公司代码": "Company code",
        "公司家数": "Number of companies",
        "公司简称": "Company abbreviation",
        "公司名称": "company name",
        "公司全称": "full name of company",
        "公司网址": "company website",
        "股东名称": "Shareholder name",
        "股东责任": "Shareholder responsibility",
        "股票代码": "Stock code",
        "股票简称": "Stock abbreviation",
        "股票名称": "name",
        "关注度": "Attention",
        "关注股票数": "Pay attention to the number of stocks",
        "行业": "industry",
        "行业名称": "Industry Name",
        "沪深300指数": "CSI 300 Index",
        "环境责任": "Environmental responsibility",
        "机构数": "Number of institutions",
        "机构数变化": "Change in number of institutions",
        "机构席位买入额(万)": "Institutional seat purchases (10,000)",
        "机构席位卖出额(万)": "Institutional seats sold (10,000)",
        "减持家数": "Reduce the number of households",
        "减持评级数": "Number of underweight ratings",
        "减持数": "Underweight",
        "简称": "Abbreviation",
        "交易日期": "transaction date",
        "结束日期": "End date",
        "近一年涨跌幅(%)": "Change in the past year (%)",
        "净额": "Net",
        "净利润规模(元)": "Net profit scale (yuan)",
        "净利润同比(%)": "Net profit year-on-year (%)",
        "净利润(元)": "Net profit (yuan)",
        "净资产_净资产": "Net assets_net assets",
        "净资产_同比增长": "Net assets_ year-on-year growth",
        "净资产(元)": "Net assets (yuan)",
        "开始日期": "start date",
        "类型": "Types of",
        "累积购买额": "Cumulative purchase amount",
        "累积买入额": "Cumulative purchase amount",
        "累积卖出额": "Cumulative sales",
        "累计股息(%)": "Cumulative dividend (%)",
        "买入次数": "Number of buys",
        "买入家数": "Number of homes bought",
        "买入评级数": "Number of buy ratings",
        "买入前三股票": "Buy the top three stocks",
        "买入数": "Number of purchases",
        "买入席位数": "Number of buy seats",
        "卖出次数": "Number of sells",
        "卖出家数": "Number of homes sold",
        "卖出评级数": "Number of sell ratings",
        "卖出数": "Number sold",
        "卖出席位数": "Number of sales attendance",
        "名称": "name",
        "目标价": "Target price",
        "年均股息(%)": "Average annual dividend (%)",
        "平均价格": "average price",
        "平均目标价": "Average target price",
        "平均目标涨幅": "Average target increase",
        "平均评级": "Average rating",
        "平均涨幅": "Average increase",
        "平均质押比例(%)": "Average pledge ratio (%)",
        "评级机构": "rating agencies",
        "评级机构数": "Number of rating agencies",
        "评级日期": "Rating date",
        "日期": "date",
        "融资次数": "Number of financing",
        "融资总额(亿)": "Total financing (100 million)",
        "商誉报告日期": "Goodwill report date",
        "商誉规模(元)": "Goodwill scale (yuan)",
        "商誉规模占净资产规模比例(%)": "Proportion of goodwill scale in net assets scale (%)",
        "商誉减值(元)": "Goodwill impairment (yuan)",
        "商誉减值占净利润比例(%)": "Percentage of goodwill impairment in net profit (%)",
        "商誉减值占净资产比例(%)": "The percentage of goodwill impairment in net assets (%)",
        "商誉(元)": "Goodwill (yuan)",
        "商誉占净资产比例(%)": "Proportion of goodwill in net assets (%)",
        "上榜次数": "Number of rankings",
        "上年度同期净利润(元)": "Net profit for the same period of last year (yuan)",
        "上年商誉": "Goodwill of the previous year",
        "上年商誉(元)": "Goodwill of the previous year (yuan)",
        "上市日期": "Listing date",
        "上证-收盘价": "Shanghai Stock Exchange-Closing Price",
        "上证-涨跌幅": "Shanghai Stock Exchange-Change",
        "社会责任": "Social responsibility",
        "深证-收盘价": "Shenzhen Stock Exchange-Closing Price",
        "深证-涨跌幅": "Shenzhen Stock Exchange-Change",
        "省    份": "Province",
        "是否净流入": "Whether net inflow",
        "收盘价": "Closing price",
        "所属行业": "Industry",
        "统计时间": "Statistics Time",
        "未达预警线比例(%)": "Proportion of not reaching the warning line (%)",
        "无限售股质押数(股)": "Pledged number of unlimited shares (shares)",
        "限售股质押数(股)": "Pledge number of restricted shares (shares)",
        "详细": "detailed",
        "小单净流入-净额": "Small order net inflow-net",
        "小单净流入-净占比": "Small order net inflow-net proportion",
        "序号": "Serial number",
        "业绩变动幅度-上限": "Performance change range-upper limit",
        "业绩变动幅度-下限": "Performance change range-lower limit",
        "业绩变动原因": "Reasons for performance changes",
        "英文名称": "English name",
        "营业部名称": "Sales department name",
        "预告内容": "Preview content",
        "预估平仓线(元)": "Estimated closing line (yuan)",
        "预计净利润(元)-上限": "Estimated net profit (yuan)-upper limit",
        "预计净利润(元)-下限": "Estimated net profit (yuan)-lower limit",
        "员工责任": "Employee responsibility",
        "增持家数": "Increase the number of households",
        "增持评级数": "Overweight ratings",
        "占流通股比例": "Percentage of outstanding shares",
        "占流通股比例增幅": "Increase in the proportion of outstanding shares",
        "占所持股份比例(%)": "Percentage of shares held (%)",
        "占总股本比例(%)": "Percentage of total equity (%)",
        "涨跌额": "Ups and downs",
        "涨跌幅": "Quote change",
        "证券代码": "Securities code",
        "证券简称": "Securities short name",
        "质押比例(%)": "Pledge ratio (%)",
        "质押笔数": "Number of pledges",
        "质押公司股票代码": "Pledge company stock code",
        "质押公司数量": "Number of pledge companies",
        "质押股份数量(股)": "Number of pledged shares (shares)",
        "质押股数(股)": "Number of pledged shares (shares)",
        "质押机构": "Pledge institution",
        "质押开始日期": "Pledge start date",
        "质押日收盘价(元)": "Closing price of pledge day (yuan)",
        "质押市值(元)": "Pledge market value (yuan)",
        "质押数量(股)": "Number of pledges (shares)",
        "质押总笔数": "Total number of pledges",
        "质押总股本": "Pledged total equity",
        "质押总股数(股)": "Total pledged shares (shares)",
        "质押总市值(元)": "Total market value of pledge (yuan)",
        "中单净流入-净额": "Medium Single Net Inflow-Net",
        "中单净流入-净占比": "Net inflow of medium singles-net proportion",
        "中性家数": "Number of Neutral Homes",
        "中性评级数": "Neutral rating number",
        "中性数": "Neutral number",
        "主力净流入-净额": "Main net inflow-net",
        "主力净流入-净占比": "Main net inflow-net proportion",
        "主力净流入最大股": "The main net inflow of the largest stocks",
        "主力净流入最大股代码": "Major net inflow largest stock code",
        "注册地址": "Registered address",
        "综合评级": "Comprehensive rating",
        "综合评级↑": "Comprehensive rating↑",
        "总成交额(万元)": "Total turnover (ten thousand yuan)",
        "总成交量(手)": "Total volume (hands)",
        "总得分": "Total Score",
        "最低目标价": "Lowest target price",
        "最高目标价": "Highest target price",
        "最新价": "Latest price",
        "最新价(元)": "Latest price (yuan)",
        "最新评级": "Latest rating",
        "最新一期商誉(元)": "The latest goodwill (yuan)",
        "最新质押市值": "The latest pledge market value",
        }

        for k in fname_dict.keys():
            if k in rtn_df.columns:
                rtn_df.rename(columns={k:fname_dict[k].lower()}, inplace=True)

        if rtn_df.__len__() > 0:
            rtn_df = self.regular_column_names(rtn_df)
            rtn_df = self.regular_df_date_to_ymd(rtn_df)

            if add_market:
                rtn_df = self.add_market_to_code(rtn_df)
                rtn_df['code'] = rtn_df['code'].apply(lambda _d: str(_d).upper())

        return (rtn_df)

    def pprint(self, df):
        str = tabulate.tabulate(df, headers='keys', tablefmt='psql')
        #logging.info(str)
        return(str)

    def get_stock_configuration(self, selected, stock_global):
        rtn = {
            "stock_list": None,
            "csv_dir": None,
            "out_dir": None,
        }

        if selected:
            selected_stocks = self.load_select()
            out_dir = "/home/ryan/DATA/result/selected"

            # selected INDEX first
            if stock_global == 'AG_INDEX':
                csv_dir = "/home/ryan/DATA/DAY_Global/AG_INDEX"
                stock_list = selected_stocks['CN_INDEX']
            elif stock_global == 'US_INDEX':
                csv_dir = "/home/ryan/DATA/DAY_Global/stooq/US_INDEX"
                stock_list = selected_stocks['US_INDEX']
            elif stock_global == "HK_INDEX":
                csv_dir = "/home/ryan/DATA/DAY_Global/HK_INDEX"
                stock_list = selected_stocks['HK_INDEX']

            # Then selected Stocks
            elif stock_global == "AG":
                csv_dir = "/home/ryan/DATA/DAY_Global/AG"
                stock_list = selected_stocks['CN']
            elif stock_global == 'HK':
                csv_dir = "/home/ryan/DATA/DAY_Global/HK"
                stock_list = selected_stocks['HK']
            elif stock_global == 'US':
                csv_dir = "/home/ryan/DATA/DAY_Global/stooq/US"
                stock_list = selected_stocks['US']
            # elif stock_global == "AG_AK":
            #     csv_dir = "/home/ryan/DATA/DAY_Global/akshare/AG"
            #     stock_list = selected_stocks['CN']
            elif stock_global == 'HK_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/HK"
                stock_list = selected_stocks['HK']
            elif stock_global == 'US_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/US"
                stock_list = selected_stocks['US']


            # Then (selected) Holded Stocks (
            elif stock_global == "AG_HOLD":
                csv_dir = "/home/ryan/DATA/DAY_Global/AG"
                stock_list = selected_stocks['CN_HOLD']
            elif stock_global == 'HK_HOLD':
                csv_dir = "/home/ryan/DATA/DAY_Global/HK"
                stock_list = selected_stocks['HK_HOLD']
            elif stock_global == 'US_HOLD':
                csv_dir = "/home/ryan/DATA/DAY_Global/stooq/US"
                stock_list = selected_stocks['US_HOLD']

            # Then (selected) Holded Stocks (AK Share source)
            elif stock_global == "AG_HOLD_AK":
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/AG"
                stock_list = selected_stocks['CN_HOLD']
            elif stock_global == 'HK_HOLD_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/HK"
                stock_list = selected_stocks['HK_HOLD']
            elif stock_global == 'US_HOLD_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/US"
                stock_list = selected_stocks['US_HOLD']



        else:  # selected == False
            if stock_global == 'AG':
                csv_dir = "/home/ryan/DATA/DAY_Global/AG"
                out_dir = "/home/ryan/DATA/result"
                stock_list = self.get_A_stock_instrment()  # 603999
                stock_list = self.add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
                stock_list = self.remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
            elif stock_global == 'HK':
                csv_dir = "/home/ryan/DATA/DAY_Global/HK"
                out_dir = "/home/ryan/DATA/result/hk"
                df_instrument = self.get_instrument()
                stock_list = df_instrument.query("market==31 and category==2").reset_index().drop('index', axis=1)  # 1973
            elif stock_global == 'US':
                csv_dir = "/home/ryan/DATA/DAY_Global/US"
                out_dir = "/home/ryan/DATA/result/us"
                df_instrument = self.get_instrument()
                stock_list = df_instrument.query("market==74 and category==13").reset_index().drop('index', axis=1)  # 11278
            elif stock_global == 'MG':  #41,11,美股知名公司,MG
                csv_dir = "/home/ryan/DATA/DAY_Global/MG"
                out_dir = "/home/ryan/DATA/result/mg"
                df_instrument = self.get_instrument()
                stock_list = df_instrument.query("market==41 and category==11").reset_index().drop('index', axis=1)  # 289
            elif stock_global == 'CH':  #40,11,中国概念股,CH
                csv_dir = "/home/ryan/DATA/DAY_Global/CH"
                out_dir = "/home/ryan/DATA/result/ch"
                df_instrument = self.get_instrument()
                stock_list = df_instrument.query("market==40 and category==11").reset_index().drop('index', axis=1)  # 78
            
            # Then (not selected) Stocks (AK Share source)
            # elif stock_global == "AG_AK":
            #     csv_dir = "/home/ryan/DATA/DAY_Global/akshare/AG"
            #     stock_list = selected_stocks['CN_HOLD']
            elif stock_global == 'HK_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/HK"
                stock_list = self.get_ak_hk_us_list('HK')[['code','name']]
                out_dir = "/home/ryan/DATA/result/hk"
            elif stock_global == 'US_AK':
                csv_dir = "/home/ryan/DATA/DAY_Global/akshare/US"
                out_dir = "/home/ryan/DATA/result/us"
                stock_list = self.get_ak_hk_us_list('US')[['code','name']]

        rtn = {
            "stock_list": stock_list,
            "csv_dir": csv_dir,
            "out_dir": out_dir,
        }

        return (rtn)

    def get_ak_hk_us_list(self,market):
        if market not in ['HK','US']:
            logging.error("unknown market, expected ['HK','US'], got "+str(market))
            exit()
        csv_f = "/home/ryan/DATA/result/wei_pan_la_sheng/"+market+"_spot_link.csv"

        df = pd.read_csv(csv_f, converters={'code': str, 'name':str})

        return(df)


    #keep columns in the col_name_list_kept
    def keep_column(self, df, col_keep):
        cols = df.columns.tolist()

        col_nk= []
        for i in range(col_keep.__len__()):
            if (col_keep[i] in cols):
                col_nk.append(col_keep[i])

        return(df[col_nk])

    #adjust columns order in a df
    def adjust_column(self, df, col_name_list):
        # adjust column sequence here
        #col_name_list = ['code', 'trade_date']

        cols = df.columns.tolist()
        name_list = list(reversed(col_name_list))
        for i in name_list:
            if i in cols:
                cols.remove(i)
                cols.insert(0, i)
            else:
                logging.info(__file__ + " " + "warning, no column named " + i + " in cols")

        df = df[cols]
        df = df.fillna(0)
        df = df.reset_index().drop('index', axis=1)

        return(df)

    def get_ts_field(self, ts_code, ann_date, field, big_memory=False,df_all_ts_pro=None,fund_base_merged="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged"):
        #if big_memory==True, must provide df_all_ts_pro.

        if fund_base_merged == None:
            fund_base_merged = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged"

        if big_memory:
            df = df_all_ts_pro
            df = df[df['ts_code'] == ts_code]
            if (df.__len__() == 0):
                logging.info(__file__ + " " + "no ts_code in df_all_ts_pro " + ts_code)
                return

            df = df[df['end_date'] == ann_date]
            if (df.__len__() == 0):
                logging.info(__file__ + " " + "no end_date in df_all_ts_pro " + ts_code + " " + ann_date)
                return

            data_in_field = df[field].values[0]
            df = None
            return (data_in_field)
        else:
            f = fund_base_merged + "/" + "merged_all_" + ann_date + ".csv"

            if not os.path.exists(f):
                logging.info(__file__ + " " + "file not exists, " + f)
                return

            df = pd.read_csv(f, converters={'end_date': str})

            if not field in df.columns:
                logging.info(__file__ + " " + "field not in the file, " + field + " " + f)
                return

            df = df[df['ts_code'] == ts_code]

            if (df.__len__() == 0):
                logging.info(__file__ + " " + "no ts_code in file " + ts_code + " " + f)
                return

            data_in_field = df[field].values[
                0]  # always return the first one. suppose the 1st is the most updated one if multiple lines for the code+ann_date

            return(data_in_field)

    def get_ts_quarter_field(self, ts_code, ann_date, field, base_dir="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"):

        f = base_dir + "/source/basic_quarterly/basic_" + ann_date + ".csv"

        if not os.path.exists(f):
            logging.warning(__file__ + " " + "file not exists, " + f)
            return

        df = pd.read_csv(f, converters={'trade_date': str})

        if not field in df.columns:
            logging.warning(__file__ + " " + "field not in the file, " + field + " " + f)
            return

        df = df[df['ts_code'] == ts_code]

        if (df.__len__() == 0):
            logging.warning(__file__ + " " + "no ts_code in file " + ts_code + " " + f)
            return

        data_in_field = df[field].values[0]  # always return the first one. suppose the 1st is the most updated one if multiple lines for the code+ann_date

        return(data_in_field)


    def get_tspro_query_fields(self,api):
        myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
        ts.set_token(myToken)
        field_csv = "/home/ryan/tushare_ryan/tushare_api_fields.csv"
        df_field = pd.read_csv(field_csv, encoding="utf-8", dtype=str)

        _a = df_field[df_field['API'] == api]['FIELD_NAME']
        logging.info(api+" file field count " + str(_a.__len__()))

        _c = ts.pro_api().query(api, ts_code='600519.SH', period='20191231').columns.to_series().reset_index()['index']
        logging.info(api+" tushare field count " + str(_c.__len__()))

        _d = _a.append(_c).drop_duplicates()
        logging.info(api+" finally field count " + str(_d.__len__()))
        rtn_fields = ','.join(list(_d))
        return (rtn_fields)

    def get_last_n_days_daily_basic(self,ndays=None,dayS=None,dayE=None,daily_update=None,debug=False, force_run=False):

        basic_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily"

        # logic for dayS and dayE:
        if (dayS is not None) and (dayE is not None):
            ndays = (datetime.strptime(dayE, "%Y%m%d") - datetime.strptime(dayS, "%Y%m%d")).days+1
            logging.info("get_last_n_days_daily_basic, using specifed dayS and dayE, ingore ndays. Use caculated ndays "+str(ndays))
        elif (dayE is not None) and (ndays is not None):
            dayS = (datetime.today() - timedelta(365)).strftime("%Y%m%d")
        elif (dayS is None) and (dayE is None) and (ndays is not None):
            dayS = (datetime.today() - timedelta(ndays)).strftime("%Y%m%d")
            dayE = datetime.today().strftime("%Y%m%d")
        else:
            logging.fatal("unsupported input parameter. exit")
            sys.exit(1)

        if daily_update:
            sl_out_csv = "/home/ryan/DATA/result/latest_daily_basic_" + str(ndays) + "_days.csv"  # symbol link

        out_csv = "/home/ryan/DATA/result/daily_basic_"+dayS+"_"+dayE+".csv"

        # if (not debug) and self.is_cached(file_path=out_csv, day=7) and (datetime.today() > datetime.strptime(dayE, "%Y%m%d")):
        if self.is_cached(file_path=out_csv, day=7) and (not force_run):
            logging.info("get_last_n_days_daily_basic loading cached file "+out_csv)
            return(pd.read_csv(out_csv))



        df = pd.DataFrame()
        j = 0
        for i in range(ndays):
            j += 1
            date = (datetime.strptime(dayE, "%Y%m%d")  - timedelta(days=i)).strftime("%Y%m%d")
            input_csv = basic_dir + "/basic_" + date + ".csv"

            if self.is_cached(input_csv, day=1000):
                df_sub = pd.read_csv(input_csv)

                df = df.append(df_sub)
                logging.info(str(j) + " of " + str(ndays)+" days, appended " + input_csv + ", +len " + str(df_sub.__len__()))
            else:
                logging.warning("no such file "+input_csv)


        df.to_csv(out_csv, encoding='UTF-8', index=False)

        # daily update, check every day.
        if daily_update:
            if os.path.exists(sl_out_csv):
                os.unlink(sl_out_csv)

            os.symlink(out_csv, sl_out_csv)
            logging.info("\nsymbol link created. " + sl_out_csv + " --> " + out_csv)

        return(df)

    #  对样本空间内剩余证券，按照过去一年的日均总市值由高到低排名，选取前 300 名的证券作为指数样本。
    def sort_by_market_cap_since_n_days_avg(self,ndays=None,period_start=None,period_end=None, daily_update=False,debug=False, df_parent=None,force_run=False):
        if debug:
            ndays = 5

        if period_start is None:
            period_start = (datetime.strptime(period_end,"%Y%m%d") - timedelta(days=ndays)).strftime("%Y%m%d")

        mktcap_csv = "/home/ryan/DATA/result/average_daily_mktcap_sorted_"+str(period_start)+"_"+str(period_end)+".csv"

        # if (not debug) and (not force_run) and self.is_cached(file_path=mktcap_csv, day=7):
        #     logging.info("read result from " + mktcap_csv)
        #     return (pd.read_csv(mktcap_csv))

        df = self.get_last_n_days_daily_basic(ndays=None,dayS=period_start,dayE=period_end, daily_update=daily_update,debug=debug,force_run=force_run)

        the_latest_date = df['trade_date'].unique().max() #'20210107'

        df_basic = df[(df['trade_date'] >= int(period_start)) & (df['trade_date'] <= int(period_end))]
        
        #reduce the rows to ts_code_to_code fast
        # df_basic = df_basic.groupby(by='ts_code').mean().sort_values(by=['total_mv'], ascending=[False],  inplace=False).reset_index() #code is in tspro format, 000001.SZ
        df_basic = df_basic.groupby(by='ts_code').mean().sort_values(by=['circ_mv'], ascending=[False],  inplace=False).reset_index() #code is in tspro format, 000001.SZ

        df_basic['total_mv'] = df_basic['total_mv'].apply(lambda _d: round(_d*10000,0))
        df_basic['circ_mv'] = df_basic['circ_mv'].apply(lambda _d: round(_d*10000,0))

        # total_mv_perc: the rank of the total_mv
        df_basic['circ_mv_perc'] = df_basic['circ_mv'].apply(lambda _d: round(stats.percentileofscore(df_basic['circ_mv'], _d) / 100, 4))
        df_basic['circ_mv_portion'] = df_basic['circ_mv'].apply(lambda _d: round(_d*100.0/df_basic['circ_mv'].sum(), 2))

        df_basic['total_mv_perc'] = df_basic['total_mv'].apply(lambda _d: round(stats.percentileofscore(df_basic['total_mv'], _d) / 100, 4))
        df_basic['total_mv_portion'] = df_basic['total_mv'].apply(lambda _d: round(_d*100.0/df_basic['total_mv'].sum(), 2))


        df_basic = self.ts_code_to_code(df=df_basic)
        df_basic['date'] = str(the_latest_date)
        df_basic = df_basic.drop('trade_date', axis=1)


        if df_parent is not None:
            df_basic = pd.merge(df_parent, df_basic, on='code', how='inner', suffixes=('', '_x'))

        # sort by the total_mv 总市值, decending.
        df_circ_mv_market_cap = self.add_stock_name_to_df(df=df_basic, ts_pro_format=False)
        df_circ_mv_market_cap.to_csv(mktcap_csv, encoding='UTF-8', index=False)
        logging.info("\nsaved to "+mktcap_csv)


        logging.info("10 biggest average daily CIRC MARKET CAP(流通总市值) stocks in " + str(ndays) + " days:")
        logging.info(df_circ_mv_market_cap.head(10))

        return(df_circ_mv_market_cap)


    def get_last_n_days_stocks_amount(self,ndays=365, dayS=None, dayE=None, daily_update=None,debug=False, force_run=False):
    # def get_last_n_days_stocks_amount(self,ndays=365):

        #logic for dayS and dayE:
        if (dayS is not None ) and (dayE is not None):
            ndays = (datetime.strptime(dayE, "%Y%m%d") - datetime.strptime(dayS, "%Y%m%d")).days+1
            logging.info("get_last_n_days_stocks_amount, using specifed dayS and dayE, ingore ndays. Use calculated ndays "+str(ndays))
        elif (dayE is not None) and (ndays is not None):
            dayS = (datetime.today() - timedelta(365)).strftime("%Y%m%d")
        elif (dayS is None) and (dayE is None) and (ndays is not None):
            dayS = (datetime.today() - timedelta(ndays)).strftime("%Y%m%d")
            dayE = datetime.today().strftime("%Y%m%d")
        else:
            logging.fatal("unsupported input parameter. exit")
            sys.exit(1)

        if daily_update:
            sl_out_csv = "/home/ryan/DATA/result/stocks_amount_" + str(ndays) + "_days.csv" #symbol link
            sl_daily_ma_koudi_csv = "/home/ryan/DATA/result/latest_ma_koudi.csv"
            daily_ma_koudi_csv = "/home/ryan/DATA/result/ma_koudi_" + dayS + '_' + dayE + ".csv"


        logging.info("dayS "+dayS+", dayE "+ dayE+", ndays "+str(ndays))

        if ndays < 60: #because we need calculate 60 MA/ 60 koudi later.
            logging.info("Ndays must great than 60")
            sys.exit(1)

        out_csv = "/home/ryan/DATA/result/stocks_amount_" + dayS+"_"+dayE+ ".csv"

        # if self.is_cached(file_path=out_csv, day=7) and (not debug) and (datetime.today() > datetime.strptime(dayE, "%Y%m%d")):
        if self.is_cached(file_path=out_csv, day=7) and (not force_run):
            logging.info("get_last_n_days_stocks_amount, loading cached file " + out_csv)
            df_amt = pd.read_csv(out_csv)
        else:
            df_amt = pd.DataFrame()
            df = self.get_A_stock_instrment()

            if debug:
                df = df.head(50)

            df = self.add_market_to_code(df) #df has all the stocks on market

            i = 0
            for index, row in df.iterrows():
                i += 1
                name, code = row['name'], row['code']
                csv = "/home/ryan/DATA/DAY_Global/AG/" + code + ".csv"
                if not os.path.exists(csv):
                    logging.error("file not exists, " + csv)
                    continue

                # with open(csv, 'r') as f:
                #     q = deque(f, ndays)  # read tail ndays lines
                #
                # df_sub = pd.read_csv(StringIO(''.join(q)), header=None)

                df_sub = self.regular_read_csv_to_stdard_df(csv)
                df_sub = df_sub[(df_sub['date'] >= dayS) & (df_sub['date'] < dayE)]

                df_sub = finlib_indicator.Finlib_indicator().add_ma_ema(df_sub, short=5, middle=21, long=55)

                df_amt = df_amt.append(df_sub)
                logging.info(str(i)+" of " +str(df.__len__())+" "+name + " " + code + ",  append " + str(df_sub.__len__()) + " lines.")

            df_amt.to_csv(out_csv, encoding='UTF-8', index=False)
            logging.info("df_amt saved to " + out_csv + ", len " + str(df_amt.__len__()))

    # daily update, check every day.
        if daily_update:
            if os.path.exists(sl_out_csv):
                os.unlink(sl_out_csv)

            os.symlink(out_csv, sl_out_csv)
            logging.info("\ndaily_update, the latest N days amount symbol link created. "+sl_out_csv+" --> "+out_csv)

            df_ma_koudi = df_amt[df_amt['date'] == df_amt['date'].max()].reset_index().drop('index', axis=1)
            df_ma_koudi['Tmr_Min_Inc_To_Get_MA5_Up'] = round(((df_ma_koudi['p_ma_dikou_5'] - df_ma_koudi['close'] )*100.0/df_ma_koudi['close']),2)
            df_ma_koudi['Tmr_Min_Inc_To_Get_MA21_Up'] = round(((df_ma_koudi['p_ma_dikou_21'] - df_ma_koudi['close'] )*100.0/df_ma_koudi['close']),2)
            df_ma_koudi['Tmr_Min_Inc_To_Get_MA55_Up'] = round(((df_ma_koudi['p_ma_dikou_55'] - df_ma_koudi['close'] )*100.0/df_ma_koudi['close']),2)
            df_ma_koudi['reason'] = ';'

            df_ma_koudi.loc[df_ma_koudi['Tmr_Min_Inc_To_Get_MA5_Up']>5, ['reason']] += constant.MA5_UP_KOUDI_DISTANCE_GT_5+";"
            df_ma_koudi.loc[df_ma_koudi['Tmr_Min_Inc_To_Get_MA21_Up']>5, ['reason']] += constant.MA21_UP_KOUDI_DISTANCE_GT_5+";"
            df_ma_koudi.loc[df_ma_koudi['Tmr_Min_Inc_To_Get_MA55_Up']>5, ['reason']] += constant.MA55_UP_KOUDI_DISTANCE_GT_5+";"


            df_ma_koudi.loc[(df_ma_koudi['Tmr_Min_Inc_To_Get_MA5_Up']>0) & (df_ma_koudi['Tmr_Min_Inc_To_Get_MA5_Up']<=1), ['reason']] += constant.MA5_UP_KOUDI_DISTANCE_LT_1+";"
            df_ma_koudi.loc[(df_ma_koudi['Tmr_Min_Inc_To_Get_MA21_Up']>0) & (df_ma_koudi['Tmr_Min_Inc_To_Get_MA21_Up']<=1), ['reason']] += constant.MA21_UP_KOUDI_DISTANCE_LT_1+";"
            df_ma_koudi.loc[(df_ma_koudi['Tmr_Min_Inc_To_Get_MA55_Up']>0) & (df_ma_koudi['Tmr_Min_Inc_To_Get_MA55_Up']<=1), ['reason']] += constant.MA55_UP_KOUDI_DISTANCE_LT_1+";"


            df_ma_koudi.loc[(df_ma_koudi['two_week_fluctuation_sma_short_5']<3), ['reason']] += constant.TWO_WEEK_FLUC_SMA_5_LT_3+";"
            df_ma_koudi.loc[(df_ma_koudi['two_week_fluctuation_sma_middle_21']<3), ['reason']] += constant.TWO_WEEK_FLUC_SMA_21_LT_3+";"
            df_ma_koudi.loc[(df_ma_koudi['two_week_fluctuation_sma_long_55']<3), ['reason']] += constant.TWO_WEEK_FLUC_SMA_55_LT_3+";"


            df_ma_koudi = self.add_stock_name_to_df(df=df_ma_koudi)

            col_name_list = ['date', 'code', 'name','close', 'reason',
                        'Tmr_Min_Inc_To_Get_MA5_Up',	'Tmr_Min_Inc_To_Get_MA21_Up',	'Tmr_Min_Inc_To_Get_MA55_Up',
                        'two_week_fluctuation_sma_short_5',	'two_week_fluctuation_sma_middle_21',	'two_week_fluctuation_sma_long_55',
                        'close_5_sma',		'close_21_sma',		'close_55_sma',
                        'p_ma_dikou_5',	'p_ma_dikou_21',	'p_ma_dikou_55',]

            df_ma_koudi = self.adjust_column(df=df_ma_koudi, col_name_list=col_name_list)

            df_ma_koudi.to_csv(daily_ma_koudi_csv, encoding='UTF-8', index=False)
            logging.info("\nMA/Koudi saved to "+daily_ma_koudi_csv+" , len "+str(df_ma_koudi.__len__()))

            if os.path.exists(sl_daily_ma_koudi_csv):
                os.unlink(sl_daily_ma_koudi_csv)

            os.symlink(daily_ma_koudi_csv, sl_daily_ma_koudi_csv)
            logging.info("\nthe latest koudi symbol link created. " + sl_daily_ma_koudi_csv + " --> " + daily_ma_koudi_csv)

        return(df_amt)

    # 对样本空间内证券按照过去一年的日均成交金额由高到低排名
    def sort_by_amount_since_n_days_avg(self, ndays=None, period_start=None, period_end=None, debug=False, df_parent=None, daily_update=False,force_run=False):
        # this file contains all the stocks. No filter <<< No.

        if period_end is None:
            period_end = datetime.today().strftime("%Y%m%d")

        if period_start is None:
            period_start = (datetime.strptime(period_end,"%Y%m%d") - timedelta(days=ndays)).strftime("%Y%m%d")


        amt_csv = "/home/ryan/DATA/result/average_daily_amount_sorted_"+str(period_start)+"_"+str(period_end)+".csv"
        
        # if (not force_run) and self.is_cached(file_path = amt_csv, day = 7) and (datetime.today() > datetime.strptime(period_end, "%Y%m%d")):
        #     logging.info("read result from "+amt_csv)
        #     return(pd.read_csv(amt_csv))

        df_amt = self.get_last_n_days_stocks_amount(ndays=ndays, dayS=period_start, dayE=period_end, daily_update=daily_update, debug=debug, force_run=force_run)
        df_amt = self.regular_df_date_to_ymd(df_amt)

        if debug:
            # ndays = 30
            df_amt = df_amt.head(50)

        if df_parent is not None:
            df_amt = pd.merge(df_parent,df_amt, on='code', how='inner', suffixes=('', '_x'))

        the_latest_date = df_amt['date'].unique().max() # the latest date, e.g '20210107'

        # df_amt = df_amt[(df_amt['date'] >= period_start) & (df_amt['date'] <= period_end)] #Can be removed

        # amount 成交额(元)
        df_amt = df_amt.groupby(by='code').mean().sort_values(by=['amount'], ascending=[False],
                                                              inplace=False).reset_index()

        # amount rank
        df_amt['amount_perc'] = df_amt['amount'].apply(lambda _d: round(stats.percentileofscore(df_amt['amount'], _d) / 100, 4))

        df_amt['amount'] = df_amt['amount'].apply(lambda _d: round(_d,0))


        df_amt['date']=str(the_latest_date)

        df_amt = self.add_stock_name_to_df(df=df_amt, ts_pro_format=False)
        df_amt.to_csv(amt_csv, encoding='UTF-8', index=False)
        logging.info("saved to "+amt_csv)

        logging.info("10 biggest average daily AMOUNT(成交额) Stocks in " + str(ndays) + " days, dayS "+period_start+" dayE "+period_end+" :")
        logging.info(df_amt.head(10))

        return(df_amt)


    def get_index_candidate(self, index_name):
        # output of t_daily_hs300_candiate.py
        # hs300_candidate_list.csv  sz100_candidate_list.csv    szcz_candidate_list.csv   zz100_candidate_list.csv
        csv_f = "/home/ryan/DATA/result/"+index_name+"_candidate_list.csv"
        if not self.is_cached(file_path=csv_f, day=7):
            logging.error("file not exist, or empty, or not updated in 7 days. "+csv_f)
            sys.exit(1)

        df = pd.read_csv(csv_f)
        df_keep =  df[df['predict'] == constant.TO_BE_KEPT].reset_index().drop('index', axis=1)
        df_remove =  df[df['predict'] == constant.TO_BE_REMOVED].reset_index().drop('index', axis=1)
        df_add =  df[df['predict'] == constant.TO_BE_ADDED].reset_index().drop('index', axis=1)
        return({
            "index_name":index_name,
            "df_keep":df_keep,
            "df_remove":df_remove,
            "df_add":df_add,
        })



    def load_index(self, index_code, index_name, force_run=False):
        token = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'

        pro = ts.pro_api(token=token)

        # ts.set_token(token=token)

        csv_index = "/home/ryan/DATA/pickle/"+index_name+".csv"


        if self.is_cached(file_path=csv_index, day=7) and (not force_run):
            df_index_latest = pd.read_csv(csv_index, converters={'code': str, 'date': str})
            logging.info("loaded index from " + csv_index + " ,len " + str(df_index_latest.__len__()))
        else:
            # df_indices = pro.index_basic() #contains HS100, ZZ100/200/500/700
            # #
            # # df_zz100 = pro.index_weight(index_code='000903.SH')  # ZZ100
            # # df_zz200 = pro.index_weight(index_code='000904.SH')  # ZZ200
            # # df_zz500 = pro.index_weight(index_code='000905.SH')  # ZZ500
            # # df_hs300 = pro.index_weight(index_code='000300.SH')  # HS300
            logging.info("fetching index_weight from tushare, index_code "+index_code)
            df_index = pro.index_weight(index_code=index_code)  # HS300

            df_index.columns = ['index_code', 'code', 'date', 'weight']  # rename original df column names
            df_index = df_index[['code', 'date', 'weight']]


            index_latest_period = str(df_index.date.unique().max())  # '20201201
            df_index_latest = df_index[df_index['date'] == index_latest_period]
            df_index_latest = df_index_latest.sort_values(by=['weight'], ascending=[False], inplace=False).reset_index().drop('index', axis=1)

            df_index_latest = self.ts_code_to_code(df=df_index_latest)
            df_index_latest = self.add_stock_name_to_df(df=df_index_latest)

            print(self.pprint(df_index_latest.head(2)))
            logging.info("got index "+index_name +" list of period " + str(index_latest_period)+ ", len "+str(df_index_latest.__len__()))

            df_index_latest.to_csv(csv_index, encoding='UTF-8', index=False)
            logging.info("latest index save to " + csv_index + " ,len " + str(df_index_latest.__len__()))


        return (df_index_latest)

    def add_stock_name_to_df(self, df, ts_pro_format=False):
        # add stock name
        # if ts_pro_format:
        #     df=self.ts_code_to_code(df)

        name_df = self.regular_read_csv_to_stdard_df(data_csv="/home/ryan/DATA/pickle/instrument_A.csv")
        name_df = name_df[['code','name']]

        if ts_pro_format:
            name_df = self.add_market_to_code(df=df,dot_f=True,tspro_format=True)
            df = pd.merge(df, name_df, left_on=['ts_code'], right_on=['code'], how="left")
            df = self.adjust_column(df, ['ts_code', 'name'])
        else:
            df = pd.merge(df, name_df, on=['code'], how="left",suffixes=('','_x'))
            df = self.adjust_column(df, ['code', 'name'])

        return(df)


    def add_stock_name_to_df_us_hk(self, df, market='US'):
        market = market.upper()
        name_csv = "/home/ryan/DATA/result/wei_pan_la_sheng"+ "/" + market + "_spot_link.csv"
        name_df = pd.read_csv(name_csv, encoding="utf-8", converters={'code': str})
        name_df = name_df[['code','name']]

        df = pd.merge(df, name_df, on=['code'], how="left",suffixes=('','_x'))
        df = self.adjust_column(df, ['code', 'name'])

        return(df)






    def count_min_max_value_days(self,df,col_name):
        latest_close = df[col_name].iloc[-1]

        this_is_min_of_last_n_records = 0
        this_is_max_of_last_n_records = 0

        if df.__len__() > 2:
            for i in range(2, df.__len__()):
                if latest_close >= df[col_name].iloc[-1 * i]:
                    this_is_max_of_last_n_records = i
                else:
                    break

            for i in range(2, df.__len__()):
                if latest_close <= df[col_name].iloc[-1 * i]:
                    this_is_min_of_last_n_records = i
                else:
                    break

        logging.info("this_is_max_of_last_n_records of column "+col_name+ " , " + str(this_is_max_of_last_n_records))
        logging.info("this_is_min_of_last_n_records of column "+col_name+ " , " + str(this_is_min_of_last_n_records))

        return({'this_is_max_of_last_n_records':this_is_max_of_last_n_records,
                'this_is_min_of_last_n_records':this_is_min_of_last_n_records})


   #up-volume equal to or greater than the largest down-volume day over the prior 10 days
    def pocket_pivot_check(self,df):
        if df.__len__() < 2:
            return(False)

        #check price critiria
        if ((df['close'].iloc[-2] < df['close_50_sma'].iloc[-2]) and (df['close'].iloc[-1] > df['close_50_sma'].iloc[-1]))  or  ( (df['close'].iloc[-2] < df['close_15_sma'].iloc[-2]) and (df['close'].iloc[-1] > df['close_15_sma'].iloc[-1])):
            logging.info("Pocket Pivot Price condition satisfied. P break SMA15 or SMA50")
        else:
            logging.info("Pocket Pivot Price condition failed.")
            return(False)

        #next check volume critiria
        latest_volume = df['volume'].iloc[-1]

        pocket_pivot_this_vol_gt_N_records_of_down_vol = 0



        for i in range(2, df.__len__()):
            if df['close_-1_d'].iloc[-1*i] < 0: #price was down that day
                if latest_volume >= df['volume'].iloc[-1 * i]:
                    pocket_pivot_this_vol_gt_N_records_of_down_vol += 1
                else:
                    break
            else:
                pocket_pivot_this_vol_gt_N_records_of_down_vol += 1

        logging.info("this_vol_gt_N_records_of_down_vol "+ str(pocket_pivot_this_vol_gt_N_records_of_down_vol))

        return({'pocket_pivot_this_vol_gt_N_records_of_down_vol':pocket_pivot_this_vol_gt_N_records_of_down_vol})

    #return True if AG market is open
    def is_market_open_ag(self):
        hour = datetime.now().hour
        minute = datetime.now().minute

        rtn = False
        if (hour > 9) and (hour < 15): #10 ~ 14.59
            rtn = True
        elif hour==9 and minute>=30: #9.30 ~ 9.59
            rtn = True

        return(rtn)

    #return True if HK market is open
    def is_market_open_hk(self):
        hour = datetime.now().hour
        minute = datetime.now().minute

        rtn = False
        if (hour >= 9) and (hour <= 16): #9 ~ 15.59
            rtn = True
        return(rtn)

    #return True if US market is open
    def is_market_open_us(self):
        hour = datetime.now().hour
        minute = datetime.now().minute

        rtn = True
        if (hour > 9) and (hour <= 16): #10.00 ~ 16.59
            rtn = False
        return(rtn)

#https://www.mytecbits.com/internet/python/week-number-of-month
    def week_number_of_month(self,date_value):
        #return (date_value.isocalendar()[1] - date_value.replace(day=1).isocalendar()[1]  )

        today_week_of_year = date_value.isocalendar()[1]
        month_1st_week_of_year = date_value.replace(day=1).isocalendar()[1]

        if today_week_of_year <= 5 and month_1st_week_of_year >= 52:
            rtn = today_week_of_year
        else:
            rtn = today_week_of_year - month_1st_week_of_year +1

        return(rtn)

    def get_ak_live_price(self,stock_market='AG',allow_delay_min=15,force_fetch=False):
        ########################
        # stock_market in AG, US, HK
        #################
        stock_market = stock_market.upper()
        b = "/home/ryan/DATA/result/wei_pan_la_sheng"

        if not os.path.isdir(b):
            os.mkdir(b)

        nowS = datetime.now().strftime('%Y%m%d_%H%M')  # '20201117_2003'
        date_cn = datetime.now().strftime('%Y%m%d')
        date_us = (datetime.now()-timedelta(1)).strftime('%Y%m%d')

        # run at 14:55 (run_time). Find the stocks increase fastly since 14:00 or 14:30 to run_time.
        a_spot_csv = b + "/" + stock_market + "_spot_" + nowS + ".csv"
        a_spot_csv_link = b + "/" + stock_market + "_spot_link.csv"
        a_spot_csv_link_old = b + "/" + stock_market + "_spot_link_old.csv"

        if self.is_cached(file_path=a_spot_csv_link, day=1 / 24 / 60 * allow_delay_min) and (not force_fetch):  # cached in 15 minutes
            stock_spot_df = pd.read_csv(a_spot_csv_link, encoding="utf-8", converters={'code': str})
            logging.info("loading " + stock_market + " spot df from " + a_spot_csv_link)
            return(stock_spot_df)
        else:

            if stock_market == 'AG':
                # 获取 A 股实时行情数据. 单次返回所有 A 股上市公司的实时行情数据
                # A 股数据是从新浪财经获取的数据, 重复运行本函数会被新浪暂时封 IP, 建议增加时间间隔
                stock_spot_df = ak.stock_zh_a_spot().drop_duplicates()

                # 获取科创板实时行情数据. 单次返回所有科创板上市公司的实时行情数据
                # 从新浪财经获取科创板股票数据
                stock_zh_kcb_spot_df = ak.stock_zh_kcb_spot().drop_duplicates()

                # Merge KCB to AG
                stock_spot_df = pd.concat([stock_spot_df, stock_zh_kcb_spot_df]).reset_index().drop('index', axis=1)
                stock_spot_df = stock_spot_df.drop('code', axis=1)
                stock_spot_df = stock_spot_df.rename(columns={
                    "symbol": "code", "trade": "close",
                }, inplace=False)
                stock_spot_df['code'] = stock_spot_df['code'].apply(lambda _d: _d.upper())
                stock_spot_df['date'] = date_cn

            elif stock_market == 'HK':
                stock_spot_df = ak.stock_hk_spot().drop_duplicates()  # 获取港股的实时行情数据
                stock_spot_df['symbol'] = stock_spot_df['symbol'].apply(lambda _d: str(_d).zfill(5))
                stock_spot_df = stock_spot_df.rename(columns={
                    "symbol": "code", "lasttrade": "close",
                }, inplace=False)
                stock_spot_df['code'] = stock_spot_df['code'].apply(lambda _d: _d.upper())
                stock_spot_df['date'] = date_cn

            elif stock_market == 'US':
                stock_spot_df = ak.stock_us_spot().drop_duplicates()  # 获取美股行情报价
                stock_spot_df = stock_spot_df.rename(columns={
                    "symbol": "code", "price": "close",
                }, inplace=False)
                stock_spot_df['code'] = stock_spot_df['code'].apply(lambda _d: _d.upper())
                stock_spot_df['date'] = date_us

            stock_spot_df = self.adjust_column(df=stock_spot_df, col_name_list=['date','code'])
            stock_spot_df.to_csv(a_spot_csv, encoding='UTF-8', index=False)
            self.pprint(stock_spot_df.head(3))
            logging.info(stock_market + " spot saved to " + a_spot_csv)

            if os.path.lexists(a_spot_csv_link_old):
                os.unlink(a_spot_csv_link_old)
                logging.info("removed previous old link " + a_spot_csv_link_old)

            if os.path.lexists(a_spot_csv_link):
                os.rename(a_spot_csv_link, a_spot_csv_link_old)
                logging.info("renamed previous new link to old link, to " + a_spot_csv_link_old)

            os.symlink(a_spot_csv, a_spot_csv_link)
            logging.info(__file__ + ": " + "symbol link created  " + a_spot_csv_link + " -> " + a_spot_csv)

            stock_spot_df = pd.read_csv(a_spot_csv_link, encoding="utf-8", converters={'code': str})
            return(stock_spot_df)

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def get_roe_div_pe(self, market='AG'):
        if market=='AG':
            df_fund = self.load_all_ts_pro(debug=False)
            df_fund = df_fund[df_fund['end_date'] == self.get_report_publish_status()['completed_year_rpt_date']]
            df_daily = self.get_last_n_days_daily_basic(ndays=1, dayE=self.get_last_trading_day())

            df = pd.merge(df_fund, df_daily, left_on='ts_code', right_on='ts_code')
            df['roe_pe'] = df['roe'] / df['pe_ttm']
            df_target = df[['ts_code', 'name', 'roe_pe', 'pe_ttm', 'roe']].sort_values(by='roe_pe', ascending=False)
            df_target = self.ts_code_to_code(df=df_target)
            print(self.pprint(df_target.head(100)))

        if market=='US':
            df = self.load_tv_fund(market='US',period='d')
            df['roe_pe'] = df['roe_ttm'] / df['pe_ttm']
            df_target = df[['code', 'name','roe_pe', 'roe_ttm', 'pe_ttm']].sort_values(by='roe_pe', ascending=False)
            print(self.pprint(df_target.head(100)))

        return(df_target)

    #input: na
    #output:
    def load_tv_fund(self, market='US', period='d'):

        if market == 'US':
            csv_f = "/home/ryan/DATA/pickle/Stock_Fundamental/TradingView/america_latest_"+period+".csv"
        elif market == 'AG':
            csv_f = "/home/ryan/DATA/pickle/Stock_Fundamental/TradingView/china_latest_"+period+".csv"


        df = pd.read_csv(csv_f)
        df = df.fillna(0)

        tv_col_name_dict = constant.TRADINGVIEW_COLS

        for c in df.columns:
            if c in tv_col_name_dict.keys() and tv_col_name_dict[c] != "xxxx":
                df.rename(columns={c: tv_col_name_dict[c] }, inplace=True)

        df = self.add_stock_name_to_df_us_hk(df=df, market=market)
        return(df)

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass


