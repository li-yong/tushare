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

        output_pickle = "/home/ryan/DATA/result/jaqs/ts_all.pickle"

        logging.info(__file__+" "+"consolidate ts_pro to a df requires lots of memory, > 500M will be consumed.")

        path = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged"

        if debug:
            path = path + ".dev"

        if self.is_cached(output_pickle, day=7) and (not overwrite):
            logging.info(__file__+" "+"load tushare pro all from " + output_pickle)
            df_all_ts_pro = pd.read_pickle(output_pickle)
            return (df_all_ts_pro)

        if not os.path.isdir(path):
            logging.info('path not exist ' + path)
            exit()

        #allFiles = glob.glob(path + "/*.csv")
        allFiles = glob.glob(path + "/*201[0-9]*.csv")
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

    def get_today_stock_basic(self):
        # todayS = datetime.today().strftime('%Y-%m-%d')
        todayS = self.get_last_trading_day()

        # csv_basic = "/home/ryan/DATA/tmp/basic_" + todayS + ".csv"  ##get_stock_basics每天都会更新一次
        csv_basic = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_" + todayS + ".csv"  ##get_stock_basics每天都会更新一次

        df_basic = pd.DataFrame()

        if not os.path.isfile(csv_basic):
            logging.info(__file__+" "+"Getting Basic, ts.get_stock_basics of " + todayS)  # 获取沪深上市公司基本情况
            try:
                df_basic = ts.get_stock_basics()
                df_basic = df_basic.reset_index()
                df_basic.code = df_basic.code.astype(str)  # convert the code from numpy.int to string.
                df_basic.reset_index().to_csv(csv_basic, encoding='UTF-8', index=False)
            except:
                logging.info(__file__+" "+"exception in get_today_stock_basic()" + str(e))
            finally:
                if sys.exc_info() == (None, None, None):
                    pass  # no exception
                else:
                    logging.info(str(traceback.print_exception(*sys.exc_info())).encode('utf8'))  # python3
                    # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8')) #python2
                    logging.info(sys.exc_value.message)  # print the human readable unincode
                    sys.exc_clear()
        else:
            # logging.info(__file__+" "+"\nLoading Basic")
            df_basic = pandas.read_csv(csv_basic, converters={'code': str})

        return (df_basic)

    def df_filter(self, df):
        # code in df: the code must in format 'SH600xxx' etc

        df_basic = self.get_today_stock_basic()
        cols = ['code', 'name', 'esp', 'npr', 'timeToMarket']
        df_basic = df_basic[cols]
        # logging.info(__file__+" "+"df_basic.__len__() is " + str(df_basic.__len__()))

        df_basic = df_basic.fillna(0)
        df_basic = df_basic[df_basic['timeToMarket'] != 0]

        a = datetime.now() - timedelta(360)
        b = a.date().strftime('%Y%m%d')  # '20170505'
        df_basic = df_basic[df_basic['timeToMarket'] < int(b)]
        # logging.info(__file__+" "+"after timetoMarket>360, df_basic.__len__() is " + str(df_basic.__len__()))

        df_basic = df_basic[df_basic['esp'] > 0]
        # logging.info(__file__+" "+"after esp>0, df_basic.__len__() is " + str(df_basic.__len__()))

        df_basic = df_basic[df_basic['npr'] > 0]
        # logging.info(__file__+" "+"after npr>0, df_basic.__len__() is " + str(df_basic.__len__()))

        df_basic = self.add_market_to_code(df=df_basic)  # the code must in format 'SH600xxx' etc

        # a= pd.merge(df_basic,df, on='code',how='inner')
        # logging.info(str(a.__len__()))

        logging.info(__file__+" "+"input df len " + str(df.__len__()) + ". ")

        d = df_basic['code'].tolist()
        df = df[df['code'].isin(d)]

        logging.info(__file__+" "+"after filter(timetomarket>360, esp>0, npr>0), df len " + str(df.__len__()))

        return df

        if False:
            for i in range(df_basic.__len__()):
                code = str(df_basic.iloc[i]['code'])
                timeToMarket = str(df_basic.iloc[i]['timeToMarket'])

                if pd.isnull(timeToMarket):
                    logging.info(__file__+" "+"code " + code + " timetomarket is " + str(timeToMarket))
                    continue
                elif timeToMarket == '0':
                    logging.info(__file__+" "+"code " + code + " timetomarket is " + str(timeToMarket))
                    continue
                elif datetime.strptime(timeToMarket, '%Y%m%d') + timedelta(360) > datetime.now():
                    logging.info(__file__+" "+"the stock " + str(code) + " is less than 1 year, " + str(timeToMarket))
                    pass

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
        while i >= 2010:
            i = i - 1
            full_period_list.append(str(i) + "0331")
            full_period_list.append(str(i) + "0630")
            full_period_list.append(str(i) + "0930")
            full_period_list.append(str(i) + "1231")
            full_period_list_yearly.append(str(i) + "1231")

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

    def get_A_stock_instrment(self, today_df=None, debug=False, force_update=False):  # return 3515 records

        df = pd.DataFrame()

        if debug:
            instrument_csv = "/home/ryan/DATA/pickle/instrument_A.csv.debug"
        else:
            instrument_csv = "/home/ryan/DATA/pickle/instrument_A.csv"

        if os.path.isfile(instrument_csv) and (not force_update):
            df = pd.read_csv(instrument_csv, converters={'code': str})
        else:
            print("file not exist. " + instrument_csv)
            exit()

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

    def add_market_to_code_single(self, code):
        code_S = code
        if re.match('^6', code):
            code_S = "SH" + code
        elif re.match('^[0|3]', code):
            code_S = "SZ" + code
        elif re.match('^[9]', code):  # B Gu
            pass
            #logging.info(("ignore B GU " + code))
        elif re.match('^SH', code):  #
            code_S = code
        elif re.match('^SZ', code):  #
            code_S = code
        else:
            pass
            #logging.info(("Fatal: UNKNOWN CODE " + code))
        return code_S

    def add_market_to_code(self, df, dot_f=False, tspro_format=False):

        # tspro_format : 600000.SH

        dot = ''

        if dot_f == True:
            dot = '.'

        # support the column name in df is 'code'
        for index, row in df.iterrows():
            code = row['code']
            # print index
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
                df.at[index, 'code'] = code_S2
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

        for i in range(collist.__len__()):
            if collist[i] == 'ts_code':
                collist[i] = 'code'

        df.columns = collist  # apply the new columns name to df. rename columns

        for index, row in df.iterrows():
            code = row['code']

            dcode = re.match('(\d{6})\.(.*)', code).group(1)  # group(1):600000,
            mkt = re.match('(\d{6})\.(.*)', code).group(2)  # group(2):SH

            df.at[index, 'code'] = mkt + dcode

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

        csv_f = "/home/ryan/DATA/pickle/trading_day_2020.csv"

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

        csv_f = "/home/ryan/DATA/pickle/trading_day_2020.csv"

        if not os.path.isfile(csv_f):
            # logging.info(__file__+" "+"downloading trading day data")
            a = ts.trade_cal()
            # a.to_pickle(dump)
            a.to_csv(csv_f, encoding='UTF-8', index=False)
        else:
            # logging.info(__file__+" "+"loading trading day data")
            # a = pandas.read_pickle(dump)
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
        csvf = "/home/ryan/DATA/pickle/trading_day_2020.csv"
        df_trade_cal = ts.pro_api().trade_cal(exchange='SSE', start_date='19980101', end_date='20201231')
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
        # basic_df = get_pro_basic()

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

        if os.stat(file_path).st_size <= 10:
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

        if rem_D6DotC2:
            code = rem_D6DotC2.group(1)
            mkt = rem_D6DotC2.group(2)
            code_format = "D6.C2"

        if rem_C2D6:
            code = rem_D6DotC2.group(2)
            mkt = rem_D6DotC2.group(1)
            code_format = "C2D6"

        dict = {'code': code, 'mkt': mkt, 'format': code_format}

        dict['D6.C2'] = dict['code'] + "." + dict['mkt']
        dict['C2D6'] = dict['mkt'] + dict['code']

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

    def remove_garbage(self, df, code_filed_name, code_format):
        # code_filed_name in code, ts_code
        # code_format in "D6.C2", "C2D6"

        df_init_len = df.__len__()

        stable_report_perid = self.get_year_month_quarter()['stable_report_perid']

        f = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step1/rpt_" + stable_report_perid + ".csv"

        df_garbage = pd.read_csv(f, converters={'end_date': str})
        df_garbage = df_garbage[['stopProcess', 'ts_code', 'name', 'end_date']]
        df_garbage = df_garbage[df_garbage['stopProcess'] == 1].reset_index().drop('index', axis=1)

        garbage_cnt = df_garbage.__len__()

        # determin input df code format

        for i in range(garbage_cnt):
            garbage_ts_code = df_garbage.iloc[i]
            garbage_ts_code = garbage_ts_code['ts_code']  # 600519.SH

            dict = self.get_code_format(garbage_ts_code)

            code_target = dict[code_format]
            df = df[df[code_filed_name] != code_target]

        if "level_0" in df.columns:
            df = df.drop('level_0', axis=1)

        if "index" in df.columns:
            df = df.drop('index', axis=1)

        if "Unnamed: 0" in df.columns:
            df = df.drop('Unnamed: 0', axis=1)

        df = df.reset_index().drop('index', axis=1)
        df_after_len = df.__len__()
        logging.info(str(df_init_len) + '->' + str(df_after_len) + ". removed " + str(df_init_len - df_after_len) + " garbage stocks.")
        return (df)

    # convert daily df to monthly df with resample/reshape
    def daily_to_monthly_bar(self, df_daily):

        df_daily = self.regular_column_names(df_daily)

        df_daily['date'] = pd.to_datetime(df_daily['date'], format="%Y-%m-%d")

        df_daily = df_daily.reset_index().set_index('date')

        ####
        logic = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum',
            'tnv': 'mean',
        }

        # df_weekly = df.resample('W', loffset=pd.offsets.timedelta(days=-6)).apply(logic)
        # day_of_this_month = monthrange(datetime.datetime.today().year, datetime.datetime.today().month)[1]
        # left_days_in_current_month = datetime.datetime.today().day - day_of_this_month
        # df_monthly = df.resample('M', loffset=pd.offsets.timedelta(days=left_days_in_current_month)).apply(logic)

        df_weekly = df_daily.resample('W').apply(logic).reset_index()
        df_monthly = df_daily.resample('M').apply(logic).reset_index()

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
            self.pprint(df.head(2))

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
    def regular_read_csv_to_stdard_df(self, data_csv,add_market=True):
        base_dir = "/home/ryan/DATA/DAY_Global"
        data_csv = str(data_csv)
        rtn_df = pd.DataFrame()

        data_csv_fp = os.path.abspath(data_csv)
        dir = os.path.dirname(data_csv_fp)

        if not os.path.isfile(data_csv_fp):
            logging.fatal(__file__+" "+"file not exist. " + data_csv_fp)
            exit(0)

        if dir == base_dir + "/AG":
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, header=None, skiprows=1, names=['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'tnv'])
        elif dir in [base_dir + "/stooq/US_INDEX", base_dir + "/stooq/US"]:
            #DOW.csv  SP500.csv, AAPL.csv
            add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir == base_dir + "/US":
            add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir == base_dir + "/HK":
            add_market = False
            rtn_df = pd.read_csv(data_csv_fp, converters={'code': str, 'date': str}, encoding="utf-8")
        elif dir == base_dir + "/AG_INDEX":
            rtn_df = pd.read_csv(data_csv_fp, skiprows=1, header=None, names=['code', 'date', 'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'], converters={'code': str, 'date': str}, encoding="utf-8")
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

            rtn_df['code'] = rtn_df['code'].apply(lambda _d: str(_d).upper())

        return (rtn_df)

    def pprint(self, df):
        print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))

    def get_stock_configuration(self, selected, stock_global):
        rtn = {
            "stock_list": None,
            "csv_dir": None,
            "out_dir": None,
        }

        if selected:
            selected_stocks = self.load_select()
            out_dir = "/home/ryan/DATA/result/selected"

            # INDEX first
            if stock_global == 'AG_INDEX':
                csv_dir = "/home/ryan/DATA/DAY_Global/AG_INDEX"
                stock_list = selected_stocks['CN_INDEX']
            elif stock_global == 'US_INDEX':
                csv_dir = "/home/ryan/DATA/DAY_Global/stooq/US_INDEX"
                stock_list = selected_stocks['US_INDEX']
            elif stock_global == "HK_INDEX":
                csv_dir = "/home/ryan/DATA/DAY_Global/HK_INDEX"
                stock_list = selected_stocks['HK_INDEX']

            # Then Stocks
            elif stock_global == "AG":
                csv_dir = "/home/ryan/DATA/DAY_Global/AG"
                stock_list = selected_stocks['CN']
            elif stock_global == 'HK':
                csv_dir = "/home/ryan/DATA/DAY_Global/HK"
                stock_list = selected_stocks['HK']
            elif stock_global == 'US':
                csv_dir = "/home/ryan/DATA/DAY_Global/stooq/US"
                stock_list = selected_stocks['US']
        else:  # selected == False
            if stock_global == 'AG':
                csv_dir = "/home/ryan/DATA/DAY_Global/AG"
                out_dir = "/home/ryan/DATA/result"
                stock_list = self.get_A_stock_instrment()  # 603999
                stock_list = self.add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
                stock_list = self.remove_garbage(stock_list, code_filed_name='code', code_format='C2D6')
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

        rtn = {
            "stock_list": stock_list,
            "csv_dir": csv_dir,
            "out_dir": out_dir,
        }

        return (rtn)

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

    #input: df [open,high, low, close]
    #output: {hit:[T|F], high:value, low:value, }
    def w_shape_exam(self, df):
        pass

