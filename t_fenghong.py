# coding: utf-8

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
import traceback
import sys
import tushare.util.conns as ts_cs
import finlib
from optparse import OptionParser

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

# This script to get fenghong history info
global force_run_global

the_latest_report_date = finlib.Finlib().get_year_month_quarter()['last_quarter']
year = the_latest_report_date['year']  #int 2018
quarter = the_latest_report_date['quarter']  #int 3

#todayS = datetime.datetime.today().strftime('%Y-%m-%d')
#todaySs = datetime.datetime.today().strftime('%Y%m%d')
todaySs = finlib.Finlib().get_last_trading_day()
todaySl = datetime.datetime.strptime(todaySs, '%Y%m%d').strftime('%Y-%m-%d')
todayS = todaySl

start = '2015-01-01'
end = todayS

#reference
profit_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_profit_all.csv"  #分配预案
profit_csv_debug = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_profit_000651.csv"
forecast_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_forecast_all.csv"  #业绩预告
fundhold_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_fundholding_all.csv"  #基金持股
xsg_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_xsg_all.csv"  #限售股解禁
new_stock_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_new_stock.csv"  #新股数据
sh_margin_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_sh_margin.csv"  #融资融券（沪市）
sz_margin_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_sz_margin.csv"  #融资融券（深市）

#output csv
profit_analysis_csv = "/home/ryan/DATA/result/fenghong_profit_analysis.csv"
fenghong_score_csv = '/home/ryan/DATA/result/fenghong_score.csv'


def get_reference_data():
    # ===== Reference ======#
    df_profit_all = pandas.DataFrame()
    df_forecast_all = pandas.DataFrame()
    df_fundholding_all = pandas.DataFrame()
    df_xsg_all = pandas.DataFrame()

    df_new_stock = pd.DataFrame()

    ######################## ts.new_stocks  ###################
    logging.info(__file__+" "+"getting new_stocks, ts.new_stocks")  # 新股数据
    if finlib.Finlib().is_cached(new_stock_csv, 3):
        df_new_stock = pd.read_csv(new_stock_csv, converters={'code': str})
    else:
        df_new_stock = ts.new_stocks()
        df_new_stock.to_csv(new_stock_csv, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + new_stock_csv)

    year_q = finlib.Finlib().get_year_month_quarter()

    #for year in range(1980, year_q['year']+1): #should not (2010,2019) as it will overwrite the 1980 to 2010 data.
    for year in range(2017, year_q['year'] + 1):  #should not (2010,2019) as it will overwrite the 1980 to 2010 data.
        #for year in range(2018,2019): #debug
        ######################## ts.profit_data  ###################
        logging.info(__file__+" "+'profit_data year ' + str(year) + ". ")
        sys.stdout.flush()

        dump_reference_profit_data = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/profit_data/pd_" + str(year) + ".csv"
        try:
            exc_info = sys.exc_info()
            df_tmp = pd.DataFrame()

            if (year > 2008) and ((not os.path.isfile(dump_reference_profit_data)) or (year == year_q['last_quarter']['year'] and quarter == year_q['last_quarter']['quarter'])):
                #if (year > 2008) and ((not os.path.isfile(dump_reference_profit_data))):
                #logging.info(__file__+" "+"\tGetting profit_data, ts.profit_data\t")  # 分配预案
                logging.info(__file__+" "+"getting profit_data, ts.profit_data. year " + str(year))
                sys.stdout.flush()
                df_tmp = ts.profit_data(year=year, top=30000, retry_count=10, pause=0.001)
                df_tmp.to_csv(dump_reference_profit_data, encoding='UTF-8', index=False)
                logging.info(__file__ + ": " + "saved, " + dump_reference_profit_data)

                #df_tmp.to_pickle(dump_reference_profit_data)
                #logging.info(df_tmp.__len__())
            elif os.path.isfile(dump_reference_profit_data):
                #logging.info(__file__+" "+"\tloading profit_data"+dump_reference_profit_data)
                df_tmp = pandas.read_csv(dump_reference_profit_data, converters={'code': str})

        except:
            logging.info(__file__+" "+"\tcaught exception, profit_data, year " + str(year) + ", " + dump_reference_profit_data)
            exit(0)
        finally:
            df_profit_all = df_profit_all.append(df_tmp, ignore_index=True)

            if exc_info == (None, None, None):
                pass  #no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info
            #logging.info('\tprofit_data year ' + str(year) + " end")
            #logging.info(__file__+" "+"\t---> df_profit_all len "+str(df_profit_all.__len__())) #update df all in every loop

            df_profit_all.to_csv(profit_csv, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "df_profit_all saved, " + profit_csv + " . len " + str(df_profit_all.__len__()))

        ######################## ts.forecast_data  ###################
        for quarter in range(1, 5):

            if (year == year_q['year']) and (quarter >= year_q['quarter']):
                continue

            #logging.info(__file__+" "+"\tforecast_data, year " + str(year) + " quarter " + str(quarter) + " start ")
            #sys.stdout.flush()
            dump_reference_forecast_data = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/forecast/fd_" + str(year) + "_" + str(quarter) + ".csv"
            try:
                exc_info = sys.exc_info()
                df_tmp = pd.DataFrame()

                if (year > 2010) and ((not os.path.isfile(dump_reference_forecast_data)) or (year == year_q['last_quarter']['year'] and quarter == year_q['last_quarter']['quarter'])):
                    #if (year > 2010) and ((not os.path.isfile(dump_reference_forecast_data))):
                    logging.info(__file__+" "+"getting forecast_data, ts.forecast_data, year " + str(year) + " quarter " + str(quarter))  # 业绩预告
                    sys.stdout.flush()
                    df_tmp = ts.forecast_data(year, quarter)
                    df_tmp.to_csv(dump_reference_forecast_data, encoding='UTF-8', index=False)
                    logging.info(__file__ + ": " + "saved, " + dump_reference_forecast_data)
                    #df_tmp.to_pickle(dump_reference_forecast_data)
                elif os.path.isfile(dump_reference_forecast_data):
                    #logging.info(__file__+" "+"\tloading forecast_data")
                    df_tmp = pandas.read_csv(dump_reference_forecast_data, converters={'code': str})

            except:
                logging.info(__file__+" "+"\tcaught exception, forecast_data, year " + str(year) + " quarter " + str(quarter))
            finally:
                df_forecast_all = df_forecast_all.append(df_tmp, ignore_index=True)

                if exc_info == (None, None, None):
                    pass  # no exception
                else:
                    traceback.print_exception(*exc_info)
                del exc_info
                #logging.info('\tforecast_data year ' + str(year) +" quarter "+str(quarter) + " end")
                #logging.info(__file__+" "+"\t---> df_forecast_all len " + str(df_forecast_all.__len__()))  # update df all in every loop
                df_forecast_all.to_csv(forecast_csv, encoding='UTF-8', index=False)
                logging.info(__file__ + ": " + "saved, " + forecast_csv + " . len " + str(df_forecast_all.__len__()))

        ########################  ts.fund_holdings ###################
        for quarter in range(1, 5):
            if (year == year_q['year']) and (quarter >= year_q['quarter']):
                continue

            #logging.info(__file__+" "+"\tfund_holdings, year "+str(year) + " quarter "+str(quarter)+ " start ")
            dump_reference_fund_holdings = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/fund_hold/fh_" + str(year) + "_" + str(quarter) + ".csv"
            try:
                exc_info = sys.exc_info()
                df_tmp = pd.DataFrame()

                if (year > 2010) and ((not os.path.isfile(dump_reference_fund_holdings)) or (year == year_q['last_quarter']['year'] and quarter == year_q['last_quarter']['quarter'])):
                    #if (year > 2010) and ((not os.path.isfile(dump_reference_fund_holdings))):
                    logging.info(__file__+" "+"\t\tgetting fund_holdings, ts.fund_holdings, year " + str(year) + " quarter " + str(quarter))  # 基金持股
                    sys.stdout.flush()
                    df_tmp = ts.fund_holdings(year, quarter)
                    df_tmp.to_csv(dump_reference_fund_holdings, encoding='UTF-8', index=False)
                    logging.info(__file__ + ": " + "saved, " + dump_reference_fund_holdings)

                    #df_tmp.to_pickle(dump_reference_fund_holdings)
                elif os.path.isfile(dump_reference_fund_holdings):
                    #logging.info(__file__+" "+"\tloading fund_holdings")
                    df_tmp = pandas.read_csv(dump_reference_fund_holdings, converters={'code': str})

            except:
                logging.info(__file__+" "+"\tcaught exception, fund_holdings, year " + str(year) + " quarter " + str(quarter))
            finally:
                df_fundholding_all = df_fundholding_all.append(df_tmp, ignore_index=True)

                if exc_info == (None, None, None):
                    pass  #no exception
                else:
                    traceback.print_exception(*exc_info)
                del exc_info
                #logging.info('\tfund_holdings year ' + str(year)  +" quarter "+str(quarter) + " end")
                #logging.info(__file__+" "+"\t---> df_fundholding_all len "+str(df_fundholding_all.__len__())) #update df all in every loop
                df_fundholding_all.to_csv(fundhold_csv, encoding='UTF-8', index=False)
                logging.info(__file__ + ": " + "saved, " + fundhold_csv + " . len " + str(df_fundholding_all.__len__()))

        ######################## ts.dump_reference_xsg_data  ###################

        for month in range(1, 13):
            if year < 2010:
                continue  # start from year 2010

            #logging.info(__file__+" "+"\trestricted shares lifted, year " + str(year) + " month " + str(month) + " start")
            sys.stdout.flush()
            dump_reference_xsg_data = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/restricted_lifted/rl_" + str(year) + "_" + str(month) + ".csv"

            try:
                exc_info = sys.exc_info()
                df_tmp = pd.DataFrame()

                #if (year > 2010) and ((not os.path.isfile(dump_reference_xsg_data)) or (year == 2018)):
                if (year > 2010) and ((not os.path.isfile(dump_reference_xsg_data))):
                    logging.info(__file__+" "+"\t\tgetting xsg_data, ts.xsg_data, year " + str(year) + " month " + str(month))  # 限售股解禁
                    df_tmp = ts.xsg_data(year, month)
                    df_tmp.to_csv(dump_reference_xsg_data, encoding='UTF-8', index=False)
                    logging.info(__file__ + ": " + "saved, " + dump_reference_xsg_data)

                    #df_tmp.to_pickle(dump_reference_xsg_data)
                elif os.path.isfile(dump_reference_xsg_data):
                    #logging.info(__file__+" "+"\tloading xsg_data")
                    df_tmp = pandas.read_csv(dump_reference_xsg_data, converters={'code': str})

            except:
                logging.info(__file__+" "+"\tcaught exception, xsg_data, year " + str(year) + " month " + str(month))
            finally:
                df_xsg_all = df_xsg_all.append(df_tmp, ignore_index=True)

                if exc_info == (None, None, None):
                    pass  # no exception
                else:
                    traceback.print_exception(*exc_info)
                del exc_info
                #logging.info('\txsg_data year ' + str(year) + " quarter " + str(quarter) + " end")
                #logging.info(__file__+" "+"\t---> df_xsg_all len " + str(df_xsg_all.__len__()))  # update df all in every loop
                df_xsg_all.to_csv(xsg_csv, encoding='UTF-8', index=False)
                logging.info(__file__ + ": " + "saved, " + xsg_csv + " . len " + str(df_xsg_all.__len__()))
    ''''######################## ts.sh_margins  Obsolated###################

    dump_reference_sh_margins = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sh_margin/sh_margin.csv"
    if (year > 2010) and ((not os.path.isfile(dump_reference_sh_margins)) or (year == 2018)):
        #logging.info(__file__+" "+"\ngetting sh_margins, ts.sh_margins, start "+str(start)+ " end "+str(end))  # 融资融券（沪市）
        sys.stdout.flush()

        try:
            exc_info = sys.exc_info()
            df_tmp = pd.DataFrame()
            df_sh_margins = ts.sh_margins(start=start, end=end)
            df_tmp.to_csv(dump_reference_sh_margins, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + dump_reference_sh_margins)

            #df_sh_margins.to_pickle(dump_reference_sh_margins)
        except:
            logging.info(__file__+" "+"\tcaught exception, sh_margins, year " + str(year) + " month " + str(month))
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info
    elif os.path.isfile(dump_reference_sh_margins):
        #logging.info(__file__+" "+"\tloading sh_margins")
        df_sh_margins = pandas.read_csv(dump_reference_sh_margins,converters={'code': str})
        df_sh_margins.to_csv(sh_margin_csv,  encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + sh_margin_csv)

    ######################## ts.sz_margins  Obsolated###################

    dump_reference_sz_margins = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sz_margin/sz_margin.csv"
    if (year > 2010) and ((not os.path.isfile(dump_reference_sz_margins)) or (year == 2018)):
        #logging.info(__file__+" "+"\ngetting sz_margins, ts.sz_margins, start "+str(start)+ " end "+str(end))  # 融资融券（深市）
        sys.stdout.flush()

        try:
            exc_info = sys.exc_info()

            df_sz_margins = ts.sz_margins(start=start, end=end)
            df_tmp.to_csv(dump_reference_sz_margins, encoding='UTF-8', index=False)
            logging.info(__file__ + ": " + "saved, " + dump_reference_sz_margins)

            #df_sz_margins.to_pickle(dump_reference_sz_margins)
        except:
            logging.info(__file__+" "+"\tcaught exception, sz_margins, year " + str(year) + " month " + str(month))
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info

    elif os.path.isfile(dump_reference_sz_margins):
        #logging.info(__file__+" "+"\tloading sz_margins")
        df_sz_margins = pandas.read_csv(dump_reference_sz_margins,converters={'code': str})
        df_sz_margins.to_csv(sz_margin_csv,  encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + sz_margin_csv)

   '''

    logging.info(__file__+" "+"df_profit_all len " + str(df_profit_all.__len__()))
    logging.info(__file__+" "+"df_forecast_all len " + str(df_forecast_all.__len__()))
    logging.info(__file__+" "+"df_fundholding_all len " + str(df_fundholding_all.__len__()))
    logging.info(__file__+" "+"df_xsg_all len " + str(df_xsg_all.__len__()))

    logging.info(__file__+" "+"df_new_stock len " + str(df_new_stock.__len__()))
    #logging.info(__file__+" "+"df_sh_margins len "+str(df_sh_margins.__len__()))
    #logging.info(__file__+" "+"df_sz_margins len "+str(df_sz_margins.__len__()))

    logging.info(__file__+" "+"End of the sub get_reference_data")
    #end of get_reference_data


def fetch_data_no_adj(debug=False, force_fetch=False):
    default_start_date = '1990-01-01'  # start fetch date when csv file not exist or empty.
    #df_profit = pd.DataFrame()
    #df_profit = pd.read_csv(profit_csv, converters={'code': str})

    #remove duplicate
    #df_profit.drop_duplicates(inplace=True)
    #df_profit.fillna(0, inplace=True)
    #df_profit = df_profit.reset_index().drop('index', axis=1)
    #logging.info(("df_profit len: " + str(df_profit.__len__())))
    #df = df_profit

    df_code_name_map = finlib.Finlib().get_A_stock_instrment()
    total_len = df_code_name_map.__len__()
    i = 0
    #api = ts_cs.api(retry_count=10)
    cons = ts.get_apis()
    #ts.set_token('af47c923316de1ed0385279ed5645c7d94c306e80cdd70dd43d4ddf5')
    #pro = ts.pro_api()

    #pro.query()

    for code in list(df_code_name_map['code']):
        i += 1

        code = str(code)
        name = df_code_name_map[df_code_name_map['code'] == code]['name'].values[0]
        code_m = finlib.Finlib().add_market_to_code_single(code)
        csv_f = "/home/ryan/DATA/DAY_No_Adj/" + code_m + ".csv"
        df_tmp = pd.DataFrame()  # base of the csv

        #if finlib.Finlib().is_cached(csv_f, day=0) and (not force_fetch):
        #    logging.info(__file__+" "+"ignore because csv_f was updated within 1 days, " + csv_f)
        #    continue

        if (not os.path.isfile(csv_f)) or (os.stat(csv_f).st_size <= 200):  # 200 bytes
            logging.info(__file__+" "+"file not exist or empty file, full fetch " + csv_f)
            sys.stdout.flush()
            start_date_req = default_start_date
        else:
            ############ delta update start
            default_date_d = datetime.datetime.strptime(default_start_date, '%Y-%m-%d')

            df_tmp = pd.read_csv(csv_f, converters={'code': str})

            ####### fix start
            #df_tmp = df_tmp[:-1]
            #df_tmp.to_csv(csv_f, encoding='UTF-8', index=False)
            #logging.info(__file__+" "+". saved, len " + str(df_tmp.__len__()))
            #continue
            ####### fix end

            last_row = df_tmp[-1:]
            last_date = str(last_row['date'].values[0])
            next_date = datetime.datetime.strptime(last_date, '%Y-%m-%d') + datetime.timedelta(1)
            a_week_before_date = datetime.datetime.strptime(todayS, '%Y-%m-%d') - datetime.timedelta(20)

            if next_date.strftime('%Y-%m-%d') > todayS:
                logging.info(__file__+" "+"file already updated, not fetching again. " + str(i) + " of " + str(total_len) + ". updated to " + last_date + " " + csv_f)
                #i_cnt += 1
                continue

            # last date in csv is 7 days ago, most likely the source is not update, so skip this csv.
            # logging.info(__file__+" "+"Next "+next_date.strftime('%Y-%m-%d'))
            # logging.info(__file__+" "+"a week before "+ a_week_before_date.strftime('%Y-%m-%d'))
            #if next_date.strftime('%Y-%m-%d') < a_week_before_date.strftime('%Y-%m-%d'):
                #logging.info(__file__+" "+"file too old to updated, not fetching. " + str(i) + " of " + str(total_len) + ". updated to " + last_date + " " + csv_f)
                #i_cnt += 1
                #continue

            #if next_date > default_date_d:  # csv already have data
            start_date_req = next_date.strftime('%Y%m%d')
            logging.info(__file__+" "+"append exist csv from " + start_date_req + ". ")
            #else:
            #    logging.info(__file__+" "+"will do a full update, since " + start_date_req + ". ")

            ############ delta update end

        if i % 500 == 0:  #renew connection every N times
            ts.close_apis(cons)
            cons = ts.get_apis()

        logging.info(__file__+" "+"fetch " + csv_f + " " + str(i) + " of " + str(df_code_name_map.__len__()))
        sys.stdout.flush()

        try:
            exc_info = sys.exc_info()

            #fetching delta
            df_hist_data = ts.bar(code=code, conn=cons,  adj=None, \
                                  start_date=start_date_req, end_date=todayS, retry_count=10)

            if df_hist_data is None:
                logging.info(__file__+" "+"ts.bar return None for " + code)
                continue
            else:
                logging.info(__file__+" "+" len fetched " + str(df_hist_data.__len__()) + " ")

        except:
            logging.info(__file__+" "+"\tcaught exception on, code " + str(code))
            continue
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info

        # , start_date='2016-04-20', end_date='2016-04-30')
        df_hist_data = df_hist_data.reindex(index=df_hist_data.index[::-1])  # revert

        df_hist_data = df_hist_data.reset_index()

        # rearrange columns
        cols = df_hist_data.columns.tolist()

        cols = ['code', 'datetime', 'open', 'high', 'low', 'close', 'vol', 'amount']  # adjust column order
        df_hist_data = df_hist_data[cols]
        df_hist_data.columns = ['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt']  # rename column
        # convert date column from datetime to string
        df_hist_data['date'] = df_hist_data['date'].dt.strftime('%Y-%m-%d')

        df_hist_data = df_tmp.append(df_hist_data, ignore_index=True)  #append delta to existing data.

        df_hist_data.to_csv(csv_f, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + ". saved, len " + str(df_hist_data.__len__()) + " " + csv_f)


'''
    logging.info(__file__+" "+"fetch No Adj Starts")

    api = ts_cs.api(retry_count=10)
    cons  = ts.get_apis()

    df_result = pd.DataFrame(columns=['code', 'name', 'year', 'report_date', 'divi', 'shares','price','divi_percent','shares_percent'])

    for i in range(df.__len__()):
        code = df.iloc[i, df.columns.get_loc('code')]  # 000685
        name = df.iloc[i, df.columns.get_loc('name')]  #
        year = df.iloc[i, df.columns.get_loc('year')]  #2003.0
        report_date = df.iloc[i, df.columns.get_loc('report_date')]  #'2014-02-09'
        divi = df.iloc[i, df.columns.get_loc('divi')]  #1.5
        shares = df.iloc[i, df.columns.get_loc('shares')]#0.0

        code_m = finlib.Finlib().add_market_to_code_single(code)
        csv_f = "/home/ryan/DATA/DAY_No_Adj/"+code_m+".csv"

        if not re.match("\d{4}", str(report_date)):
            continue
        elif int( re.match("\d{4}", str(report_date)).group(0)) < 1990: #pass year less than 2000
            continue

        last_trading_date = finlib.Finlib().get_last_trading_day(report_date)

        #if not last_trading_date == report_date:
        #    pass

        if divi == 0.0 and shares == 0.0:
            continue

        #if not os.path.isfile(csv_f):
        if True:
            logging.info(__file__+" "+"fetch "+csv_f +". " +str(i) +" of "+str(df.__len__()))
            sys.stdout.flush()

            try:
                exc_info = sys.exc_info()
                df_hist_data = ts.bar(code=code, conn=cons, adj=None,retry_count=10)

                if df_hist_data is None:
                    logging.info(__file__+" "+"ts.bar return None for "+code)
                    continue
            except:
                logging.info(__file__+" "+"\tcaught exception on, code " + str(code))
                continue
            finally:
                if exc_info == (None, None, None):
                    pass  # no exception
                else:
                    traceback.print_exception(*exc_info)
                del exc_info

            #, start_date='2016-04-20', end_date='2016-04-30')
            df_hist_data = df_hist_data.reindex(index=df_hist_data.index[::-1]) #revert

            df_hist_data = df_hist_data.reset_index()

            #rearrange columns
            cols = df_hist_data.columns.tolist()

            cols = ['code', 'datetime','open','high','low','close','vol','amount'] #adjust column order
            df_hist_data = df_hist_data[cols]
            df_hist_data.columns = ['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt']  #rename column


            df_hist_data.to_csv(csv_f,  encoding='UTF-8', index=False)
            logging.info(__file__+" "+". saved, len "+str(df_hist_data.__len__()))
'''


def analyze_one(debug=False):
    if (not force_run_global) and finlib.Finlib().is_cached(profit_analysis_csv, day=5):
        logging.info(__file__+" "+"skip file, it been updated in 5 day. " + profit_analysis_csv)
        return

    df_profit = pd.DataFrame()
    if debug:
        df_profit = pd.read_csv(profit_csv_debug, converters={'code': str})  #debug
    else:
        df_profit = pd.read_csv(profit_csv, converters={'code': str})

    fields_profit = [
        'code',  #股票代码
        'name',  #股票名称
        'year',  #分配年份
        'report_date',  #公布日期
        'divi',  #分红金额（每10股）
        'shares',  #转增和送股数（每10股）
    ]

    fields_forecast = [
        #code',代码
        #name',名称
        'type',  #业绩变动类型【预增、预亏等】
        'report_date',  #发布日期
        'pre_eps',  #上年同期每股收益
        'range',  #业绩变动范围
    ]

    fields_xsg = [
        #code：股票代码
        #name：股票名称
        'date',  #解禁日期
        'count',  #解禁数量（万股）
        'ratio',  #占总盘比率
    ]

    fields_fundholding = [
        #code：股票代码
        #name：股票名称
        'date',  #报告日期
        'nums',  #基金家数
        'nlast',  #与上期相比（增加或减少了）
        'count',  #基金持股数（万股）
        'clast',  #与上期相比
        'amount',  #基金持股市值
        'ratio',  #占流通盘比率
    ]

    fields_newstock = [
        #code：股票代码
        #name：股票名称
        'ipo_date',  #上网发行日期
        'issue_date',  #上市日期
        'amount',  #发行数量(万股)
        'markets',  #上网发行数量(万股)
        'price',  #发行价格(元)
        'pe',  #发行市盈率
        'limit',  #个人申购上限(万股)
        'funds',  #募集资金(亿元)
        'ballot',  #网上中签率(%)
    ]

    fields_margin = [
        'opDate',  #信用交易日期
        'rzye',  #本日融资余额(元)
        'rzmre',  # 本日融资买入额(元)
        'rqyl',  # 本日融券余量
        'rqylje',  # 本日融券余量金额(元)
        'rqmcl',  # 本日融券卖出量
        'rzrqjyzl',  #本日融资融券余额(元)
    ]

    logging.info(("df_profit len: " + str(df_profit.__len__())))

    #remove duplicate
    df_profit.drop_duplicates(inplace=True)
    df_profit.fillna(0, inplace=True)
    df_profit = df_profit.reset_index().drop('index', axis=1)
    logging.info(("df_profit len: " + str(df_profit.__len__())))
    df = df_profit

    logging.info(__file__+" "+"FengHong Analysis Starts")

    df_result = pd.DataFrame(columns=['code', 'name', 'year', 'report_date', 'divi', 'shares', 'price', 'divi_percent', 'shares_percent'])

    for i in range(df.__len__()):
        code = df.iloc[i, df.columns.get_loc('code')]  # 000685
        name = df.iloc[i, df.columns.get_loc('name')]  #
        year = df.iloc[i, df.columns.get_loc('year')]  #2003.0
        report_date = df.iloc[i, df.columns.get_loc('report_date')]  #'2014-02-09'
        divi = df.iloc[i, df.columns.get_loc('divi')]  #1.5
        shares = df.iloc[i, df.columns.get_loc('shares')]  #0.0

        code_m = finlib.Finlib().add_market_to_code_single(code)
        csv_f = "/home/ryan/DATA/DAY_No_Adj/" + code_m + ".csv"

        if not re.match("\d{4}", str(report_date)):
            continue
        #elif int( re.match("\d{4}", str(report_date)).group(0)) < 1990: #pass year less than 2000
        elif int(re.match("\d{4}", str(report_date)).group(0)) < 2014:  #pass year less than 2000
            continue

        report_dateSs = datetime.datetime.strptime(report_date, '%Y-%m-%d').strftime('%Y%m%d')
        last_trading_dateSs = finlib.Finlib().get_last_trading_day(report_dateSs)
        last_trading_date = datetime.datetime.strptime(last_trading_dateSs, '%Y%m%d').strftime('%Y-%m-%d')

        #if not last_trading_date == report_date:
        #    pass

        if divi == 0.0 and shares == 0.0:
            continue

        if not os.path.isfile(csv_f):
            logging.info(__file__+" "+"Cannot contine, not found file " + csv_f)
            continue
        else:
            df_csv = pd.read_csv(csv_f, converters={'code': str}, skiprows=1, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])

        #calculate start
        price = df_csv[df_csv['date'] == last_trading_date]['c']

        for i in range(-1, -10, -1):
            if price.__len__() == 0:  #in case the last_trading_date has no stock data
                slice_start_index = i
                slice_end_index = i + 1

                if slice_end_index == 0:
                    slice_end_index = None

                # then checking the lastest has data days.
                price = df_csv[df_csv['date'] <= last_trading_date][slice_start_index:slice_end_index]['c']

            else:
                break

        if price.__len__() == 0:
            logging.info(__file__+" "+"WARNING:CANNOT GET HISTORY PRICE,  SHOULD NOT BE HERE. " + code + " " + last_trading_date)
            continue

        price = price.values[0]

        if price == 0:
            logging.info(__file__+" "+"Not continue as price is 0. " + code + " " + last_trading_date)
            continue

        divi_percent = abs(100 * divi / (price * 10))  #divi is on 10 shares
        share_percent = 100 * shares / 10

        df_result = df_result.append(pd.DataFrame({
            'code': [code],
            'name': [name],
            'year': [year],
            'report_date': [report_date],
            'divi': [divi],
            'shares': [shares],
            'price': [price],
            'divi_percent': [divi_percent],
            'shares_percent': [share_percent],
        }))

        logging.info(__file__+" "+"code " + str(code) + " report date " + str(report_date) + ".  div percent " + str(divi_percent) + ", share percent " + str(share_percent))

        #ts.bar(code='000651', conn=cons, start_date='2016-04-20', end_date='2016-04-30')

    df_result.to_csv(profit_analysis_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved, " + profit_analysis_csv)
    return (df_result)


def analyze_two():
    # divi_p: percent of divi_percent_sum
    # share_p: percent of shares_sum
    # divi_score: divi_sum/fhcnt
    # share_score: share_sum/fhcnt

    if (not force_run_global) and finlib.Finlib().is_cached(fenghong_score_csv, day=5):
        logging.info(__file__+" "+"skip file, it been updated in 5 day. " + fenghong_score_csv)
        return

    df_result = pd.DataFrame(columns=['code', 'name', 'score', 'divi_m_score', 'share_m_score', 'fhcnt', 'fhcnt_p', 'divi_sum', 'divi_p', 'share_sum', 'share_p', 'divi_m', 'share_m'])
    df = pd.read_csv(profit_analysis_csv, converters={'code': str})

    codes = df['code'].unique()

    leng = codes.__len__()

    for i in range(leng):
        code = codes[i]
        logging.info('analyze_two, stage 1, ' + str(i) + " of " + str(leng) + "," + str(code))

        df_tmp = df[df['code'] == code]
        name = df_tmp.iloc[0]['name']
        fhcnt = df_tmp.__len__()
        divi_percent_sum = df_tmp['divi_percent'].sum()
        divi_m = df_tmp['divi_percent'].mean()
        share_sum = df_tmp['shares_percent'].sum()
        share_m = df_tmp['shares_percent'].mean()

        df_result = df_result.append(pd.DataFrame({
            'code': [code],
            'name': [name],
            'score': [0],
            'divi_m_score': [0],
            'share_m_score': [0],
            'fhcnt': [fhcnt],
            'fhcnt_p': [0],
            'divi_sum': [divi_percent_sum],
            'divi_p': [0],
            'divi_m': [divi_m],
            'share_sum': [share_sum],
            'share_p': [0],
            'share_m': [share_m],
        }))

    fhcnt_all = df_result['fhcnt']
    divi_all = df_result['divi_sum']
    share_all = df_result['share_sum']
    divi_m_all = df_result['divi_m']
    share_m_all = df_result['share_m']

    leng = df_result.__len__()
    for i in range(leng):
        code = df_result.iloc[i]['code']
        logging.info('analyze_two, stage 2, ' + str(i) + " of " + str(leng) + "," + str(code))

        fhcnt = df_result.iloc[i]['fhcnt']
        divi = df_result.iloc[i]['divi_sum']
        share = df_result.iloc[i]['share_sum']
        divi_m = df_result.iloc[i]['divi_m']
        share_m = df_result.iloc[i]['share_m']

        fhcnt_perc = stats.percentileofscore(fhcnt_all, fhcnt) / 100
        divi_perc = stats.percentileofscore(divi_all, divi) / 100
        share_perc = stats.percentileofscore(share_all, share) / 100
        divi_m_perc = stats.percentileofscore(divi_m_all, divi_m) / 100
        share_m_perc = stats.percentileofscore(share_m_all, share_m) / 100

        df_result.iloc[i, df_result.columns.get_loc('fhcnt_p')] = fhcnt_perc
        df_result.iloc[i, df_result.columns.get_loc('divi_p')] = divi_perc
        df_result.iloc[i, df_result.columns.get_loc('share_p')] = share_perc
        df_result.iloc[i, df_result.columns.get_loc('divi_m_score')] = divi_m_perc
        df_result.iloc[i, df_result.columns.get_loc('share_m_score')] = share_m_perc

        score = fhcnt_perc * 0.1 + divi_perc * 0.3 + share_perc * 0.3 + divi_m_perc * 0.3
        df_result.iloc[i, df_result.columns.get_loc('score')] = score

    cols = ['code', 'name', 'score', 'divi_m_score', 'share_m_score', 'fhcnt', 'fhcnt_p', 'divi_sum', 'divi_p', 'share_sum', 'share_p']  # adjust column order, sort column order.
    df_result = df_result[cols]

    df_result = df_result.sort_values('score', ascending=False)
    df_result.to_csv(fenghong_score_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + 'fenghong score result saved to ' + fenghong_score_csv)


def main():
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))
    parser = OptionParser()

    parser.add_option("-e", "--fetch_reference", action="store_true", dest="fetch_ref_f", default=False, help="fetch reference")

    parser.add_option("-f", "--fetch_no_adj_data", action="store_true", dest="fetch_data_f", default=False, help="fetch NoAdj data")

    parser.add_option("-o", "--force_fetch_data", action="store_true", dest="force_fetch_data_f", default=False, help="always fetch, even if file age < 3 days.")

    parser.add_option("-a", "--analyze_one", action="store_true", dest="analyze_1_f", default=False, help="analyze fenghong data stage 1")

    parser.add_option("-b", "--analyze_two", action="store_true", dest="analyze_2_f", default=False, help="analyze fenghong data stage 2")

    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="debug mode")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    (options, args) = parser.parse_args()
    fetch_ref_f = options.fetch_ref_f
    fetch_data_f = options.fetch_data_f
    force_fetch_data_f = options.force_fetch_data_f
    analyze_1_f = options.analyze_1_f
    analyze_2_f = options.analyze_2_f
    debug = options.debug
    force_run_f = options.force_run_f

    logging.info(__file__+" "+"fetch_ref_f: " + str(fetch_ref_f))
    logging.info(__file__+" "+"fetch_data_f: " + str(fetch_data_f))
    logging.info(__file__+" "+"force_fetch_data_f: " + str(force_fetch_data_f))
    logging.info(__file__+" "+"analyze_1_f: " + str(analyze_1_f))
    logging.info(__file__+" "+"analyze_2_f: " + str(analyze_2_f))
    logging.info(__file__+" "+"debug: " + str(debug))
    logging.info(__file__+" "+"force_run_f: " + str(force_run_f))

    global force_run_global
    force_run_global = False
    if force_run_f:
        force_run_global = True

    ########################
    # download reference data from tushare.
    #########################
    if fetch_ref_f:
        get_reference_data()

    if fetch_data_f:
        fetch_data_no_adj(force_fetch=force_fetch_data_f)

    ########################
    # analyze the fenghong of each time, rows are not merged by code
    #########################
    if analyze_1_f:
        df_result = analyze_one(debug=debug)

    ########################
    # Merge fenghong by code, give score
    #########################
    if analyze_2_f:
        analyze_two()

    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
