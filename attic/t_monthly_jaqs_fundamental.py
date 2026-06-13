# coding: utf-8
import finlib
import pandas as pd
import re
import itertools
import os
import finlib
import tushare as ts
from sqlalchemy import create_engine
import mysql.connector

from datetime import datetime, timedelta

from optparse import OptionParser

from jaqs.data.dataapi import DataApi
import pickle
import sys

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

global force_run_global

#日行情估值表	lb.secDailyIndicator	股票
#http://jaqs.readthedocs.io/zh_CN/latest/data_api.html?highlight=secDailyIndicator
#http://jaqs.readthedocs.io/zh_CN/release-0.6.0/base_data.html
'''
pe		市盈率
pb_new		市净率
pe_ttm		市盈率TTM
pcf_ocf		市现率(PCF,经营现金流)
pcf_ocfttm		市现率(PCF,经营现金流TTM)
pcf_ncf		市现率(PCF,现金净流量)
pcf_ncfttm		市现率(PCF,现金净流量TTM)
ps		市销率(PS)
ps_ttm		市销率(PS,TTM)
turnoverratio		换手率
freeturnover		换手率(自由流通股本)
total_share		当日总股本
float_share		当日流通股本
close		当日收盘价
price_div_dps		股价/每股派息
free_share		当日自由流通股本
profit_ttm		归属母公司净利润(TTM)
profit_lyr		归属母公司净利润(LYR)
net_assets		当日净资产
cash_flows_oper_act_ttm		经营活动产生的现金流量净额(TTM)
cash_flows_oper_act_lyr		经营活动产生的现金流量净额(LYR)
operrev_ttm		营业收入(TTM)
operrev_lyr		营业收入(LYR)
limit_status	Int	涨跌停状态





https://jaqs.readthedocs.io/zh_CN/release-0.6.0/base_data.html?highlight=total_mv

symbol string 证券代码
trade_date string 交易日期
total_mv double 当日总市值
float_mv double 当日流通市值
pe double 市盈率
pb_new double 市净率
pe_ttm double 市盈率TTM
pcf_ocf double 市现率(PCF,经营现金流)
pcf_ocfttm double 市现率(PCF,经营现金流TTM)
pcf_ncf double 市现率(PCF,现金净流量)
pcf_ncfttm double 市现率(PCF,现金净流量TTM)
ps double 市销率(PS)
ps_ttm double 市销率(PS,TTM)
turnoverratio double 换手率
freeturnover double 换手率(自由流通股本)
total_share double 当日总股本
float_share double 当日流通股本
close double 当日收盘价
price_div_dps double 股价/每股派息
free_share double 当日自由流通股本
profit_ttm double 归属母公司净利润(TTM)
profit_lyr double 归属母公司净利润(LYR)
net_assets double 当日净资产
cash_flows_oper_act_ttm double 经营活动产生的现金流量净额(TTM)
cash_flows_oper_act_lyr double 经营活动产生的现金流量净额(LYR)
operrev_ttm double 营业收入(TTM)
operrev_lyr double 营业收入(LYR)
limit_status Int 涨跌停状态

'''
'''
结果示例(前5条记录)：
http://jaqs.readthedocs.io/zh_CN/release-0.6.0/data_api.html?highlight=inst_type
code	code_type	value
1	inst_type	股票
10	inst_type	回购
100	inst_type	指数
101	inst_type	股指期货
102	inst_type	国债期货

df, msg = api.query(
                view="lb.instrumentInfo",
                fields="status,list_date, fullname_en, market",
                filter="inst_type=1&status=1&symbol=600000.SH",
                data_format='pandas')

'''

#csv_jaqs_fund = "/home/ryan/DATA/result/jaqs_fundamental.csv" #ryan comment on 18 08 24, remove the line if no broken.
j_quartly_report_csv = "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"


def fetch_secDailyIndicator(debug=False, force_fetch=False):
    default_start_date = '19900101'  #start fetch date when csv file not exist or empty.
    default_start_date = '20060101'  #start fetch date when csv file not exist or empty.

    #dump_file = "/home/ryan/DATA/tmp/"+start_date+"_"+end_date+".pickle"
    base_dir = "/home/ryan/DATA/DAY_JAQS"

    df_code_name_map = finlib.Finlib().get_A_stock_instrment(debug=debug)

    total_len = df_code_name_map.__len__()

    #df_code_name_map = df_code_name_map.head(3)

    #http://jaqs.readthedocs.io/zh_CN/latest/user_guide.html
    #user and password is on https://www.quantos.org
    api = finlib.Finlib().renew_jaqs_api()
    i_cnt = 1

    for code in list(df_code_name_map['code']):

        #code = '600519' #ryan debug

        code = str(code)
        if re.match('^6', code):
            market = "SH"
        elif re.match('^0', code):
            market = "SZ"
        elif re.match('^3', code):
            market = "SZ"
        else:
            logging.info(__file__+" "+"unknown market for code " + code)
            continue
            #exit(1)

        csv_f = base_dir + "/" + market + code + ".csv"
        #csv_f = base_dir+"/"+market+code+"_secDailyIndicator.csv"

        logging.info(csv_f + " ")
        sys.stdout.flush()

        if (not force_run_global) and finlib.Finlib().is_cached(csv_f, day=3) and (not force_fetch):
            logging.info(__file__+" "+"ignore because csv_f was updated within 3 days, " + csv_f)
            continue

        #todayS = datetime.today().strftime('%Y%m%d')
        todayS = finlib.Finlib().get_last_trading_day()

        todayS = datetime.strptime(todayS, '%Y-%m-%d').strftime('%Y%m%d')  #last trading day. eg. 20181202-->20181130

        df_tmp = pd.DataFrame()  #base of the csv

        if (not os.path.isfile(csv_f)) or (os.stat(csv_f).st_size <= 200):  #200 bytes
            logging.info(__file__+" "+"file not exist or empty file, full fetch " + csv_f)
            start_date_req = default_start_date
        else:
            ############ delta update start
            default_date_d = datetime.strptime(default_start_date, '%Y%m%d')

            df_tmp = pd.read_csv(csv_f, converters={'code': str, 'trade_date': str})
            last_row = df_tmp[-1:]
            last_date = str(last_row['trade_date'].values[0])
            next_date = datetime.strptime(last_date, '%Y%m%d') + timedelta(1)
            a_week_before_date = datetime.strptime(todayS, '%Y%m%d') - timedelta(20)

            if next_date.strftime('%Y%m%d') > todayS:
                logging.info(__file__+" "+"file " + csv_f + " already updated, not fetching again. " + str(i_cnt) + " of " + str(total_len) + ". updated to " + last_date)
                i_cnt += 1

                continue

            # last date in csv is 7 days ago, most likely the source is not update, so skip this csv.
            # logging.info(__file__+" "+"Next "+next_date.strftime('%Y-%m-%d'))
            # logging.info(__file__+" "+"a week before "+ a_week_before_date.strftime('%Y-%m-%d'))
            if next_date.strftime('%Y%m%d') < a_week_before_date.strftime('%Y%m%d'):
                logging.info(__file__+" "+"file too old to updated, not fetching. " + str(i_cnt) + " of " + str(total_len) + ". updated to " + last_date)
                #i_cnt += 1
                #continue

            #if next_date > default_date_d:  # csv already have data
            start_date_req = next_date.strftime('%Y%m%d')
            logging.info(__file__+" "+"append exist csv from " + start_date_req + ". ")
            #else:
            #    logging.info(__file__+" "+"will do a full update, since " + start_date_req + ". ")

            ############

        name = df_code_name_map[df_code_name_map['code'] == code]['name'].values[0]
        symbol = finlib.Finlib().append_market_to_code_single_dot(code)
        logging.info(code + " " + name + " " + symbol + " " + str(i_cnt) + " of " + str(total_len) + ". ")
        sys.stdout.flush()
        i_cnt += 1

        if i_cnt % 500 == 0:
            api = finlib.Finlib().renew_jaqs_api()

        #df, msg = api.query(
        #    view="lb.secDailyIndicator",
        #    fields='symbol,trade_date,total_mv, float_mv, pb,pe,pe_ttm,ps,ps_ttm, pcf_ocf,pcf_ocfttm,pcf_ncf,pcf_ncfttm,net_assets,close,price_div_dps',
        #    filter='symbol='+symbol+'&start_date='+start_date_req+'&end_date='+todayS
        #)

        ##debug start
        '''
        from jaqs.data import DataView
        from jaqs.data import RemoteDataService
        dv = DataView()
        ds = RemoteDataService()

        data_config = {
            "remote.data.address": "tcp://data.quantos.org:8910",
            "remote.data.username": "13651887669",
            "remote.data.password": "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1NTE1Mzg0NTQyNjgiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM2NTE4ODc2NjkifQ.MT6sg03zcLJprsx4NjsCbNqfIX0aYfycTyLZ4BsTh3c"
        }

        ds.init_from_config(data_config)

        secs = '600030.SH,000063.SZ,000001.SZ'
        props = {'start_date': 20160601, 'end_date': 20170601, 'symbol': secs,
                 'fields': 'open,close,high,low,volume,pb,net_assets,eps_basic',
                 'freq': 1}
        dv.init_from_config(props, data_api=ds)
        dv.prepare_data()
        snap1 = dv.get_snapshot(20170504, symbol='600030.SH,000063.SZ', fields='close,pb')
        '''
        #debug end

        df, msg = api.query(view="lb.secDailyIndicator", fields='symbol,trade_date,total_mv, float_mv, pb,pb_new, pe,pe_ttm,ps,ps_ttm,turnoverratio, freeturnover, total_share, float_share, free_share, pcf_ocf,pcf_ocfttm,pcf_ncf,profit_ttm,profit_lyr,pcf_ncfttm,net_assets,close,price_div_dps,cash_flows_oper_act_ttm,cash_flows_oper_act_lyr,operrev_ttm,operrev_lyr,limit_status', filter='symbol=' + symbol + '&start_date=' + start_date_req + '&end_date=' + todayS)

        if (msg == '0,'):
            logging.info(__file__+" "+" len fetched " + str(df.__len__()) + " ")

            code_df = pd.DataFrame([code] * df.__len__(), columns=['code'])
            name_df = pd.DataFrame([name] * df.__len__(), columns=['name'])
            df = code_df.join(df)
            df = name_df.join(df)
            #df_result = df_result.append(df)

            #cols = ['code', 'name', 'trade_date', 'close', 'symbol', 'ps', 'ps_ttm', 'pe','pe_ttm', 'pb', 'price_div_dps',
            #        'float_mv', 'net_assets', 'pcf_ncf', 'pcf_ncfttm', 'pcf_ocf', 'pcf_ocfttm']

            cols = ['code', 'name', 'trade_date', 'close', 'symbol', 'ps', 'ps_ttm', 'pe', 'pe_ttm', 'pb', 'price_div_dps', 'float_mv', 'net_assets', 'pcf_ncf', 'pcf_ncfttm', 'pcf_ocf', 'pcf_ocfttm', 'float_share', 'free_share', 'limit_status', 'total_mv', 'total_share']

            # the actual return not match the document.
            #cols = ['code', 'name', 'trade_date', 'close', 'symbol', 'ps', 'ps_ttm', 'pe_ttm', 'pe', 'pe_ttm', 'pb',
            #         'price_div_dps',
            #        'float_mv', 'net_assets', 'pcf_ncf', 'pcf_ncfttm', 'pcf_ocf', 'pcf_ocfttm', 'total_mv',
            #        'turnoverratio', 'freeturnover', 'total_share', 'float_share', 'free_share', 'profit_ttm',
            #        'profit_lyr', 'cash_flows_oper_act_ttm', 'cash_flows_oper_act_lyr', 'operrev_ttm', 'operrev_lyr',
            #        'limit_status']

            df = df[cols]
            df = df.sort_values('trade_date').drop_duplicates()

            df = df.reset_index().drop('index', axis=1)
            df_result = df_tmp.append(df, ignore_index=True)

            df_result.to_csv(csv_f, encoding='UTF-8', index=False)
            #logging.info(__file__+" "+"saved, len " + str(df_result.__len__()))
            logging.info(__file__ + ": " + "saved, " + csv_f + " , Len " + str(df_result.__len__()))

        else:
            logging.info(__file__+" "+"query secDailyIndicator failed, msg " + msg)
            continue

    return ()


''' DO NOT NEED THIS FUNCTION AT THIS TIME 20181201'''
'''
def fetch_cashFlow(debug=False, force_fetch=False):
    default_start_date = '20060101' #start fetch date when csv file not exist or empty.

    base_dir = "/home/ryan/DATA/DAY_JAQS"


    df_code_name_map = finlib.Finlib().get_A_stock_instrment(debug=debug)

    total_len = df_code_name_map.__len__()

    #df_code_name_map = df_code_name_map.head(3)

    #http://jaqs.readthedocs.io/zh_CN/latest/user_guide.html
    #user and password is on https://www.quantos.org
    api = finlib.Finlib().renew_jaqs_api()
    i_cnt = 1

    for code in list(df_code_name_map['code']):

        code = '600519' #ryan debug

        code = str(code)
        if re.match('^6',code):
            market="SH"
        elif  re.match('^0',code):
            market="SZ"
        elif re.match('^3', code):
            market = "SZ"
        else:
            logging.info(__file__+" "+"unknown market for code "+code)
            continue
            #exit(1)

        csv_f = base_dir+"/"+market+code+"_cashflow.csv"

        logging.info(csv_f+" ")
        sys.stdout.flush()

        if finlib.Finlib().is_cached(csv_f, day=3) and (not force_fetch):
            logging.info(__file__+" "+"ignore because csv_f was updated within 3 days, " + csv_f)
            continue

        todayS = datetime.today().strftime('%Y%m%d')
        #todayS = finlib.Finlib().get_last_trading_day(datetime.today().strftime('%Y-%m-%d'))

        df_tmp = pd.DataFrame()  #base of the csv

        logging.info(__file__+" "+"file not exist or empty file, full fetch "+csv_f)
        start_date_req = default_start_date


        name = df_code_name_map[df_code_name_map['code'] == code]['name'].values[0]
        symbol = finlib.Finlib().append_market_to_code_single_dot(code)
        logging.info(code + " " + name + " "+ symbol+ " "+str(i_cnt)+ " of "+str(total_len)+". ")
        sys.stdout.flush()
        i_cnt += 1

        if i_cnt % 500 == 0:
            api = finlib.Finlib().renew_jaqs_api()

        #df, msg = api.query(
        #    view="lb.secDailyIndicator",
        #    fields='symbol,trade_date,total_mv, float_mv, pb,pe,pe_ttm,ps,ps_ttm, pcf_ocf,pcf_ocfttm,pcf_ncf,pcf_ncfttm,net_assets,close,price_div_dps',
        #    filter='symbol='+symbol+'&start_date='+start_date_req+'&end_date='+todayS
        #)

        df, msg = api.query(view="lb.cashFlow",
                            fields="free_cash_flow",
                            filter='symbol=' + symbol + '&start_date=' + start_date_req + '&end_date=' + todayS)

        if (msg == '0,'):
            logging.info(__file__+" "+" len fetched "+str(df.__len__())+" ")

            code_df = pd.DataFrame([code]*df.__len__(),columns=['code'])
            name_df = pd.DataFrame([name]*df.__len__(),columns=['name'])
            df = code_df.join(df)
            df = name_df.join(df)
            #df_result = df_result.append(df)

            #df = df.sort_values('trade_date').drop_duplicates()

            df = df.reset_index().drop('index', axis=1)
            df_result = df_tmp.append(df, ignore_index=True)

            df_result.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info(__file__+" "+"saved, len " + str(df_result.__len__()))

        else:
            logging.info(__file__+" "+"query secDailyIndicator failed, msg "+msg)
            continue


    return()
'''


def calc_quartly_report():

    if (not force_run_global) and finlib.Finlib().is_cached(j_quartly_report_csv, day=5):
        logging.info(__file__+" "+"skip file, it been updated in 5 day. " + j_quartly_report_csv)
        return

    base_dir = "/home/ryan/DATA/DAY_JAQS"
    df_code_name_map = finlib.Finlib().get_A_stock_instrment()

    total_len = df_code_name_map.__len__()

    i_cnt = 1
    j_df_q_r = pd.DataFrame()  #jaqs quarterly report.    #j_df_q_r = j_df_q_r.assign(year_quarter=pd.Series([0] * df.__len__()) #insert a column.not as useful as col.insert
    this_len = last_len = 0

    for code in list(df_code_name_map['code']):
        logging.info(__file__+" "+"=== " + str(i_cnt) + " of " + str(total_len) + " ===")
        i_cnt += 1

        code = str(code)
        if re.match('^6', code):
            market = "SH"
        elif re.match('^0', code):
            market = "SZ"
        elif re.match('^3', code):
            market = "SZ"
        else:
            logging.info(__file__+" "+"unknown market for code " + code)
            continue
            #exit(1)

        csv_f = base_dir + "/" + market + code + ".csv"

        if not os.path.isfile(csv_f):
            logging.info(__file__+" "+"not found source file to calc quartly report " + csv_f)
            continue

        df = pd.read_csv(csv_f, converters={'code': str})

        cols = df.columns.tolist()
        cols.insert(0, 'year_quarter')  #insert a column to df list

        df_tmp = pd.DataFrame(index=list(range(0, 1)), columns=cols)  #python3
        #df_tmp = pd.DataFrame(index=range(0,1), columns=cols)#python2

        #for year in range(1990,2019):
        for year in range(2015, 2019):
            for quarter in range(1, 5):
                #year = 2015; quarter=4;
                if quarter == 1:
                    q_start = str(year) + "0101"
                    q_end = str(year) + "0331"
                if quarter == 2:
                    q_start = str(year) + "0401"
                    q_end = str(year) + "0631"
                if quarter == 3:
                    q_start = str(year) + "0701"
                    q_end = str(year) + "0931"
                if quarter == 4:
                    q_start = str(year) + "1001"
                    q_end = str(year) + "1231"

                query = "trade_date >= " + q_start + " and trade_date <= " + q_end + " and code=='" + code + "'"
                #logging.info(query)
                year_quarter = str(year) + "_" + str(quarter)  #format compliance with /DATA/result/fundamental.csv

                df_q = df.query(query)
                #logging.info(__file__+" "+"date len "+str(df_q.__len__()))

                if df_q.__len__() > 0:
                    df_tmp.iloc[0, df_tmp.columns.get_loc('close')] = df_q['close'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('ps')] = df_q['ps'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('ps_ttm')] = df_q['ps_ttm'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pe')] = df_q['pe'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pe_ttm')] = df_q['pe_ttm'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pb')] = df_q['pb'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('price_div_dps')] = df_q['price_div_dps'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('float_mv')] = df_q['float_mv'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('net_assets')] = df_q['net_assets'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pcf_ncf')] = df_q['pcf_ncf'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pcf_ncfttm')] = df_q['pcf_ncfttm'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pcf_ocf')] = df_q['pcf_ocf'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('pcf_ocfttm')] = df_q['pcf_ocfttm'].mean()

                    #not all the fields in doc actually returns
                    df_tmp.iloc[0, df_tmp.columns.get_loc('total_mv')] = df_q['total_mv'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('turnoverratio')] = df_q['turnoverratio'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('freeturnover')] = df_q['freeturnover'].mean()
                    df_tmp.iloc[0, df_tmp.columns.get_loc('total_share')] = df_q['total_share'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('float_share')] = df_q['float_share'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('free_share')] = df_q['free_share'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('profit_ttm')] = df_q['profit_ttm'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('profit_lyr')] = df_q['profit_lyr'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('cash_flows_oper_act_ttm')] = df_q[
                    #    'cash_flows_oper_act_ttm'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('cash_flows_oper_act_lyr')] = df_q[
                    #    'cash_flows_oper_act_lyr'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('operrev_ttm')] = df_q['operrev_ttm'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('operrev_lyr')] = df_q['operrev_lyr'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('limit_status')] = df_q['limit_status'].mean()
                    #df_tmp.iloc[0, df_tmp.columns.get_loc('pb_new')] = df_q['pb_new'].mean()

                    df_tmp.iloc[0, df_tmp.columns.get_loc('year_quarter')] = year_quarter
                    df_tmp.iloc[0, df_tmp.columns.get_loc('code')] = df_q.iloc[0]['code']
                    df_tmp.iloc[0, df_tmp.columns.get_loc('name')] = df_q.iloc[0]['name']
                    df_tmp.iloc[0, df_tmp.columns.get_loc('symbol')] = df_q.iloc[0]['symbol']

                    j_df_q_r = j_df_q_r.append(df_tmp)
                    this_len = j_df_q_r.__len__()

                    #logging.info(__file__+" "+"j_df_q_r length "+str(this_len))

                    if (this_len >= last_len):
                        last_len = this_len
                    else:
                        logging.info(__file__+" "+"should not be here")
                        exit(0)

                else:
                    #logging.info(__file__+" "+"no record, " + code + " " + q_start + " " + q_end)
                    pass

        #j_df_q_r.to_csv(j_quartly_report_csv, encoding='UTF-8', index=False)  # save first,  memory error on reset_index().drop.
        #logging.info(code +' saved, '+j_quartly_report_csv)

    cols = ['year_quarter', 'code', 'name', 'close', 'symbol', 'ps', 'ps_ttm', 'pe', 'pe_ttm', 'pb', 'price_div_dps', 'float_mv', 'net_assets', 'pcf_ncf', 'pcf_ncfttm', 'pcf_ocf', 'pcf_ocfttm']  #remove 'Unnamed: 0'
    j_df_q_r = j_df_q_r[cols]
    j_df_q_r.reset_index().drop('index', axis=1).to_csv(j_quartly_report_csv, encoding='UTF-8', index=False)

    logging.info(__file__ + ": " + "quartly report saved to " + j_quartly_report_csv)
    return ()


def main():
    ########################
    #
    #########################

    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))
    parser = OptionParser()

    parser.add_option("-f", "--fetch_data_all", action="store_true", dest="fetch_all", default=False, help="fetch all the quarterly fundatation history data. saved to /home/ryan/DATA/DAY_JAQS")

    parser.add_option("-o", "--force_fetch_data", action="store_true", dest="force_fetch_data", default=False, help="always fetch, even if file age < 3 days.")

    parser.add_option("-q", "--calc_quartly_report", action="store_true", dest="calc_quartly_report", default=False, help="calc quartly mean data from the daily. (close price BuFuQuan.) Report saved to " + j_quartly_report_csv)

    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="use debug, instrument_A.csv.debug, when -f")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all
    force_fetch_data_f = options.force_fetch_data
    calc_quartly_report_f = options.calc_quartly_report
    debug = options.debug
    force_run_f = options.force_run_f

    logging.info(__file__+" "+"fetch_all: " + str(fetch_all_f))
    logging.info(__file__+" "+"force_fetch: " + str(force_fetch_data_f))
    logging.info(__file__+" "+"calc_quartly_report: " + str(calc_quartly_report_f))
    logging.info(__file__+" "+"debug: " + str(debug))
    logging.info(__file__+" "+"force_run_f: " + str(force_run_f))

    global force_run_global
    force_run_global = False
    if force_run_f:
        force_run_global = True

    if fetch_all_f:
        fetch_secDailyIndicator(debug=debug, force_fetch=force_fetch_data_f)
        #fetch_cashFlow(debug=debug, force_fetch=force_fetch_data_f)
    elif calc_quartly_report_f:
        calc_quartly_report()

    logging.info(__file__+" "+"script completed")
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
