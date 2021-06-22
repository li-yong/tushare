# coding: utf-8
# encoding= utf-8

import pandas as pd
from pandas import DataFrame

import finlib

import finlib_indicator

import tushare as ts
import datetime

import talib 
import logging
import time
import os
import re
import constant
import math
import time
import numpy as np
import matplotlib.pyplot as plt

import akshare as ak
import glob
from scipy.stats import variation

########################### graham instrinsic value
def roe_pe():
    df_today = finlib.Finlib().get_today_stock_basic(date_exam_day='20210531')
    df_today = df_today[['code','industry','close','pe','pe_ttm','pb','ps','ps_ttm', 'dv_ratio', 'dv_ttm','list_date','on_market_days']]

    csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20201231.csv'
    csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20210331.csv'
    df_roe = finlib.Finlib().regular_read_csv_to_stdard_df(csv)
    df_roe = df_roe[['code','name','roe']]
    df_roe = pd.merge(df_roe, df_today, on='code')
    df_roe['roe_div_pe_ttm'] = df_roe['roe']/df_roe['pe_ttm']
    df_roe['roe_div_pe'] = df_roe['roe']/df_roe['pe']
    df_roe['roe_div_ps_ttm'] = df_roe['roe']/df_roe['ps_ttm']
    df_roe['roe_div_ps'] = df_roe['roe']/df_roe['ps']
    df_roe['roe_div_pb'] = df_roe['roe']/df_roe['pb']

    df_roe = finlib.Finlib().df_format_column(df=df_roe, precision='%.1e')

    df_roe_pe = df_roe[df_roe['pe']>0.1]
    df_roe_pe_ttm = df_roe[df_roe['pe_ttm']>0.1]
    df_roe_ps = df_roe[df_roe['ps']>0.1]
    df_roe_pb = df_roe[df_roe['pb']>0.1]

    print("==== ROE_DIV_PB ====")
    print(finlib.Finlib().pprint(df_roe_pb.sort_values(by='roe_div_pb', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pb','roe_div_pb']].head(10)))

    print("==== ROE_DIV_PS ====")
    print(finlib.Finlib().pprint(df_roe_ps.sort_values(by='roe_div_ps', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','ps','roe_div_ps']].head(10)))

    print("==== ROE_DIV_PE_TTM ====")
    print(finlib.Finlib().pprint(df_roe_pe_ttm.sort_values(by='roe_div_pe_ttm', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pe_ttm','roe_div_pe_ttm']].head(10)))

    print("==== ROE_DIV_PE ====")
    print(finlib.Finlib().pprint(df_roe_pe.sort_values(by='roe_div_pe', ascending=False).reset_index().drop('index', axis=1)[['code','name','industry','on_market_days','roe','pe','roe_div_pe']].head(100)))

    print("haha")


def coefficient_variation_price_amount():

    # a = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')
    # a = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')
    a = finlib.Finlib().get_stock_configuration(selected=False, stock_global='AG')

    df_stock_list = a['stock_list']
    df_csv_dir = a['csv_dir']
    df_out_dir = a['out_dir']
    df_rtn = pd.DataFrame()

    i=1
    for index, row in df_stock_list.iterrows():
        name, code = row['name'], row['code']
        print(str(i)+" of "+str(df_stock_list.__len__())+" , "+str(code)+" "+name)
        i+=1

        csv = df_csv_dir + "/"+ code+".csv"
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv).tail(100)
        cv_close = round(variation(df['close']), 2)
        cv_amount = round(variation(df['amount']), 2)
        df_rtn = df_rtn.append(pd.DataFrame().from_dict({'name':[name],'code':[code],'cv_close':[cv_close], 'cv_amount':[cv_amount]}))

    df_rtn = df_rtn.reset_index().drop('index',axis=1)
    print(finlib.Finlib().pprint(df_rtn.sort_values(by='cv_close',ascending=False)))


def remove_garbage_by_fcf():
    df = finlib.Finlib()._remove_garbage_fcf_profit_act_n_years(n_year=1)
    df = finlib.Finlib()._remove_garbage_must(df=df)
    print(finlib.Finlib().pprint(df[['code','name']].head(100)))
    return(df)


def price_amount_increase():

    startD = 20210607
    endD = 20210614

    # df_rtn=pd.DataFrame(columns=['group_name','price_change','amount_change'])
    df_rtn = pd.DataFrame()
    r_idx = 0

    #prepare amount df
    df_amount = finlib.Finlib().get_last_n_days_stocks_amount(ndays=5, dayS=str(startD), dayE=str(endD), daily_update=None, short_period= True, debug=False, force_run=False)
    df_close_start =  df_amount[df_amount['date']==int(startD)]
    df_close_end =  df_amount[df_amount['date']==int(endD)]
    df_amount =  pd.merge(df_close_start[['code','date','close','amount']],df_close_end[['code','date','close','amount']], on='code',how='inner', suffixes=('_dayS','_dayE'))
    df_amount['amount_increase'] = round((df_amount['amount_dayE'] - df_amount['amount_dayS']) *100.0 /  df_amount['amount_dayS'],2)

    #prepare close df
    df_basic = finlib.Finlib().get_last_n_days_daily_basic(ndays=10,dayS=None,dayE=None,daily_update=None,debug=False, force_run=False)
    df_close_start = df_basic[df_basic['trade_date']==int(startD)]
    df_close_end = df_basic[df_basic['trade_date']==int(endD)]
    df_close =  pd.merge(df_close_start[['ts_code','close','trade_date']],df_close_end[['ts_code','close','trade_date']], on='ts_code',how='inner', suffixes=('_dayS','_dayE'))
    df_close = finlib.Finlib().ts_code_to_code(df=df_close)
    df_close = finlib.Finlib().add_stock_name_to_df(df=df_close)

    if df_close.empty:
        logging.fatal("unexpected empty dataframe df_close, cannot contine")
        return

    #calculate HS300
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="HS300", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000300.csv"), df_close=df_close, df_amount=df_amount))
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="ZhongZhen100", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000903.csv"), df_close=df_close, df_amount=df_amount))
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="ZhongZhen500", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SH000905.csv"), df_close=df_close, df_amount=df_amount))
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="ShenZhenChenZhi", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399001.csv"), df_close=df_close, df_amount=df_amount))
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="ShenZhen100", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/SZ399330.csv"), df_close=df_close, df_amount=df_amount))
    df_rtn = df_rtn.append(_get_avg_chg_of_code_list(list_name="KeJiLongTou", df_code_column_only=pd.read_csv("/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/CSI931087.csv"), df_close=df_close, df_amount=df_amount))


    #calculate garbage stocks close/amount increase
    df_rtn_garb = pd.DataFrame()
    for csv in glob.glob("/home/ryan/DATA/result/garbage/latest_*.csv"):
        # logging.info("reading "+csv)
        df = pd.read_csv(csv)
        df_rtn_garb = df_rtn_garb.append(_get_avg_chg_of_code_list(list_name=csv.split(sep='/')[-1], df_code_column_only=df[['code']], df_close=df_close, df_amount=df_amount))

    logging.info("\n===== INDEX Increase ======")
    logging.info(finlib.Finlib().pprint(df_rtn.sort_values('price_change', ascending=False, inplace=False)))

    logging.info("\n===== Garbage Increase ======")
    logging.info(finlib.Finlib().pprint(df_rtn_garb.sort_values('price_change', ascending=False, inplace=False)))
    # exit(0)


def _get_avg_chg_of_code_list(list_name, df_code_column_only, df_close,df_amount):
    if df_close.empty:
        logging.error("Unexpected empty input df df_close.")
        return()

    df_2 = pd.merge(df_code_column_only[['code']].drop_duplicates(),
                    df_close[['code', 'name', 'close_dayS', 'trade_date_dayS', 'close_dayE', 'trade_date_dayE']],
                    on='code', how='inner')
    df_2 = pd.merge(df_2, df_amount[['code', 'amount_increase']], on='code', how='inner')
    df_2['close_delta'] = round((df_2['close_dayE'] - df_2['close_dayS']) * 100.0 / df_2['close_dayS'], 2)
    chg_mean_perc_close = round(df_2['close_delta'].mean(), 2)
    chg_mean_perc_amt = round(df_2['amount_increase'].mean(), 2)

    print(str(df_close.trade_date_dayS.iloc[0]) + "->" + str(df_close.trade_date_dayE.iloc[0]) + " len " + str(
        df_2.__len__()) + " " + list_name + ",  change average close " + str(
        chg_mean_perc_close) + "%,  change average amount " + str(chg_mean_perc_amt) + "%")

    return(pd.DataFrame.from_dict({'date_s':[df_close['trade_date_dayS'].iloc[0]],
                                   'date_e':[df_close['trade_date_dayE'].iloc[0]],

                                   'group_name':[list_name],
                                   'price_change':[chg_mean_perc_close],
                                   'amount_change':[chg_mean_perc_amt],
                                   'len': [df_2.__len__()],
                                   }))



def grep_garbage():
    #output saved to /home/ryan/DATA/result/garbage/*.csv
    df = finlib.Finlib().get_A_stock_instrment()
    df = finlib.Finlib().remove_garbage(df)

    finlib.Finlib()._remove_garbage_fcf_profit_act_n_years(n_year=1)

    #_remove_garbage_must
    finlib.Finlib()._remove_garbage_beneish_low_rate(df, m_score=-1)
    finlib.Finlib()._remove_garbage_change_named_stock(df, n_year=3)
    finlib.Finlib()._remove_garbage_high_pledge_ration(df, statistic_ratio_threshold=50, detail_ratio_sum_threshold=70)
    finlib.Finlib()._remove_garbage_low_roe_pe(df, market='AG', roe_pe_ratio_threshold=0.5)
    finlib.Finlib()._remove_garbage_by_fund_n_years(df, n_years=1)  #esp < bottom 0.1 etc
    finlib.Finlib()._remove_garbage_rpt_s1(df, code_field_name='code', code_format='C2D6')
    finlib.Finlib()._remove_garbage_by_profit_on_market_days_st(df)
    finlib.Finlib()._remove_garbage_none_standard_audit_statement(df, n_year=1)


########################### graham instrinsic value
def graham_intrinsic_value():
    df = finlib_indicator.Finlib_indicator().graham_intrinsic_value()
    logging.info("\n==== Graham instrinsic value ====")
    logging.info(finlib.Finlib().pprint(df.head(10)))
    return(df)


########################### Evaluate tr/pe ratio
def evaluate_tr_pe():
    df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
    df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())

    df_ag_hold = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list']
    tmp = finlib.Finlib().add_amount_mktcap(df=df_ag_hold)
    tmp = finlib.Finlib().add_tr_pe(df=tmp, df_daily=df_daily, df_ts_all=df_ts_all)
    tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')

    print(finlib.Finlib().pprint(tmp))

#########################################
def evaluate_grid(market='AG'):
    #markte in [AG_HOLD,AG, HK, US]
    logging.info("\n==== Evaluate_grid "+market+" ====")
    #
    # logging.info("========== Evaluate_grid 52 Week  ========")
    # logging.info("=============================")

    high_field='52 Week High'
    low_field='52 Week Low'

    # all_column = False
    all_column = True
    (df_52week,df_g_n4,df_g_n3_52week,df_g_n2,df_g_n1,df_g_p1_52week,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)

    cols = ['code','name', 'grid', 'eq_pos','roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)','atr_14','vola_month','volatility']
    # cols += ['cs_pos','grid_perc_resis_spt_dist',"l1","l2","l3","l4","l5","l6","l7" ,'description']
    cols += ['grid_support','grid_resistance','grid_perc_to_support','grid_perc_to_resistance']


    # print("==========  PositiveGrid 1 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_p1_52week[cols].head(2)))  #grid == 1

    # print("==========  NegtiveGrid 3 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_n3_52week[cols].head(2)))  #grid == -3


    # df_ag_hold = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list']
    # df_ag = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')['stock_list']

    df_mkt = finlib.Finlib().get_stock_configuration(selected=True, stock_global=market)['stock_list']

    # ddf_rtnf = finlib.Finlib().adjust_column(df=df, col_name_list=['code', 'tr_pe'])


    # df_hold_52week = pd.merge(df_52week, df_ag_hold, on=['code'], how='inner', suffixes=('', '_select'))[cols]
    df_52week = pd.merge(df_52week, df_mkt, on=['code'], how='inner', suffixes=('', '_select'))[cols]

    df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
    df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())


    tmp = finlib.Finlib().add_amount_mktcap(df=df_52week)
    tmp = finlib.Finlib().add_tr_pe(df=tmp, df_daily=df_daily, df_ts_all=df_ts_all)
    tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')
    logging.info("\n==========  52 Week "+market+" ========")
    logging.info(finlib.Finlib().pprint(tmp))

    print("roe_ttm", "pe_ttm", 'Operating Margin (TTM)', 'Gross Margin (TTM)')
    _ = df_g_p4[['roe_ttm', 'pe_ttm', 'Operating Margin (TTM)', 'Gross Margin (TTM)']].describe().iloc[1];  print('p4: ', end='');  print(_['roe_ttm'], _['pe_ttm'], _['Operating Margin (TTM)'], _['Gross Margin (TTM)'])
    _ = df_g_p3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p1_52week[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])

    _ = df_g_n1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n3_52week[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])



    ###################

    logging.info("\n==========  3 Month "+market+" ========")
    logging.info("=============================")

    high_field='3-Month High'
    low_field ='3-Month Low'

    # all_column = False
    all_column = True
    (df_3month,df_g_n4,df_g_n3_3month,df_g_n2,df_g_n1,df_g_p1_3month,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)


    # print("========== 3 month  PositiveGrid 1 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_p1_3month[cols].head(2)))  #grid == 1

    # print("========== 3 month NegtiveGrid 3 "+ str(high_field)+"  "+str(low_field)+" ========")
    # print(finlib.Finlib().pprint(df_g_n3_3month[cols].head(2)))  #grid == -3


    df = pd.merge(df_52week, df_3month, on='code', how='inner', suffixes=("", "_3M"))
    df_llrh = df[(df.grid==-3) & (df.grid_3M==1)][cols]  #long low, recent high
    logging.info("\n==== 52Week Low, 3Month High ====")
    print(finlib.Finlib().pprint(df_llrh.head(30)))

    df_lhrl = df[(df.grid==1) & (df.grid_3M==-3)][cols]  #long high, recent low
    logging.info("\n==== 52Week high, 3Month Lowe ====")
    print(finlib.Finlib().pprint(df_lhrl.head(30)))

    #
    print("roe_ttm", "pe_ttm",'Operating Margin (TTM)','Gross Margin (TTM)')
    _ = df_g_p4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p1_3month[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])

    _ = df_g_n1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n3_3month[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])


    # df_g_p4.sort_values('grid_perc_resis_spt_dist', ascending=False, inplace=False).head(5)

    exit(0)

#########################################
def tv_filter():
    df_us_1D = finlib.Finlib().load_tv_fund(market='US', period='1D')
    df_us_1W = finlib.Finlib().load_tv_fund(market='US', period='1W')

    df=df_us_1D
    df = df[df['Total Debt (MRQ)']<=df['Total Revenue (FY)']]
    df = df[df['Basic EPS (TTM)']>0]
    df = df[df['roe_ttm']>5]
    df = df[df['pe_ttm']>5]
    df = df[df['ps']>5]

    cols=['code','name','Enterprise Value (MRQ)','EBITDA (TTM)','Enterprise Value/EBITDA (TTM)',
          'roe_ttm','ps','pe_ttm','Operating Margin (TTM)','vola_month',
          'Total Revenue (FY)', 'Total Debt (MRQ)',
          '52 Week High','close','52 Week Low',
          ]
    df[cols].describe()


    df_us_1D[df_us_1D['code'].isin(['BILI','FUTU','MSFT','DIS','TWLO','DELL'])][cols]


    print(1)



def ag_industry_selected():
    path = "/home/ryan/DATA/ag_selected_industry"
    allFiles = glob.glob(path + "/" + "*.BK*")
    df_rtn = pd.DataFrame()

    for f in allFiles:
        df = pd.read_csv(f, names=['code'], converters={'code': str}, header=None)
        df['industry'] = f.split("/")[-1].split("_")[0]
        df_rtn = df_rtn.append(df)
        logging.info("loading "+f+" appended len "+str(df.__len__())+" , all line "+str(df_rtn.__len__()))

    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn = finlib.Finlib().add_market_to_code(df=df_rtn)
    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)

    df_rtn = finlib.Finlib().remove_garbage(df=df_rtn)

    out_csv = path + "/" + "ag_industry_selected.csv"
    df_rtn.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("saved to "+out_csv)
    print(finlib.Finlib().pprint(df_rtn))
    return (df_rtn)


#### MAIN #####
# grep_garbage() #save to files /home/ryan/DATA/result/garbage/*.csv

df_all = finlib.Finlib().get_A_stock_instrment()
df_all = finlib.Finlib().add_industry_to_df(df=df_all)


print(1)


df_industry = ag_industry_selected()
exit()

df_fcf = remove_garbage_by_fcf() # update garbage, consider fcf and must
df_increase = price_amount_increase()
df_intrinsic_value = graham_intrinsic_value()
evaluate_grid(market='AG')

#########################################
df = finlib.Finlib().load_price_n_days(ndays=20)

df = finlib.Finlib().ts_code_to_code(df=df)
df_rtn = pd.DataFrame()

for code in df['code'].unique():
    df_1 = df[df['code']==code].reset_index().drop('index', axis=1)

    if df_1.__len__() < 5:
        continue

    df_1['vol_avg_5'] = df_1['vol'].rolling(window=5).mean()

    # df_1['price_change_in_day'] = round((df_1['close'] - df_1['open'])*100/df_1['open'], 2)
    df_1['price_change_across_day'] = round((df_1['close'] - df_1['close'].shift(1))*100/df_1['close'].shift(1), 2)
    df_1['vol_change_across_day'] = round((df_1['vol'] - df_1['vol'].shift(1))*100/df_1['vol'].shift(1), 2)
    df_1['vol_change_vs_avg_5'] = round((df_1['vol'] - df_1['vol_avg_5'].shift(1))*100/df_1['vol_avg_5'].shift(1), 2)
    df_1['vp_chg_ratio']= round(df_1['vol_change_across_day'] / df_1['price_change_across_day'],2)
    df_1['pct_chg_next_day']= df_1['pct_chg'].shift(-1)
    df_1['corr']=round(df_1['pct_chg_next_day'].corr(df_1['vp_chg_ratio']),2)
    df_1 = df_1.drop(columns=['open','high','low','pre_close','change'])

    df_rtn = df_rtn.append(df_1.tail(1))
    # print(str(code)+" pv_chg_ration / pct_chg_next_day cor " + str(df_1['corr'].iloc[-1]))

print(1)
df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
df_rtn.sort_values(by='corr', ascending=False)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vp_chg_ratio', ascending=False).head(10)))
print(finlib.Finlib().pprint(df_rtn.head(10)))

#largest vol increase
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_across_day', ascending=False).head(10)))

#smallest vol increase (compare to vol_day-1)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_across_day', ascending=True).head(10)))

#smallest vol increase (compare to vol_ma5)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_vs_avg_5', ascending=True).head(10)))
#smallest vol increase (compare to vol_ma5)
print(finlib.Finlib().pprint(df_rtn.sort_values(by='vol_change_vs_avg_5', ascending=False).head(10)))


df_pin_bar_uppershadow = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_LONG_UPPER_SHADOW)
df_pin_bar_leg = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_LONG_LOWER_SHADOW)
df_pin_bar_guangtou = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_GUANG_TOU)
df_pin_bar_guangjiao = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.BAR_GUANG_TOU)

df_hammer= pd.merge(df_pin_bar_leg, df_pin_bar_guangtou, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
df_hammer_rev= pd.merge(df_pin_bar_uppershadow, df_pin_bar_guangjiao, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)

#########################################


csv = '/home/ryan/DATA/pickle/daily_update_source/ag_daily_20210319.csv'
df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)

df = df.sort_values(by='volume')
df = finlib.Finlib().add_stock_name_to_df(df=df)

#########################################
df_tv = finlib.Finlib().load_tv_fund(market='AG', period="d")
df_tv = finlib.Finlib().add_stock_name_to_df(df_tv)
df_tv = finlib.Finlib().add_ts_code_to_column(df=df_tv)

_df = df_tv[['code','name','close','52 Week High']]
_df['ratio']=df_tv['close']/df_tv['52 Week High']
df_low_p = _df[_df['ratio']<0.5]
df = finlib.Finlib().remove_garbage(df=df_low_p)
print(finlib.Finlib().pprint(df))
exit(0)


# df_pv_db_buy_filter = finlib.Finlib().regular_read_csv_to_stdard_df('/home/ryan/DATA/result/today/talib_and_pv_db_buy_filtered_AG.csv')
df_hs300_candi = finlib.Finlib().regular_read_csv_to_stdard_df('/home/ryan/DATA/result/hs300_candidate_list.csv')
df_pv_db_buy_filter = finlib.Finlib()._remove_garbage_must(df=df_hs300_candi)
exit(0)


df_pv_db_buy_filter.drop_duplicates(inplace=True)

df_pv_db_buy_filter.sort_values('2mea', ascending=False, inplace=True)
# already sorted by Increase_2D in t_daily_pattern_Hit_Price_Volume.py
df_pv_db_buy_filter = df_pv_db_buy_filter.loc[df_pv_db_buy_filter['close_p'] != '0.0']
len_df_pv_db_buy_filter_0 = str(df_pv_db_buy_filter.__len__())
df_pv_db_buy_filter = finlib.Finlib().remove_garbage(df_pv_db_buy_filter, code_field_name='code', code_format='C2D6')
logging.info(__file__ + " " + "\t df_pv_db_buy_filter length " + str(df_pv_db_buy_filter.__len__()))



df = finlib.Finlib().load_tv_fund(market='US')

#ROE > 0

#PE rank < 0.85 perc

net_profit

_remove_garbage_high_pledge_ration

low_roe_pe

#Depeat fuzhai < zichan





# a = finlib.Finlib().is_on_market(ts_code="600519.SH", date="20161231", basic_df=None)
# df = finlib.Finlib().load_all_ts_pro()
df = finlib.Finlib().get_A_stock_instrment()
df_1 = finlib.Finlib().remove_garbage(df=df)
print(finlib.Finlib().pprint(df_1))

df_us = finlib.Finlib().get_roe_div_pe(market='US')
df_ag = finlib.Finlib().get_roe_div_pe(market='AG')

df = df_ag
df_gar_1 = df[df['pe_ttm']<1]
df_gar_2 = df[df['roe']<3]
df_gar_3 = df[df['roe_pe']<0.5]

# df = finlib.Finlib().load_tv_fund(market='us',period='d')





pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b")


#获取申万一级行业列表
df1 = pro.index_classify(level='L1', src='SW')

#获取申万二级行业列表
df2 = pro.index_classify(level='L2', src='SW')

#获取申万三级级行业列表
df3 = pro.index_classify(level='L3', src='SW')


df = pro.hk_daily(ts_code='00001.HK', start_date='20190101', end_date='20190904')
################
df = ak.stock_hk_daily(symbol="00700",adjust="qfq")
exit(0)



exit(0)


# finlib_indicator.Finlib_indicator().print_support_price_by_price_volume(code='SZ000568', market='US_INDEX')
# finlib_indicator.Finlib_indicator().print_support_price_by_price_volume(code='SZ000568', market='AG')


#from futu import *
#
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.SZCZ_INDEX_BUY_CANDIDATE)
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA55_NEAR_MA21)
# print(finlib.Finlib().pprint(df1[['code','date','reason']]))
#
# df1 = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA21_NEAR_MA55_N_DAYS)
# print(finlib.Finlib().pprint(df1[['code','date','reason']]))
#
# df2 = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.PV2_VOLUME_RATIO_BOTTOM_10P)
# pass
# #####################
#
# #Year 2020
# # finlib.Finlib().get_last_n_days_stocks_amount(dayS='20191101', dayE='20201031', debug=True)
# # exit(0)
# # finlib.Finlib().get_last_n_days_stocks_amount(dayS='20200501', dayE='20201031')
#
# #Year 2021
# finlib.Finlib().get_last_n_days_stocks_amount(dayS='20200501', dayE='20210430', daily_update=True,
#                                               force_run=False, #ignore file existance, calculate everytime.
#                                               debug=True,  #only check head 3 rows
#                                               )  # HS300
# exit(0)
# finlib.Finlib().get_last_n_days_stocks_amount(dayS='20201101', dayE='20210430')  # SHEN_ZHEN
#
#
#
# exit(0)
#
#
#
#
# this_year = datetime.datetime.today().year
#
# this_year = 2020
# this_month = 2
#
#
# last_year = this_year - 1
# this_month = datetime.datetime.today().month
# ndays = 365
#
# if this_month <= 6:
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '0501', dayE=str(this_year) + '0430') # HS300
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '1101', dayE=str(this_year) + '0430') # SHEN_ZHEN
#
#     df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays,period_end=None, debug=True,force_run=True) #output  /home/ryan/DATA/result/average_daily_amount_sorted.csv
#
#     a = finlib_indicator.Finlib_indicator().get_indicator_critirial(query=constant.MA5_UP_KOUDI_DISTANCE_GT_5)
#
#     exit(0)
#
# if this_month >6:
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(last_year) + '1101', dayE=str(this_year) + '1031') # HS300
#     finlib.Finlib().get_last_n_days_stocks_amount(dayS=str(this_year) + '0501', dayE=str(this_year) + '1031') # SHEN_ZHEN
#
#
# finlib.Finlib().get_last_n_days_stocks_amount(ndays=365)
#
#




points = [[1, 5], [2, 3], [4, 1], [8, 5]]
x = [1,2,4,8]
y = [5,3,1,5]
coefficients = np.polyfit(x=x, y=y, deg=6)
poly = np.poly1d(coefficients)
new_x = np.linspace(x[0], x[-1])

new_y = poly(new_x)
plt.plot(x, y, "o", new_x, new_y)
plt.show()
plt.xlim([x[0]-1, x[-1] + 1 ])
plt.savefig("line.jpg")



# df = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.DOUBLE_BOTTOM_AG_SELECTED, selected=True)
# print(finlib.Finlib().pprint(df))
#
#
# df = finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.DOUBLE_BOTTOM_AG_SELECTED, selected=False)
# print(finlib.Finlib().pprint(df))


################ Filter price under 30 weeks MA,
################ Screen price growth steadly, e.g increase 2 point every days.
################  Looks duplicate with VCP.py at some level,
################  Consider if VCP.py output can be reused.
out_f = "/home/ryan/DATA/result/price_quality.csv"
stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')
stock_list = finlib.Finlib().add_ts_code_to_column(df=stock_list, code_col='code')


df = finlib.Finlib()._remove_garbage_high_pledge_ration(df=stock_list)


pro= ts.pro_api()
df = pro.pledge_detail(ts_code='000935.SZ')
print(finlib.Finlib().pprint(df))

csv_dir = "/home/ryan/DATA/DAY_Global/AG"
i = 0

for index, row in stock_list.iterrows():
    i += 1
    print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
    name, code = row['name'], row['code']

    csv_f = csv_dir + "/" + code + ".csv"


    if not os.path.exists(csv_f):
        print("csv_f not exist, " + csv_f)
        continue

    print("reading " + csv_f)
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    close_sma_2 = df['close'].rolling(window=2).mean()
    close_ema_2 = df['close'].ewm(span=2, min_periods=0, adjust=False, ignore_na=False).mean()

    finlib_indicator.Finlib_indicator().add_rsi()
    finlib_indicator.Finlib_indicator().add_ma_ema()


    pass



exit(0)

#END OF

#
# df1 = finlib.Finlib().evaluate_by_ps_pe_pb()
# print(df1.columns.values)
#
# df2 = finlib.Finlib().get_A_stock_instrment()
# df2 = df2[df2['code']=='600158']
# df3 = finlib.Finlib().remove_garbage(df2)
#

# (no_profit two years | PB


#on market < 2 years: PS

#on  2 < market  < 10 years: PE


#finlib.Finlib().get_today_stock_basic(date_exam_day='20200828')


n_days = 300
dir='/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source'
csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"


def fetch_top_list_inst():
    pro = ts.pro_api()

    df_top_list = pd.DataFrame()
    df_top_inst = pd.DataFrame()


    for i in range(n_days):
        dateS = (datetime.datetime.today() - datetime.timedelta(days=i)).strftime("%Y%m%d")

        if finlib.Finlib().is_a_trading_day_ag(dateS):
            # 龙虎榜每日明细
            # output columns:
            # trade_date	str	Y	交易日期
            # ts_code	str	Y	TS代码
            # name	str	Y	名称
            # close	float	Y	收盘价
            # pct_change	float	Y	涨跌幅
            # turnover_rate	float	Y	换手率
            # amount	float	Y	总成交额
            # l_sell	float	Y	龙虎榜卖出额
            # l_buy	float	Y	龙虎榜买入额
            # l_amount	float	Y	龙虎榜成交额
            # net_amount	float	Y	龙虎榜净买入额
            # net_rate	float	Y	龙虎榜净买额占比
            # amount_rate	float	Y	龙虎榜成交额占比
            # float_values	float	Y	当日流通市值
            # reason	str	Y	上榜理由

            df1 = pro.query('top_list', trade_date=dateS)
            time.sleep(1) #60 ci /sec
            print("df_1 len "+str(df1.__len__()))
            df_top_list = df1.append(df_top_list)

            # 龙虎榜机构明细
            # trade_date	str	Y	交易日期
            # ts_code	str	Y	TS代码
            # exalter	str	Y	营业部名称
            # buy	float	Y	买入额（万）
            # buy_rate	float	Y	买入占总成交比例
            # sell	float	Y	卖出额（万）
            # sell_rate	float	Y	卖出占总成交比例
            # net_buy	float	Y	净成交额（万）
            df2 = pro.query('top_inst', trade_date=dateS)
            print("df_2 len "+str(df2.__len__()))
            df_top_inst = df2.append(df_top_inst)


    df_top_list.to_csv(csv_f_top_list, encoding='UTF-8', index=False)
    logging.info("top_list saved to "+csv_f_top_list+" , len "+ str(df_top_list.__len__()) )


    df_top_inst.to_csv(csv_f_top_inst, encoding='UTF-8', index=False)
    logging.info("top_inst saved to "+csv_f_top_inst+" , len "+str(df_top_inst.__len__()) )

    return()



# buy the stock in top_list at the next day(T+1) opening, sell at two days(T+2) closing.
# evaluated ATR, Profit
# pft_2, pft_3, pft_5
def merge_top_list_inst():
    df = pd.DataFrame(columns=["trade_date", "code", "e1", "e2", "e3", "e4", "e5"])

    csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
    csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"

    df_top_list = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_list)
    df_top_inst = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_inst)

    n_days_before = (datetime.datetime.today() - datetime.timedelta(days=5)).strftime("%Y%m%d")

    #ryan debug
    #n_days_before = "20200806"

    df_top_list = df_top_list[df_top_list['trade_date'] <= n_days_before].reset_index().drop('index', axis=1)
    df_top_inst = df_top_inst[df_top_inst['trade_date'] <= n_days_before].reset_index().drop('index', axis=1)

    df_gar = df_top_list[(df_top_list['reason'] =='退市整理的证券') | (df_top_list['reason'] =='退市整理期') ]
    df_top_list = finlib.Finlib()._df_sub_by_code(df=df_top_list, df_sub=df_gar)
    df_top_inst = finlib.Finlib()._df_sub_by_code(df=df_top_inst, df_sub=df_gar)

    for i in range(df_top_list.__len__()):
        code_m = df_top_list.iloc[i]['code']
        on_date = df_top_list.iloc[i]['trade_date']

        d1 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")
        d2 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=2)).strftime("%Y%m%d")
        d3 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=3)).strftime("%Y%m%d")
        d4 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=4)).strftime("%Y%m%d")
        d5 = (datetime.datetime.strptime(on_date, "%Y%m%d") + datetime.timedelta(days=5)).strftime("%Y%m%d")

        p0 = finlib.Finlib().get_price(code_m=code_m, date=on_date)
        p1 = finlib.Finlib().get_price(code_m=code_m, date=d1)
        p2 = finlib.Finlib().get_price(code_m=code_m, date=d2)
        p3 = finlib.Finlib().get_price(code_m=code_m, date=d3)
        p4 = finlib.Finlib().get_price(code_m=code_m, date=d4)
        p5 = finlib.Finlib().get_price(code_m=code_m, date=d5)

        e1 = round(100* (p1 - p0)/p0, 0)
        e2 = round(100* (p2 - p0)/p0, 0)
        e3 = round(100* (p3 - p0)/p0, 0)
        e4 = round(100* (p4 - p0)/p0, 0)
        e5 = round(100* (p5 - p0)/p0, 0)

        df = df.append({
            "trade_date": on_date,
            "code": code_m,
            "e1": e1,
            "e2": e2,
            "e3": e3,
            "e4": e4,
            "e5": e5,
        }, ignore_index=True)

        print(1)
    df_top_list = pd.merge(left=df_top_list, right=df, on=['trade_date', "code"], how="inner") #adding e1-e5
    df_top_inst = pd.merge(left=df_top_inst, right=df_top_list, on=['trade_date', "code"], how="inner")


    # df_top_list[(df_top_list['reason'] =='无价格涨跌幅限制的证券') ].mean()
    # df_top_list[(df_top_list['reason'] =='异常期间价格涨幅偏离值累计达到15.39%') ].mean()

    df_top_list.to_csv(csv_f_top_list)
    logging.info("top_list with earn saved to "+csv_f_top_list+" , len "+ str(df_top_list.__len__()) )

    df_top_inst.to_csv(csv_f_top_inst)
    logging.info("top_inst with earn saved to "+csv_f_top_inst+" , len "+ str(df_top_inst.__len__()) )

    #
    # for r in df_top_list['reason'].drop_duplicates():
    #     print(r)
    #     print(df_top_list[(df_top_list['reason'] == r)]['e1'].mean())
    # #
    #0                                       日跌幅偏离值达到7%的前五只证券
    # 2                                       日涨幅偏离值达到7%的前五只证券
    # 6                                        日换手率达到20%的前五只证券
    # 9                               连续三个交易日内，涨幅偏离值累计达到20%的证券
    # 16                                       日振幅值达到15%的前五只证券
    # 28                      连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券
    # 29                                                 退市整理期
    # 67                              连续三个交易日内，跌幅偏离值累计达到20%的证券
    # 89                                           无价格涨跌幅限制的证券
    # 90                                               退市整理的证券
    # 91                非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券
    # 93                         有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前三只证券
    # 95                         有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券
    # 100                            有价格涨跌幅限制的日价格振幅达到15%的前三只证券
    # 102                             有价格涨跌幅限制的日换手率达到20%的前三只证券
    # 114                             有价格涨跌幅限制的日换手率达到30%的前五只证券
    # 130             连续三个交易日内，跌幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券
    # 131             连续三个交易日内，涨幅偏离值累计达到12%的ST证券、*ST证券和未完成股改证券
    # 257    连续三个交易日内，日均换手率与前五个交易日的日均换手率的比值达到30倍，且换手率累计达20%的证券
    # 262                ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券
    # 304                              当日无价格涨跌幅限制的A股，出现异常波动停牌的
    # 337                                异常期间价格涨幅偏离值累计达到15.39%
    # 433               非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券
    # 455                          有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券


    return()



# PCA analysis, Line Regressino
def pca_analysis():

    csv_f_top_list = dir + "/top_list_" + str(n_days) + "_days.csv"
    csv_f_top_inst = dir + "/top_inst_" + str(n_days) + "_days.csv"

    df_top_list = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_list)
    df_top_inst = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f_top_inst) #17905*26

    #df_top_inst['exalter'].unique().__len__() : 2676
    #top 100 frequence exalter
    top_freq_exalter_list = df_top_inst['exalter'].value_counts(sort=True).head(100).index.to_list()
    df_top_inst = df_top_inst[df_top_inst['exalter'].isin(top_freq_exalter_list)] #8272*26


    df_exalter = pd.get_dummies(data=df_top_inst['exalter'])    #8272*100
    df_top_inst = df_top_inst.join(df_exalter) #8272*126
    df_top_inst = df_top_inst.fillna(0)

    features = df_top_inst.columns.drop(['trade_date','code', 'name', 'close','exalter','e2','e3','e4','e5', 'reason']).to_list()

### LR

    df_win = df_top_inst[(df_top_inst['e1'] > 3)]
    df_win = df_win[features]
    df_lose = df_top_inst[(df_top_inst['e1'] < 3)]
    df_lose = df_lose[features]

    from sklearn.linear_model import LogisticRegression
    # all parameters not specified are set to their defaults
    # default solver is incredibly slow which is why it was changed to 'lbfgs'
    logisticRegr = LogisticRegression(solver='lbfgs')

    #split trainset
    import numpy as np
    from sklearn.model_selection import train_test_split
    df_win_train = train_test_split(df_win)

    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    # Fit on training set only.
    scaler.fit(df_win_train)


    logisticRegr.fit(train_img, train_lbl)



    ### PCA
    from sklearn.preprocessing import StandardScaler
    df = df_top_inst

    # Separating out the features
    x = df.loc[:, features].values
    # Separating out the target
    y = df.loc[:, ['e1']].values
    # Standardizing the features
    x = StandardScaler().fit_transform(x)
    print(1)


    from sklearn.decomposition import PCA
    pca = PCA(n_components=100)
    principalComponents = pca.fit_transform(x)
    print(pca.explained_variance_ratio_)
    principalDf = pd.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2'])

### MAIN ####
if __name__ == '__main__':
    fetch_top_list_inst() #step1
    merge_top_list_inst() #step2
    pca_analysis() #step3
