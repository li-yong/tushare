# coding: utf-8
# encoding= utf-8



import pandas as pd
from pandas import DataFrame

import finlib
from scipy import stats
import finlib_indicator

import tushare as ts
import datetime
import sys
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
import copy

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
    df_rtn = finlib.Finlib().add_stock_increase(df=df_rtn)

    out_csv = path + "/" + "ag_industry_selected.csv"
    df_rtn.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("saved to "+out_csv)
    print(finlib.Finlib().pprint(df_rtn))
    return (df_rtn)


def bayes_start(csv_o):
    if finlib.Finlib().is_cached(csv_o):
        logging.info("Load from "+csv_o)
        return(pd.read_csv(csv_o))

    # df = finlib.Finlib().get_A_stock_instrment()
    # df = finlib.Finlib().remove_garbage(df)
    df = finlib.Finlib().get_stock_configuration(selected=False, stock_global='AG',remove_garbage=False)
    # df = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')
    df_rtn = pd.DataFrame()

    stock_list = df['stock_list']
    csv_dir = df['csv_dir']
    out_dir = df['out_dir']

    for index, row in stock_list.iterrows():

        code = row['code']
        name = row['name']

        # print(code+", "+name)

        _df = _bayes_a_stock(code=code, name=name, csv_f=csv_dir+"/"+code+".csv")
        df_rtn = df_rtn.append(_df)

    df_rtn.to_csv(csv_o, encoding='UTF-8', index=False)
    logging.info("bayes result saved to "+csv_o+" ,len "+str(df_rtn.__len__()))


def _bayes_a_stock(code,name,csv_f):

    df_rtn = pd.DataFrame()
    df = finlib.Finlib().regular_read_csv_to_stdard_df(csv_f,exit_if_not_exist=False)

    if type(df)==type("FILE_NOT_EXIST"):
        return(df_rtn)

    if df.__len__() < 90:
        return(df_rtn)

    df = df.tail(1000)

    # code = "SH000001"
    # code = "SH603288"
    # name = 'SH_INDEX'
    # name = '海天味业'

    short=4
    middle=10
    long=27

    df = finlib_indicator.Finlib_indicator().add_ma_ema(df=df,short=short, middle=middle, long=long)
    df['close_a5'] = df['close'].shift(-5)
    df['close_a2'] = df['close'].shift(-2)
    df['close_b1'] = df['close'].shift(1)
    df['open_b1'] = df['open'].shift(1)

    # df_up = df[df['close_a2'] > 1.01*df['close']]
    df_up = df[df['close_a5'] > 1.02*df['close']]


    df['sma_short_b1'] = df['sma_short_'+str(short)].shift(1)
    df['sma_middle_b1'] = df['sma_middle_'+str(middle)].shift(1)
    df['sma_long_b1'] = df['sma_long_'+str(long)].shift(1)


    #jincha
    df_jincha = df[(df['sma_short_b1'] < df['sma_long_b1']) & (df['sma_short_'+str(short)] > df['sma_long_'+str(long)])]

    #yunxian
    df_yunxian = df[(df['open'] < df['close']) & (df['open_b1'] > df['close_b1']) & (df['open_b1'] > df['close']) & (df['close_b1'] < df['open'])  ]

    _df = _print_bayes_possibility(code=code,name=name, condition='jincha', df_all=df, df_up=df_up,df_con=df_jincha )
    df_rtn = df_rtn.append(_df)

    _df = _print_bayes_possibility(code=code,name=name, condition='yunxian', df_all=df, df_up=df_up,df_con=df_yunxian )
    df_rtn = df_rtn.append(_df)


    return(df_rtn)



def _print_bayes_possibility(code, name, condition, df_all, df_up, df_con):
    #P(condition | up) : N(up & condition) / N(up)

    df_rtn = pd.DataFrame()

    df_con_up =  pd.merge(df_con,df_up,on=['date'],how='inner',suffixes=('','_x'))

    if df_up.__len__() == 0:
        return (df_rtn)

    if df_all.__len__() == 0:
        return (df_rtn)

    p_con_up = df_con_up.__len__()/df_up.__len__()
    p_con = df_con.__len__() / df_all.__len__()
    p_up = df_up.__len__() / df_all.__len__()

    #P(up|condition)
    if p_con == 0:
        return(df_rtn)

    P_bayes = round((p_con_up*p_up)/p_con,2)  # it is actually equal df_con_up.__len__()/df_con.__len__()

    
    logging.info(str(code)+" "+str(name)+", Bayes Possibility, P(up|"+condition+"): "+str(P_bayes)
                 +" con_up: "+str(df_con_up.__len__() )
                 +" con: "+str(df_con.__len__() )
                 +" up: "+str(df_up.__len__() )
                 +" all: "+str(df_all.__len__() )
                 )

    df_rtn = pd.DataFrame.from_dict({
        'code':[code],
        'name':[name],
        'condition':[condition],
        'P_bayes':[P_bayes],
        'con':[df_con.__len__()],
        'up':[df_up.__len__()],
        'all':[df_all.__len__()],
    })

    return(df_rtn)



def check_stop_loss_based_on_ma_across():
    file = "/home/ryan/DATA/result/price_let_mashort_equal_malong.csv"
    df = pd.read_csv(file)

    df_target = df[ (df['delta_perc'] < 10) & (df['delta_perc'] > -10)]

    df_target_no_gar = finlib.Finlib().remove_garbage(df=df_target)

    df_target_no_gar = df_target_no_gar.sort_values(by=['delta_perc'], ascending=[False]).reset_index().drop('index', axis=1)

    df_buy = df_target_no_gar[df_target_no_gar['action']=='buy'].sort_values(by=['delta_perc'],ascending=True).reset_index().drop('index', axis=1)
    df_sell = df_target_no_gar[df_target_no_gar['action']=='sell'].sort_values(by=['delta_perc'],ascending=False).reset_index().drop('index', axis=1)

    logging.info("\nBuy Target")
    logging.info(finlib.Finlib().pprint(df=df_buy))

    logging.info("\nSell Target")
    logging.info(finlib.Finlib().pprint(df=df_sell))


    print(1)


def plot_pivots(X, pivots):
    plt.xlim(0, len(X))
    plt.ylim(X.min()*0.99, X.max()*1.01)
    plt.plot(np.arange(len(X)), X, 'k:', alpha=0.5)
    plt.plot(np.arange(len(X))[pivots != 0], X[pivots != 0], 'k-')
    plt.scatter(np.arange(len(X))[pivots == 1], X[pivots == 1], color='g')
    plt.scatter(np.arange(len(X))[pivots == -1], X[pivots == -1], color='r')

def perf_review(df):
    print(1)



def stock_vs_index_perf_perc_chg():
    output_csv = "/home/ryan/DATA/result/df_profit_report_stock_vs_index_perf_pct_chg.csv"
    if finlib.Finlib().is_cached(output_csv, day=1):
        logging.info("using the result file "+output_csv)
        df_rtn = pd.read_csv(output_csv)
        return(df_rtn)


    csv_index = '/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index)

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)
    df = finlib.Finlib().add_stock_name_to_df(df=df)
    # df = finlib.Finlib().remove_garbage(df=df)

    codes = df['code'].unique()
    codes.sort()
    df_profit_report = pd.DataFrame()

    for c in codes:
        df_sub = df[df['code']==c].reset_index().drop('index', axis=1)
        df_sub = pd.merge(left=df_sub, right=df_index, how='inner', on='date', suffixes=('',"_idx"))

        df_sub['pct_chg_vs_idx']=df_sub['pct_chg']-df_sub['pct_chg_idx']

        df_test = df_sub[['code','name','date','pct_chg_vs_idx']]
        df_test = df_test.rename(columns={'pct_chg_vs_idx':'close'})
        df_test = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_test, short=4, middle=27, long=60)
        (df_test,df_si_cha, df_jin_cha) = finlib_indicator.Finlib_indicator().slow_fast_across(df=df_test,fast_col_name='close_4_sma', slow_col_name='close_27_sma')

        df_jin_cha = pd.merge(df_jin_cha[['date']], df_sub, how="inner")
        df_si_cha = pd.merge(df_si_cha[['date']], df_sub, how="inner")

        if df_jin_cha.__len__()>0 and df_si_cha.__len__()>0:
            df_profit_details = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)
            df_profit_details = finlib.Finlib().add_stock_name_to_df(df=df_profit_details)
            # logging.info(finlib.Finlib().pprint(df_profit_details))
            df_profit_report  = df_profit_report.append(df_profit_details)

        continue

    logging.info("check_amount_x_individual, profit_overall describe ")
    logging.info(df_profit_report[["profit_overall"]].describe())
    df_profit_report.to_csv(output_csv)
    logging.info("result saved to "+output_csv)

def stock_vs_index_perf_amount():
    output_csv="/home/ryan/DATA/result/df_profit_report_stock_vs_index_perf_amount.csv"
    df_rtn = pd.DataFrame()

    if finlib.Finlib().is_cached(output_csv, day=1):
        logging.info("loading the result file "+output_csv)
        df_rtn = pd.read_csv(output_csv)
        return(df_rtn)

    csv_index = '/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(csv_index)

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)
    df = finlib.Finlib().add_stock_name_to_df(df=df)
    # df = finlib.Finlib().remove_garbage(df=df)

    # df_today = df[df['date'] == df.date.max()]
    df_today = df[df['date'] == finlib.Finlib().get_last_trading_day()]

    q = df_today['amount'].quantile(0.7) #197355
    df_today = df_today[df_today['amount']>q].sort_values('amount', ascending=False, inplace=False)

    for c in df_today['code'].unique():
        df_sub = df[df['code']==c].reset_index().drop('index', axis=1)
        df_sub = pd.merge(left=df_sub, right=df_index, how='inner', on='date', suffixes=('',"_idx"))

        df_sub['amount_vs_idx']=df_sub['amount']*10000/df_sub['amount_idx']

        df_test = df_sub[['code','name','date','amount_vs_idx']]
        df_test = df_test.rename(columns={'amount_vs_idx':'close'})
        df_test = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_test, short=4, middle=27, long=60)
        (df_test,df_si_cha, df_jin_cha) = finlib_indicator.Finlib_indicator().slow_fast_across(df=df_test,fast_col_name='close_4_sma', slow_col_name='close_27_sma')

        df_jin_cha = pd.merge(df_jin_cha[['date']], df_sub, how="inner")
        df_si_cha = pd.merge(df_si_cha[['date']], df_sub, how="inner")

        bool_current_si_cha = False
        bool_current_jin_cha = False
        last_jin_cha_date = ''
        last_si_cha_date = ''

        if df_jin_cha.empty and (not df_si_cha.empty):
            bool_current_si_cha = True
            last_si_cha_date = df_si_cha.iloc[-1]['date']
        elif df_si_cha.empty and (not df_jin_cha.empty):
            bool_current_jin_cha = True
            last_jin_cha_date = df_jin_cha.iloc[-1]['date']
        elif (not df_si_cha.empty) and (not df_jin_cha.empty):
            if df_si_cha.iloc[-1]['date'] > df_jin_cha.iloc[-1]['date']:
                bool_current_si_cha = True
                last_si_cha_date = df_si_cha.iloc[-1]['date']
            else:
                bool_current_jin_cha = True
                last_jin_cha_date=df_jin_cha.iloc[-1]['date']

        if df_jin_cha.__len__()>0 and df_si_cha.__len__()>0:
            df_profit_details = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha, df_si_cha=df_si_cha)

            if df_profit_details.empty:
                continue

            df_profit_details = finlib.Finlib().add_stock_name_to_df(df=df_profit_details)
            logging.info(finlib.Finlib().pprint(df_profit_details.tail(1)))

            df_tmp = df_profit_details.tail(1)[['code',  'name', 'profit_overall']]
            df_tmp['transaction_count']=df_profit_details.__len__()
            df_tmp['current_si_cha']=bool_current_si_cha
            df_tmp['last_jin_cha_date']=last_jin_cha_date

            df_tmp['current_jin_cha']=bool_current_jin_cha
            df_tmp['last_si_cha_date']=last_si_cha_date

            df_rtn  = df_rtn.append(df_tmp)

        continue

    logging.info("check_amount_x_individual, profit_overall describe ")
    logging.info(df_rtn[["profit_overall"]].describe())

    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(output_csv)
    logging.info("result saved to "+output_csv)

    df_buy = df_rtn[df_rtn['profit_overall']>0]
    # df_buy = df_buy[df_buy['last_jin_cha_date']=='20211125']
    df_buy = df_buy[df_buy['last_jin_cha_date']==finlib.Finlib().get_last_trading_day()]
    df_buy = df_buy.sort_values(by='profit_overall', ascending=False)
    print(finlib.Finlib().pprint(df_buy.head(100)))

    logging.info("hhh")


def fudu_daily_check():
    df_base = fudu_get_base_data(base_windows=5,slide_window=3)
    df_today=fudu_get_today_data(base_windows=5,slide_window=3)
    df_short = pd.merge(left=df_base, right=df_today[['code','drop_from_max','inc_from_min']], on='code', how='inner')
    df_short = finlib.Finlib().add_turnover_rate_f_sum_mean(df=df_short,ndays=3, dayE=finlib.Finlib().get_last_trading_day())

    df_base = fudu_get_base_data(base_windows=100,slide_window=90)
    df_today = fudu_get_today_data(base_windows=100,slide_window=90)
    df_long = pd.merge(left=df_base, right=df_today[['code','drop_from_max','inc_from_min']], on='code', how='inner')

    #######
    topN=5
    logging.info("\nRecent max turnover_sum stocks, top "+str(topN))
    logging.info(finlib.Finlib().pprint(df_short.sort_values(by='tv_sum').tail(topN)[['code','name','inc_cnt','inc_mean','tv_sum']]))
    # logging.info(finlib.Finlib().pprint(df_short.sort_values(by='tv_mean').tail(topN)[['code','name','inc_cnt','inc_mean','tv_mean']]))
    ###########
    logging.info("\nRecent max increase_mean stocks, top "+str(topN))
    logging.info(finlib.Finlib().pprint(df_short.sort_values(by='inc_mean').tail(topN)[['code','name','inc_cnt','inc_mean','dec_cnt']]))
    ###########
    # df_short_inc_max = df_short.sort_values(by='inc_mean').tail
    df=pd.merge(left=df_short, right=df_long, on=['code'], how='inner', suffixes=['_s', '_l'])

    #dfr: df short_inc_max, and long dec min
    dfr = df
    dfr = dfr[(dfr['inc_std_s'].rank(pct=True) > 0.2) & (dfr['inc_std_s'].rank(pct=True) <= 0.8)]
    dfr = dfr[(dfr['inc_mean_s'].rank(pct=True) > 0.9) & (dfr['inc_mean_s'].rank(pct=True) <= 0.95)].sort_values(by='inc_mean_s')

    dfr = dfr[(dfr['dec_std_l'].rank(pct=True) > 0) & (dfr['dec_std_l'].rank(pct=True) <= 0.9)]
    dfr = dfr[(dfr['dec_mean_l'].rank(pct=True) > 0) & (dfr['dec_mean_l'].rank(pct=True) <= 0.3)].sort_values(by='dec_mean_l',ascending=False)

    #############
    logging.info("\nmax average decrease in long period, top " + str(topN))
    logging.info("Hint: place Buy order -30% of current price. Reasonable to prepare -30% lost in 90 days.")
    logging.info(dfr.sort_values(by="dec_mean_l", ascending=False).tail(topN)[['code', 'name_l', 'dec_mean_l']])

    #############
    logging.info("\nmax average increase in long period, top " + str(topN))
    logging.info("Hint:Reasonable to prepare 30% increase in 90 days.")
    logging.info(dfr.sort_values(by="inc_mean_l").tail(topN)[['code', 'name_l', 'inc_mean_l']])



    #############
    logging.info("\nstocks have max drop in long period, top " + str(topN))
    _df = dfr.sort_values(by="drop_from_max_l", ascending=False).tail(topN) #not much meaning on drop_from_max_s
    logging.info(_df[['code', 'name_l', 'window_size_l', 'drop_from_max_l']])

    logging.info("\nstocks have max drop in long period, max inc_from_min_s in short period, top " + str(topN))
    _df = _df.sort_values(by="inc_from_min_s").tail(topN)
    logging.info(_df[['code', 'name_l', 'window_size_l','window_size_s','drop_from_max_l','inc_from_min_s']])

    logging.info("\nmax drop in long period, max inc_mean in short period, top " + str(topN))
    _df = _df.sort_values(by="inc_mean_s").tail(topN)
    logging.info(_df[['code', 'name_l','window_size_l','window_size_s', 'inc_mean_s', 'drop_from_max_l','inc_from_min_s']])


    #############
    logging.info("\nmax increase in short period, top " + str(topN))
    logging.info("Hint: burst stocks")
    df_short['inc_std_rank'] = df_short['inc_std'].rank(pct=True)
    # df_short= df_short[df_short['inc_std'] < 2]
    logging.info(df_short.sort_values(by='inc_from_min').tail(topN)[['code','name','inc_from_min','inc_std','inc_std_rank','tv_mean', 'tv_sum']])

    df_short_inc_max = df_short.sort_values(by='inc_from_min').tail(100)
    df_long_dec_max = df_long.sort_values(by='drop_from_max').head(100)

    df_long_drop_short_inc=pd.merge(left=df_long_dec_max, right=df_short_inc_max,on=['code'],how='inner',suffixes=['_l','_s'])
    df_long_drop_short_inc = df_long_drop_short_inc.sort_values(by='inc_from_min_s')
    df_long_drop_short_inc = df_long_drop_short_inc.sort_values(by='drop_from_max_l',ascending=False)


    df_long_drop_short_inc=pd.merge(left=df_long.sort_values(by='drop_from_max').head(100),
                                    right=df_short.sort_values(by='inc_from_min').tail(100),
                                    on=['code'],how='inner')
    logging.info("end of fudu_daily_check\n")
    return()



def fudu_get_base_data(base_windows=5, slide_window=3):
    csv_out = "/home/ryan/DATA/result/zhangfu_tongji_base_"+str(base_windows)+"_slide_"+str(slide_window)+".csv"

    if finlib.Finlib().is_cached(file_path=csv_out, day=1):
        df_rtn=pd.read_csv(csv_out)
        return(df_rtn)

    df_rtn = pd.DataFrame()

    df = finlib.Finlib().load_all_ag_qfq_data(days=base_windows)
    csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_"+str(base_windows)+"_days.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)

    df =df[['code','date','high','low', 'close']]

    logging.info("fudu base data")

    for c in df.code.unique():
        # c = 'SZ300656'
        dfs=df[df['code']==c].tail(base_windows).reset_index().drop('index', axis=1)

        if dfs.__len__()<base_windows:
            continue

        h=dfs['high']
        l=dfs['low']

        dfs['win_max'] = h.rolling(slide_window).max()
        dfs['win_min'] = l.rolling(slide_window).min()

        dfs['win_max_idx'] = h.rolling(slide_window).apply(np.argmax)[slide_window-1:].astype(int)+np.arange(len(h)-slide_window+1)
        dfs['win_min_idx'] = l.rolling(slide_window).apply(np.argmin)[slide_window-1:].astype(int)+np.arange(len(l)-slide_window+1)

        dfs_inc = dfs[dfs['win_max_idx']>dfs['win_min_idx']]
        dfs_dec = dfs[dfs['win_max_idx']<dfs['win_min_idx']]

        dfs_inc = dfs_inc.drop_duplicates(subset=['win_max_idx','win_min_idx'], keep='last')
        dfs_dec = dfs_dec.drop_duplicates(subset=['win_max_idx','win_min_idx'], keep='last')

        dfs_inc['inc'] = round((dfs_inc['win_max'] - dfs_inc['win_min'])*100/dfs_inc['win_min'],0)
        # logging.info(c+" increase perc of window "+str(window_size)+" days,  description\n"+ str(dfs_inc['inc'].describe()))

        dfs_dec['dec'] = round((dfs_dec['win_min'] - dfs_dec['win_max'])*100/dfs_dec['win_max'],0)
        # logging.info(c+" decrease perc of window "+str(window_size)+" days,  description\n"+ str(dfs_dec['dec'].describe()))

        inc_desc=dfs_inc['inc'].describe()
        dec_desc=dfs_dec['dec'].describe()

        _df = pd.DataFrame({
            'code':[c],
            'window_size':[slide_window],
            'inc_cnt':[inc_desc['count']],
            'inc_mean':[round(inc_desc['mean'],2)],
            'inc_std':[round(inc_desc['std'],2)],
            'inc_max':[round(inc_desc['max'],2)],
            'inc_min':[round(inc_desc['min'],2)],

            'dec_cnt': [dec_desc['count']],
            'dec_mean': [round(dec_desc['mean'], 2)],
            'dec_std': [round(dec_desc['std'], 2)],
            'dec_max': [round(dec_desc['max'], 2)],
            'dec_min': [round(dec_desc['min'], 2)],
        })

        # logging.info(finlib.Finlib().pprint(_df))
        df_rtn = df_rtn.append(_df)

    df_rtn['cnt_ratio'] = round((df_rtn['inc_cnt']+1) / (df_rtn['dec_cnt']+1), 2)
    df_rtn['inc_ratio'] = round((df_rtn['inc_cnt']+1) * df_rtn['inc_mean'] / (df_rtn['dec_cnt']+1) / abs(df_rtn['dec_min']), 2)


    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("saved to " + csv_out + " len " + str(df_rtn.__len__()))
    return(df_rtn)

def fudu_get_today_data(base_windows=5,slide_window=3):
    csv_out = "/home/ryan/DATA/result/zhangfu_tongji_daily_check_base_"+str(base_windows)+"_slide_"+str(slide_window)+".csv"

    if finlib.Finlib().is_cached(file_path=csv_out, day=1):
        df_rtn=pd.read_csv(csv_out)
        return(df_rtn)

    df_rtn = pd.DataFrame()

    df = finlib.Finlib().load_all_ag_qfq_data(days=base_windows)
    csv_f = "/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_"+str(base_windows)+"_days.csv"
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_f)
    df = finlib.Finlib()._remove_garbage_on_market_days(df=df,on_market_days=90)

    df =df[['code','date','high','low', 'close']]
    i=0
    all=df.code.unique().__len__()
    logging.info("fudu generating today data")
    for c in df.code.unique():
        # logging.info(str(i) + " of " + str(all))
        i+=1

        dfs=df[df['code']==c]

        dfs=dfs.tail(slide_window).reset_index().drop('index', axis=1)

        max=dfs['high'].max()
        min=dfs['low'].min()
        now=dfs['close'].iloc[-1]
        drop_from_max = round(-100 * (max-now)/max, 0)
        inc_from_min = round(100 * (now-min)/min, 0)

        _df = pd.DataFrame({
            'code':[c],
            'window_size':[slide_window],
            'drop_from_max':[drop_from_max],
            'inc_from_min':[inc_from_min],
        })
        df_rtn = df_rtn.append(_df)

        # logging.info(_df)

    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv_out, encoding='UTF-8', index=False)
    logging.info("saved to " + csv_out + " len " + str(df_rtn.__len__()))
    return(df_rtn)


def fetch_holder():
    df_holder = pd.DataFrame()

    out_csv = "/home/ryan/DATA/result/holdernumber.csv"

    if finlib.Finlib().is_cached(out_csv, day=5):
        logging.info("loading from " + out_csv)
        df_holder = pd.read_csv(out_csv)
    #
    pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b", timeout=3)

    if df_holder.__len__() < 100:
        df_holder = pro.stk_holdernumber(end_date='20211231')
        df_holder = finlib.Finlib().ts_code_to_code(df_holder)

    i = 0
    stock_list = finlib.Finlib().get_A_stock_instrment()
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)
    stock_list = finlib.Finlib().add_ts_code_to_column(stock_list)
    for index, row in stock_list.iterrows():
        i += 1
        name, code, ts_code = row['name'], row['code'], row['ts_code']
        logging.info(str(i) + " of " + str(stock_list.__len__()) + " " + str(code) + " " + name)
        if code in df_holder['code'].to_list():
            # logging.info("code in return")
            continue
        else:
            # logging.info("not in return, fetch indidual")
            try:
                t = pro.stk_holdernumber(ts_code=ts_code)
            except Exception:
                logging.info("exception on pro.stk_holdernumber")
                continue

            try:
                t = finlib.Finlib().ts_code_to_code(t)
            except Exception:
                logging.info("exception on ts_code_to_code")
                continue

            time.sleep(1)

            # df_holder=df_holder.append(t.head(1))
            df_holder = df_holder.append(t)
            # print(str(df_holder.__len__()))
            df_holder.to_csv(out_csv, encoding='UTF-8', index=False)

    df_holder.reset_index().drop('index', axis=1).to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info("df_holder saved to "+out_csv+" , len "+str(df_holder.__len__()))
    return(df_holder)


def _jie_tao(df, show_piv=False):
    # X = df['close']
    # pivots = zigzag.peak_valley_pivots(preprocessing.minmax_scale(X) + 0.1, 0.1, -0.1)
    # df['piv'] = pivots
    #
    max_idx=df[df['close']==df['close'].max()].index.values[-1] #the latest peak
    min_idx=df[df['close']==df['close'].min()].index.values[-1] #the latest valley

    if min_idx > max_idx:
        return

    code = df['code'].iloc[0]
    max = df.iloc[max_idx]['close']
    min = df.iloc[min_idx]['close']
    close = df['close'].iloc[-1]

    inc = round((max-min)*100/min, 1)
    if inc < 10:
        return

    dec = round((max-close)*100/max, 1)

    jian_cang = round(max - 0.2*(max-min),1)
    bu_cang = round(max*(1 - 0.222857143),1)
    jie_tao = round((max/1.2),1)

    bu=False
    che=False

    if close < bu_cang:
        # print(f"code {code}, bucang. close {str(close)} < bucang {str(bu_cang)}")
        bu=True
    elif close < jian_cang:
        # print(f"code {code}, che. close {str(close)} < jian_cang {str(jian_cang)}, bucang {str(bu_cang)}")
        che=True

    if bu == False and che == False:
        return


    rtn_dict={
        'code':[code],
        'close': [close],
        "che": [che],
        "jian_cang": [jian_cang],

        "bu_cang": [bu_cang],
        "bu": [bu],


        "min": [min],
        "max":[max],
        "inc":[inc],
        "dec":[dec],

        "min_date":[df.iloc[min_idx]['date']],
        "max_date":[df.iloc[max_idx]['date']],



        "jie_tao":[jie_tao],
    }
    


    df_s = pd.DataFrame.from_dict(rtn_dict)
    return(df_s)

    if show_piv:

        plt.xlim(0, len(X))
        plt.ylim(X.min() * 0.99, X.max() * 1.01)
        plt.plot(np.arange(len(X)), X, 'k:', alpha=0.5)
        plt.plot(np.arange(len(X))[pivots != 0], X[pivots != 0], 'k-')
        plt.scatter(np.arange(len(X))[pivots == 1], X[pivots == 1], color='g')
        plt.scatter(np.arange(len(X))[pivots == -1], X[pivots == -1], color='r')

        # plt.annotate(X[pivots == 1].values, xy=(np.arange(len(X))[pivots == 1], X[pivots == 1].values))
        # plt.annotate(X[pivots == -1], xy=(np.arange(len(X))[pivots == -1], X[pivots == -1]))

        plt.show()

    return(rtn_dict)


def cmp_with_idx_inc(of,debug=False):
    if finlib.Finlib().is_cached(file_path=of,day=1):
        logging.info("loading from " +of)
        return(pd.read_csv(of))

    f='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    df = finlib.Finlib().regular_read_csv_to_stdard_df(f)
    # df = df.tail(35).head(30)
    # zzplot(df.reset_index().drop('index',axis=1))
    df = df[-1000:]
    df['v_pct_chg']=df[['volume']].pct_change()
    df=df.sort_values(by='v_pct_chg',ascending=False)
    df.head(5)


    ############
    compare_date='20220101'
    f='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv'
    dfi = finlib.Finlib().regular_read_csv_to_stdard_df(f)
    dfi = dfi[dfi['date']>=compare_date].reset_index().drop('index', axis=1)
    dfi['ac'] = round(dfi['pct_chg'].cumsum(),1)

    dfa=finlib.Finlib().load_all_ag_qfq_data(days=200)
    df_rst_jin_cha=pd.DataFrame()

    for c in dfa['code'].unique():
        if debug:
            c = 'SZ301001' #ryan debug

        dfs = dfa[dfa['code']==c]
        dfs = dfs[dfs['date']>=compare_date].reset_index().drop('index', axis=1)
        dfs['ac'] = round(dfs['pct_chg'].cumsum(),1)
        dfs = pd.merge(left=dfs,right=dfi,on='date',suffixes=('','_idx'))
        dfs = dfs[['code','date','close','ac','ac_idx']]
        # dfs['ac'] = dfs['ac'].rolling(window=10).mean()
        # dfs['ac_idx'] = dfs['ac_idx'].rolling(window=10).mean()

        dfs, df_si_cha, df_jin_cha = finlib_indicator.Finlib_indicator().slow_fast_across(df=dfs,fast_col_name='ac',slow_col_name='ac_idx')
        if dfs.__len__() == 0:
            continue

        # if df_si_cha.__len__() > 0:
        #     d = (datetime.datetime.today()  - datetime.datetime.strptime(df_si_cha.iloc[-1]['date'], '%Y%m%d')).days
        #     if d < 7:
        #         logging.info("sicha, cross down index")
        #         logging.info(finlib.Finlib().pprint(df_si_cha))

        if df_jin_cha.__len__() > 0:
            df_rst_jin_cha = df_rst_jin_cha.append(df_jin_cha)

            d = (datetime.datetime.today()  - datetime.datetime.strptime(df_jin_cha.iloc[-1]['date'], '%Y%m%d')).days
            if d < 7 and df_jin_cha.iloc[-1]['ac'] > -5:
                logging.info("jincha, cross up index")
                logging.info(finlib.Finlib().pprint(df_jin_cha))
                dfs.set_index(keys=['date'])[['ac', 'ac_idx']].plot()
                print("")

    df_rst_jin_cha.to_csv(of, encoding='UTF-8', index=False)
    logging.info("jin cha result saved to "+of)


def jie_tao(of,debug=False):
    if finlib.Finlib().is_cached(file_path=of,day=1):
        logging.info("loading from " +of)
        return(pd.read_csv(of))

    f = '/home/ryan/DATA/DAY_Global/AG_qfq/ag_all_28_days.csv'
    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=f)
    df_rtn=pd.DataFrame()
    fo = "/home/ryan/DATA/result/che_bu.csv"

    lst=df['code'].unique()
    if debug:
        lst=lst[:100]

    for c in lst:
        df_s = df[df['code']==c].reset_index().drop('index',axis=1)
        # print(df_s)
        rtn = _jie_tao(df_s)
        if type(rtn) == type(None):
            continue
        elif type(rtn)!=type(pd.DataFrame()):
            continue

        df_rtn = df_rtn.append(rtn)
        # df_rtn.to_csv(fo,index=False)

    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)
    # df_rtn = finlib.Finlib().add_amount_mktcap(df=df_rtn,mktcap_unit="100M")
    # df_rtn = finlib.Finlib().add_industry_to_df(df=df_rtn)
    # logging.info(finlib.Finlib().pprint(df_rtn.head(5)))
    df_rtn.to_csv(fo,index=False)
    logging.info(f"result saved to {fo}")
    return(df_rtn)


def stock_holder_check():
    df_holder = fetch_holder()
    df_holder[df_holder['code'] == 'SZ300661']

    # choose the latest df_holder
    df_holder_latest = df_holder[df_holder['holder_num'] > 500]
    df_holder_latest = df_holder_latest[~df_holder_latest['holder_num'].isna()]
    df_holder_latest = df_holder_latest.drop_duplicates(subset=['code'], keep='first')

    # merge
    df_amt = finlib.Finlib().get_daily_amount_mktcap()
    df = pd.merge(left=df_holder_latest, right=df_amt, on='code', how='inner')
    df = df[['code', 'holder_num', 'total_mv', 'circ_mv', 'close', 'pe', 'turnover_rate']]
    df['mv_per_holder'] = round(df['circ_mv'] / df['holder_num'], 0)
    df = finlib.Finlib().add_stock_name_to_df(df=df)

    df = df.sort_values(by='mv_per_holder', ascending=False).reset_index().drop('index', axis=1)
    print("top mv_per_holder")
    print(finlib.Finlib().pprint(df.head(50)))

    a = finlib.Finlib().remove_garbage(df=df)
    print(finlib.Finlib().pprint(a.head(50)))


def new_share_profit(csv_o):
    if finlib.Finlib().is_cached(csv_o):
        logging.info("reading from "+csv_o)
        return(pd.read_csv(csv_o))

    csv_i="/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/new_share.csv"

    df=pd.read_csv(csv_i, converters={'ipo_date': str,'issue_date': str})
    # df = finlib.Finlib().ts_code_to_code(df=df)

    df_today = finlib.Finlib().get_today_stock_basic()

    df = pd.merge(left=df,right=df_today, on='code', how='inner',suffixes=['_ipo','_now'])
    df['pct_chg'] = round((df['close_now'] - df['price'])/df['price']*100,1)
    df['pe_pct_chg'] = round((df['pe_now'] - df['pe_ipo'])/df['pe_ipo']*100,1)

    df_show = df[df['issue_date'] != None]
    day_1year=(datetime.datetime.today() - datetime.timedelta(days=180)).strftime('%Y%m%d')
    df_show = df_show[df_show['issue_date'] > day_1year]

    df_show = df_show[['code', 'name_now', 'issue_date',  'amount',
                  'market_amount',  'price', 'close_now', 'pct_chg','pe_ipo' ,'pe_now','pe_pct_chg', 'ballot',
                  'area_now', 'industry_now',
                  ]]

    show_cols=['issue_date','code','name_now','pe_ipo','ballot','price','close_now','pct_chg']
    # df_show = df_show.sort_values(by=['pct_chg'], ascending=False)[show_cols].head(10) ##the most increase
    df_show = df_show.sort_values(by=['pct_chg'], ascending=True)[show_cols] ##the most decrease

    logging.info("The most dropped 10 new shares")
    logging.info(finlib.Finlib().pprint(df_show.head(10)))
    df_show.to_csv(csv_o, encoding="UTF-8", index=False)
    logging.info("new share profit saved to " + csv_o+" len "+str(df_show.__len__()))


def chen_ben_cufflinks():
    import cufflinks as cf

    import chart_studio
    chart_studio.tools.set_credentials_file(username='li.yong', api_key='NEytWmfBUJs4TY3f5ASX')

    import warnings
    warnings.filterwarnings("ignore")

    cf.set_config_file(sharing='public', theme='pearl', world_readable=True, offline=True)
    cf.go_online()

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)
    for code in df['code'].unique():
        # logging.info(f"code {code}")
        code = 'SH600006'
        adf = df[df['code'] == code]
        adf['chen_ben_price'] = round(adf['amount'] * 10 / adf['volume'], 2)

        adf['chen_ben_ma_5'] = round(adf['amount'].rolling(window=5).sum() * 10 / adf['volume'].rolling(window=5).sum(),
                                     2)
        # adf['amount'].rolling(window=5).sum()
        # adf['volume'].rolling(window=5).sum()
        a = adf.set_index('date')

        # save image 1.png
        a['chen_ben_ma_5'].iplot(title='t',
                                 color=['red'],
                                 online=False,
                                 asImage=True,
                                 display_image=True,
                                 filename="/home/ryan/1")

        # plot to https://chart-studio.plotly.com/~li.yong/7/#/
        a['chen_ben_ma_5'].iplot(title='t',
                                 color=['red'],
                                 online=False,
                                 asPlot=True,
                                 display_image=True,
                                 )

        print(1)



def chen_ben_matplot():

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)
    for code in df['code'].unique():
        # logging.info(f"code {code}")
        code = 'SH600006'
        adf = df[df['code'] == code]
        adf['chen_ben_price'] = round(adf['amount'] * 10 / adf['volume'], 2)

        adf['chen_ben_ma_1'] = round(adf['amount'].rolling(window=1).sum() * 10 / adf['volume'].rolling(window=1).sum(),2)
        adf['chen_ben_ma_5'] = round(adf['amount'].rolling(window=5).sum() * 10 / adf['volume'].rolling(window=5).sum(),2)
        adf['chen_ben_ma_4'] = round(adf['amount'].rolling(window=4).sum() * 10 / adf['volume'].rolling(window=4).sum(),2)
        adf['chen_ben_ma_10'] = round(adf['amount'].rolling(window=10).sum() * 10 / adf['volume'].rolling(window=10).sum(),2)
        adf['chen_ben_ma_20'] = round(adf['amount'].rolling(window=20).sum() * 10 / adf['volume'].rolling(window=20).sum(),2)
        adf['chen_ben_ma_60'] = round(adf['amount'].rolling(window=60).sum() * 10 / adf['volume'].rolling(window=60).sum(),2)
        adf['chen_ben_ma_27'] = round(adf['amount'].rolling(window=27).sum() * 10 / adf['volume'].rolling(window=27).sum(),2)
        a = adf.set_index('date')

        # save image 1.png
        a[['chen_ben_ma_5', 'close']].plot()
        a[['chen_ben_ma_4', 'close']].plot()
        a[['chen_ben_ma_10', 'close']].plot()
        a[['chen_ben_ma_20', 'close']].plot()
        a[['chen_ben_ma_27','chen_ben_ma_4', 'chen_ben_ma_60']].plot()
        a[['chen_ben_ma_10','chen_ben_ma_1']].plot()

        a[['close_5_sma','chen_ben_ma_5']].plot()




        print(1)


#连续小涨
def small_up():
    out_csv =  '/home/ryan/DATA/result/small_up.csv'
    rtn_df = pd.DataFrame()

    df = finlib.Finlib().load_all_ag_qfq_data(days=300)
    for code in df['code'].unique():
        # logging.info(f"code {code}")
        # code = 'SH600006'
        adf = df[df['code'] == code]

        low_60 = adf['close'][-60:].min()
        low_120 = adf['close'][-120:].min()
        high_60 = adf['close'][-60:].max()
        high_120 = adf['close'][-120:].max()
        close = adf['close'].iloc[-1]

        dis_c_low60 = round((close-low_60)/low_60 *100,1)
        dis_c_low120 = round((close-low_120)/low_120 *100,1)

        dis_c_high60 = round((high_60-close)/high_60 *100,1)
        dis_c_high120 = round((high_120-close)/high_120 *100,1)


        # last_n_days=10
        last_n_days=7
        # 最近10个交易日，涨多跌少
        # t = adf[-15:-3]  # 12 lines
        t = adf[-1*last_n_days:]  # 12 lines

        t_len = t.__len__()
        up_len = t[t['pct_chg'] > 0].__len__()

        up_ratio = 0
        if t_len>0:
            up_ratio = round(up_len/t_len,2)


        # 涨的都不大
        pct_chg_mean = t['pct_chg'].mean()
        pct_chg_max = t['pct_chg'].max()

        # optional　前一低点，保证目前还在低位
        # optional volume_ratio, or amount_ratio
        # optional, MA5 > MA20

        rtn_df = rtn_df.append(pd.DataFrame({
            'code':[code],
            'days':[last_n_days],
            'up_ratio':[up_ratio],
            'pct_chg_mean':[pct_chg_mean],
            'pct_chg_max':[pct_chg_max],

            'dis_c_low60':[dis_c_low60],
            'dis_c_low120':[dis_c_low120],

            'dis_c_high60':[dis_c_high60],
            'dis_c_high120':[dis_c_high120],
        }))

        # Report
        # if up_ratio > 0.7 and pct_chg_mean < 5 and pct_chg_max < 8:
        #     logging.info(f"code {code}, up_ratio {str(up_ratio)} ")

    rtn_df = finlib.Finlib().add_stock_name_to_df(df=rtn_df)
    rtn_df = rtn_df.sort_values(by=['up_ratio','pct_chg_mean'],ascending=[True,True]).reset_index().drop('index', axis=1)

    rtn_df = rtn_df[(rtn_df['up_ratio']>0.6)
                    & (rtn_df['pct_chg_mean']<0.6)
                    & (0.2 < rtn_df['pct_chg_mean'])
                    & (rtn_df['dis_c_low60']<10)
    ]

    logging.info(finlib.Finlib().pprint(rtn_df.tail(30)))

    rtn_df.to_csv(out_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved " + out_csv + " . len " + str(rtn_df.__len__()))

    return(rtn_df)



#### MAIN #####
# chen_ben_matplot()
df = small_up()
exit()

date_list= finlib.Finlib().get_nong_li_date(start='20210101',end='20221231')
# date_list= finlib.Finlib().get_nong_li_date(start='20220101',end='20221231')

df_ind = finlib.Finlib().list_index_performance(date_list=date_list)
exit()

df_ind = finlib.Finlib().list_industry_performance(date_list=date_list)
df_cnp = finlib.Finlib().list_concept_performance(date_list=date_list)
print(1)



# df_ps,df_ps_now,df_ps_select = finlib.Finlib().get_a_stock_significant(perc=80,last_n_days=100)
start_d = '20210601'
days=15
period = []
for i in range(days):
    d = (datetime.datetime.strptime(start_d, "%Y%m%d") + datetime.timedelta(i) ).strftime("%Y%m%d")
    period.append(d)


# d1 = finlib.Finlib().list_industry_performance(date_list=period)
# d2 = finlib.Finlib().list_concept_performance(date_list=period)
# d3 = finlib.Finlib().list_stock_performance_in_a_concept(date_list=period, concept="光伏概念")
# d3 = finlib.Finlib().list_stock_performance_in_a_concept(date_list=period, concept="建筑节能")
# d3 = finlib.Finlib().list_stock_performance_in_a_concept(date_list=period, concept="家用电器")
# exit()

rst_dir= "/home/ryan/DATA/result"

if input("Run Comparing Index Increase? [N]")=="Y":
    of=rst_dir+"/cmp_with_idx_inc_jing_cha.csv"
    df = cmp_with_idx_inc(of)
    df = finlib.Finlib().filter_days(df=df,date_col='date',within_days=5)
    logging.info("\n##### cmp_with_idx_inc #####")
    logging.info(finlib.Finlib().pprint(df.head(5)))
    exit()

if input("Run New Share Profit? [N]")=="Y":
    csv_o = rst_dir+"/new_share_profit.csv"
    df = new_share_profit(csv_o)
    logging.info("\n##### new_share_profit #####")
    logging.info(finlib.Finlib().pprint(df.head(5)))
    # exit()

if input("Run Jie Tao? [N]")=="Y":
    csv_o = rst_dir+"/jie_tao.csv"
    df = jie_tao(csv_o)
    logging.info("\n##### jie_tao #####")
    logging.info(finlib.Finlib().pprint(df.head(5)))
    # exit()


if input("Run fudu_daily_check? [N]")=="Y":
    df = fudu_daily_check()
    logging.info("\n##### futu_daily_check #####")
    logging.info(finlib.Finlib().pprint(df.head(5)))
    # exit()


# stock_holder_check()
# exit()
if input("Run stock_vs_index_perf_perc_chg? [Y]") != "N":
    a = stock_vs_index_perf_perc_chg()
    # exit()

if input("Run stock_vs_index_perf_amount? [Y]") != "N":
    a = stock_vs_index_perf_amount()
    # exit()

if input("Run price_amount_increase? [Y]") != "N":
     # startD and endD have to be trading day.
    df_increase = finlib_indicator.Finlib_indicator().price_amount_increase(startD=None, endD=None)
    # exit()

if input("Run bayes_start? [Y]") != "N":
    csv_o = rst_dir+"/bayes.csv"
    df = bayes_start(csv_o)
    # exit()

if input("Run check_stop_loss_based_on_ma_across? [Y]") != "N":
    check_stop_loss_based_on_ma_across()
    # exit()

if input("Run result_effort_ratio ? [Y]") != "N":
    result_effort_ratio()
    # exit()

if input("Run grep_garbage ? [Y]") != "N":
    grep_garbage() #save to files /home/ryan/DATA/result/garbage/*.csv
    # exit()

if input("Run ag_industry_selected? [Y]") != "N":
    df_industry = ag_industry_selected()
    # exit()

if input("Run graham_instinsic_value? [Y]") != "N":
    df_intrinsic_value = graham_intrinsic_value()


# pro = ts.pro_api(token="4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b")
# df = pro.stk_holdernumber(ts_code='300199.SZ', start_date='20160101', end_date='20181231')

