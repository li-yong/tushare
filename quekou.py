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

# -*- coding: utf-8 -*-
import time
import akshare as ak
from snownlp import SnowNLP


def quekou_up(mkt):
    df_rst_gaokai_gaozou = pd.DataFrame()
    df_rst_dikai_gaozou = pd.DataFrame()
    df_rst_gaokai_quekou = pd.DataFrame()

    csv_out_dikai_gaozou = f"/home/ryan/DATA/result/dikai_gaozou_{mkt}.csv"
    csv_out_gaokai_gaozou = f"/home/ryan/DATA/result/gaokai_gaozou_{mkt}.csv"
    csv_out_gaokai_quekou = f"/home/ryan/DATA/result/gaokai_quekou_{mkt}.csv"

    n_days = 5
    if mkt == 'AG':
        df = finlib.Finlib().load_all_ag_qfq_data(days=n_days)
    if mkt == 'US_AK':
        df = finlib.Finlib().load_all_us_ak_data(days=n_days)
        df = finlib.Finlib().filter_mktcap_top_US_AK(df=df,topN=1000)

    mkt = finlib.Finlib().find_df_market(df.head(1000))

    #ryan debug
    # df = df[df['code']=='SZ300364']

    for code in df['code'].unique():
        dfs=df[df['code']==code].reset_index().drop('index',axis=1)

        if dfs.__len__()<n_days:
            continue

        dfs['pre_low']=dfs['low'].shift(1)
        dfs['pre_high']=dfs['high'].shift(1)
        dfs['pre_close']=dfs['close'].shift(1)

        dfs = dfs.tail(n_days).reset_index().drop('index',axis=1)
        dfs['tiaokong_kaipan_quekou'] = round((dfs['open']-dfs['pre_close'])/dfs['pre_close']*100,2) #<0, tiaokong dikai, >0 tiaokong gaokai
        dfs['zhengfu'] = round((dfs['close'] - dfs['open'])/dfs['open']*100,2)


        df_tmp_long_gaokai_gaozou = dfs[ (dfs['tiaokong_kaipan_quekou']>1) & (dfs['zhengfu']>3) & (dfs['low']>dfs['pre_high']) & (dfs['close']> dfs['open'])]  #long, kanzhang. tiaokong gaokai + gaokai gaozou
        df_tmp_long_dikai_gaozou = dfs[ (dfs['tiaokong_kaipan_quekou']<-1) & (dfs['zhengfu']>3) & (dfs['close']>dfs['pre_high']) & (dfs['close']> dfs['open'])]  #long, tiaokong dikai + dikai gaozou

        # if df_tmp_long_dikai_gaozou.__len__()>0:
        #     print(finlib.Finlib().pprint(df_tmp_long_dikai_gaozou))
        
        if df_tmp_long_gaokai_gaozou.__len__()>0:
            for index, row in df_tmp_long_gaokai_gaozou.iterrows():
                if dfs.index.max() == index:
                    print("tiaokong quekou")
                    break

                if dfs[index+1:]['low'].min() > row['pre_high']:
                    xiayan = row['pre_high']
                    shangyan = dfs[index:]['low'].min()
                    curP=dfs['close'].iloc[-1]
                    print("tiaokong quekou. "+row['code']+", "+row['date']+", xiayan: "+str(xiayan)+" shangyan: "+str(shangyan)+" curP:"+str(curP))
                    df_rst_gaokai_quekou = pd.concat([df_rst_gaokai_quekou, 
                                                      pd.DataFrame({
                                                          'code': [row['code']],
                                                          'date': [row['date']],
                                                          'qk_xy': [xiayan],
                                                          'qk_sy': [shangyan],
                                                          'cur_p': [curP],
                                                          'tiaokong_kaipan_quekou': [row['tiaokong_kaipan_quekou']],
                                                          'zhengfu': [row['zhengfu']],
                                                      })
                                                      ]).reset_index().drop('index', axis=1)

            print(finlib.Finlib().pprint(df_tmp_long_gaokai_gaozou))



        df_rst_dikai_gaozou = pd.concat([df_rst_dikai_gaozou, df_tmp_long_dikai_gaozou]).reset_index().drop('index', axis=1)
        df_rst_gaokai_gaozou = pd.concat([df_rst_gaokai_gaozou, df_tmp_long_gaokai_gaozou]).reset_index().drop('index', axis=1)

        pass #end of for loop

    if mkt == 'AG':
        df_rst_dikai_gaozou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_dikai_gaozou))
        df_rst_gaokai_gaozou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_gaokai_gaozou))

        df_rst_dikai_gaozou = finlib.Finlib().add_stock_name_to_df(df_rst_dikai_gaozou)
        df_rst_gaokai_gaozou = finlib.Finlib().add_stock_name_to_df(df_rst_gaokai_gaozou)


        #quekou
        df_rst_gaokai_quekou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_gaokai_quekou))
        df_rst_gaokai_quekou = finlib.Finlib().add_stock_name_to_df(df_rst_gaokai_quekou)

    
    if mkt== 'US_AK':
        df_rst_dikai_gaozou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_dikai_gaozou)
        df_rst_gaokai_gaozou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_gaokai_gaozou)
        df_rst_gaokai_quekou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_gaokai_quekou)


    df_rst_dikai_gaozou.to_csv(csv_out_dikai_gaozou, encoding='UTF-8', index=False)
    df_rst_gaokai_gaozou.to_csv(csv_out_gaokai_gaozou, encoding='UTF-8', index=False)

    df_rst_gaokai_quekou.to_csv(csv_out_gaokai_quekou, encoding='UTF-8', index=False)

    print("df_rst_dikai_gaozou:\n"+finlib.Finlib().pprint(+df_rst_dikai_gaozou.head(10)))
    print("df_rst_gaokai_gaozou:\n"+finlib.Finlib().pprint(df_rst_gaokai_gaozou.head(10)))
    print("df_rst_gaokai_quekou:\n"+finlib.Finlib().pprint(df_rst_gaokai_quekou.head(10)))


    logging.info('completed, csv_out_dikai_gaozou save to '+csv_out_dikai_gaozou +" len "+ str(df_rst_dikai_gaozou.__len__()))
    logging.info('completed, csv_out_gaokai_gaozou save to '+csv_out_gaokai_gaozou +" len "+ str(df_rst_gaokai_gaozou.__len__()))
    logging.info('completed, csv_out_gaokai_quekou save to '+csv_out_gaokai_quekou +" len "+ str(df_rst_gaokai_quekou.__len__()))



    return()

def quekou_dn(mkt):
    df_rst_gaokai_dizou = pd.DataFrame()
    df_rst_dikai_dizou = pd.DataFrame()
    df_rst_dikai_quekou = pd.DataFrame()

    csv_out_gaokai_dizou = f"/home/ryan/DATA/result/gaokai_dizou_{mkt}.csv"
    csv_out_dikai_dizou = f"/home/ryan/DATA/result/dikai_dizou_{mkt}.csv"
    csv_out_dikai_quekou = f"/home/ryan/DATA/result/dikai_quekou_{mkt}.csv"

    n_days = 5
    if mkt == 'AG':
        df = finlib.Finlib().load_all_ag_qfq_data(days=n_days)
    if mkt == 'US_AK':
        df = finlib.Finlib().load_all_us_ak_data(days=n_days)
        df = finlib.Finlib().filter_mktcap_top_US_AK(df=df,topN=1000)

    mkt = finlib.Finlib().find_df_market(df.head(1000))


    for code in df['code'].unique():
        # code='SH600895'
        dfs=df[df['code']==code].reset_index().drop('index',axis=1)

        if dfs.__len__()<n_days:
            continue

        dfs['pre_low']=dfs['low'].shift(1)
        dfs['pre_high']=dfs['high'].shift(1)
        dfs['pre_close']=dfs['close'].shift(1)

        dfs = dfs.tail(n_days).reset_index().drop('index',axis=1)
        dfs['tiaokong_kaipan_quekou'] = round((dfs['open']-dfs['pre_close'])/dfs['pre_close']*100,2) #<0, tiaokong dikai, >0 tiaokong dikai
        dfs['zhengfu'] = round((dfs['close'] - dfs['open'])/dfs['open']*100,2)


        df_tmp_long_gaokai_dizou = dfs[ (dfs['tiaokong_kaipan_quekou']>1) & (dfs['zhengfu']<-3) & (dfs['close']<dfs['pre_low']) & (dfs['close']< dfs['open'])]  #long, kanzhang. tiaokong dikai + dikai dizou
        df_tmp_long_dikai_dizou = dfs[ (dfs['tiaokong_kaipan_quekou']<-1) & (dfs['zhengfu']<-3) & (dfs['high']<dfs['pre_low']) & (dfs['close']< dfs['open'])]  #long, tiaokong dikai + dikai dizou

        # if df_tmp_long_dikai_dizou.__len__()>0:
        #     print(finlib.Finlib().pprint(df_tmp_long_dikai_dizou))
        
        if df_tmp_long_dikai_dizou.__len__()>0:
            for index, row in df_tmp_long_dikai_dizou.iterrows():
                if dfs.index.max() == index:
                    print("tiaokong quekou")
                    break

                if dfs[index+1:]['high'].min() < row['pre_low']:
                    shangyan = row['pre_low']
                    xiayan = dfs[index:]['high'].max()
                    curP=dfs['close'].iloc[-1]
                    print("dn tiaokong quekou. "+row['code']+", "+row['date']+", shangyan: "+str(shangyan)+" xiayan: "+str(xiayan)+" curP:"+str(curP))
                    df_rst_dikai_quekou = pd.concat([df_rst_dikai_quekou, 
                                                      pd.DataFrame({
                                                          'code': [row['code']],
                                                          'date': [row['date']],
                                                          'qk_xy': [xiayan],
                                                          'qk_sy': [shangyan],
                                                          'cur_p': [curP],
                                                          'tiaokong_kaipan_quekou': [row['tiaokong_kaipan_quekou']],
                                                          'zhengfu': [row['zhengfu']],
                                                      })
                                                      ]).reset_index().drop('index', axis=1)

            print(finlib.Finlib().pprint(df_tmp_long_dikai_dizou))



        df_rst_gaokai_dizou = pd.concat([df_rst_gaokai_dizou, df_tmp_long_gaokai_dizou]).reset_index().drop('index', axis=1)
        df_rst_dikai_dizou = pd.concat([df_rst_dikai_dizou, df_tmp_long_dikai_dizou]).reset_index().drop('index', axis=1)

        pass #end of for loop

    if mkt == 'AG':
        df_rst_gaokai_dizou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_gaokai_dizou))
        df_rst_dikai_dizou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_dikai_dizou))

        df_rst_gaokai_dizou = finlib.Finlib().add_stock_name_to_df(df_rst_gaokai_dizou)
        df_rst_dikai_dizou = finlib.Finlib().add_stock_name_to_df(df_rst_dikai_dizou)


        #quekou
        df_rst_dikai_quekou = finlib.Finlib().add_industry_to_df(finlib.Finlib().add_amount_mktcap(df_rst_dikai_quekou))
        df_rst_dikai_quekou = finlib.Finlib().add_stock_name_to_df(df_rst_dikai_quekou)

    
    if mkt== 'US_AK':
        df_rst_gaokai_dizou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_gaokai_dizou)
        df_rst_dikai_dizou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_dikai_dizou)
        df_rst_dikai_quekou = finlib.Finlib().add_amount_mkcap_industry_pe_US_AK(df_rst_dikai_quekou)


    df_rst_gaokai_dizou.to_csv(csv_out_gaokai_dizou, encoding='UTF-8', index=False)
    df_rst_dikai_dizou.to_csv(csv_out_dikai_dizou, encoding='UTF-8', index=False)

    df_rst_dikai_quekou.to_csv(csv_out_dikai_quekou, encoding='UTF-8', index=False)

    print("df_rst_gaokai_dizou:\n"+finlib.Finlib().pprint(+df_rst_gaokai_dizou.head(10)))
    print("df_rst_dikai_dizou:\n"+finlib.Finlib().pprint(df_rst_dikai_dizou.head(10)))
    print("df_rst_dikai_quekou:\n"+finlib.Finlib().pprint(df_rst_dikai_quekou.head(10)))


    logging.info('completed, csv_out_gaokai_dizou save to '+csv_out_gaokai_dizou+" len "+str(df_rst_gaokai_dizou.__len__()))
    logging.info('completed, csv_out_dikai_dizou save to '+csv_out_dikai_dizou+" len "+str(df_rst_dikai_dizou.__len__()))
    logging.info('completed, csv_out_dikai_quekou save to '+csv_out_dikai_quekou+" len "+str(df_rst_dikai_quekou.__len__()))



    return()

def quekou_show_up(mkt,report_today):
    if mkt == 'AG':
        today = finlib.Finlib().get_last_trading_day() #'20230925"
    if mkt == 'US_AK':
        today = finlib.Finlib().get_last_trading_day_us()

    csv_out_gaokai_gaozou = f"/home/ryan/DATA/result/gaokai_gaozou_{mkt}.csv"
    csv_out_dikai_gaozou = f"/home/ryan/DATA/result/dikai_gaozou_{mkt}.csv"
    csv_out_gaokai_quekou = f"/home/ryan/DATA/result/gaokai_quekou_{mkt}.csv"

    df_rst_gaokai_gaozou = pd.read_csv(csv_out_gaokai_gaozou)
    df_rst_dikai_gaozou = pd.read_csv(csv_out_dikai_gaozou)
    df_rst_gaokai_quekou = pd.read_csv(csv_out_gaokai_quekou)

    df_rst_gaozou = pd.concat([df_rst_gaokai_gaozou,df_rst_dikai_gaozou])

    if report_today:
        df_rst_gaokai_gaozou = df_rst_gaokai_gaozou[df_rst_gaokai_gaozou['date'].apply(lambda _d: str(_d))==today]
        df_rst_dikai_gaozou = df_rst_dikai_gaozou[df_rst_dikai_gaozou['date'].apply(lambda _d: str(_d))==today]
        df_rst_gaokai_quekou = df_rst_gaokai_quekou[df_rst_gaokai_quekou['date'].apply(lambda _d: str(_d))==today]    
        df_rst_gaozou = df_rst_gaozou[df_rst_gaozou['date'].apply(lambda _d: str(_d))==today]    

    

    if df_rst_gaokai_quekou.__len__() == 0:
        print('empty df_rst_gaokai_quekou')
    elif mkt == 'AG':
        sr = df_rst_gaokai_quekou.groupby(by='industry_name_L1_L2_L3')['industry_name_L1_L2_L3'].count().sort_values(ascending=True)
        for items in sr.items():
            if items[1]<=1:
                continue
            print('df_rst_gaokai_quekou '+items[0]+" count "+str(items[1]))
            print(finlib.Finlib().pprint(df_rst_gaokai_quekou[df_rst_gaokai_quekou['industry_name_L1_L2_L3']==items[0]]))
    elif mkt == 'US_AK':
        print('df_rst_gaokai_quekou ')
        print(finlib.Finlib().pprint(df_rst_gaokai_quekou))


    if df_rst_gaozou.__len__()==0:
        print("empty df_rst_gaozou")
    elif mkt == 'AG':
        sr = df_rst_gaozou.groupby(by='industry_name_L1_L2_L3')['industry_name_L1_L2_L3'].count().sort_values(ascending=True)
        for items in sr.items():
            if items[1]<=1:
                continue
            print('df_rst_gaozou '+items[0]+" count "+str(items[1]))
            print(finlib.Finlib().pprint(df_rst_gaozou[df_rst_gaozou['industry_name_L1_L2_L3']==items[0]]))
    elif mkt == 'US_AK':
        print('df_rst_gaozou ')
        print(finlib.Finlib().pprint(df_rst_gaozou))
    
    return()

def quekou_show_dn(mkt, report_today):
    if mkt == 'AG':
        today = finlib.Finlib().get_last_trading_day() #'20230925"
    if mkt == 'US_AK':
        today = finlib.Finlib().get_last_trading_day_us() #'2023-09-25'
        today = today.replace("-",'')


    csv_out_gaokai_dizou = f"/home/ryan/DATA/result/gaokai_dizou_{mkt}.csv"
    csv_out_dikai_dizou = f"/home/ryan/DATA/result/dikai_dizou_{mkt}.csv"
    csv_out_dikai_quekou = f"/home/ryan/DATA/result/dikai_quekou_{mkt}.csv"

    df_rst_gaokai_dizou = pd.read_csv(csv_out_gaokai_dizou)
    df_rst_dikai_dizou = pd.read_csv(csv_out_dikai_dizou)
    df_rst_dikai_quekou = pd.read_csv(csv_out_dikai_quekou)

    if report_today:
        df_rst_gaokai_dizou = df_rst_gaokai_dizou[df_rst_gaokai_dizou['date'].apply(lambda _d: str(_d))==today]
        df_rst_dikai_dizou = df_rst_dikai_dizou[df_rst_dikai_dizou['date'].apply(lambda _d: str(_d))==today]
        df_rst_dikai_quekou = df_rst_dikai_quekou[df_rst_dikai_quekou['date'].apply(lambda _d: str(_d))==today]

    df_rst_dizou = pd.concat([df_rst_gaokai_dizou,df_rst_dikai_dizou])

    if df_rst_dikai_quekou.__len__()==0:
        print("empty df_rst_dikai_quekou")
    elif mkt == 'AG':
        sr = df_rst_dikai_quekou.groupby(by='industry_name_L1_L2_L3')['industry_name_L1_L2_L3'].count().sort_values(ascending=True)
        for items in sr.items():
            if items[1]<=1:
                continue
            print('df_rst_dikai_quekou '+items[0]+" count "+str(items[1]))
            print(finlib.Finlib().pprint(df_rst_dikai_quekou[df_rst_dikai_quekou['industry_name_L1_L2_L3']==items[0]]))
    elif mkt == 'US_AK':
        print('df_rst_dikai_quekou ')
        print(finlib.Finlib().pprint(df_rst_dikai_quekou))
        

    if df_rst_dizou.__len__()==0:
        print("empty df_rst_dizou")
    elif mkt=='AG':
        sr = df_rst_dizou.groupby(by='industry_name_L1_L2_L3')['industry_name_L1_L2_L3'].count().sort_values(ascending=True)
        for items in sr.items():
            if items[1]<=1:
                continue
            print('df_rst_dizou '+items[0]+" count "+str(items[1]))
            print(finlib.Finlib().pprint(df_rst_dizou[df_rst_dizou['industry_name_L1_L2_L3']==items[0]]))
    elif mkt == 'US_AK':
        print('df_rst_dizou:')
        print(finlib.Finlib().pprint(df_rst_dizou))

    return()




#### MAIN #####

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json

from optparse import OptionParser
parser = OptionParser()

parser.add_option("-n", "--no_question", action="store_true", default=False, dest="no_question", help="run all without question.")
parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="debug mode.")

parser.add_option("-a", "--action", dest="action", help="run|report")
parser.add_option("--report_today", action="store_true", default=False, dest="report_today", help="only list today's report")

parser.add_option("-x", "--stock_global", dest="stock_global", help="AG|US_AK")



(options, args) = parser.parse_args()
no_question = options.no_question
debug = options.debug
mkt = options.stock_global
action = options.action
report_today = options.report_today

if mkt not in ['AG','US_AK']:
    print(f"unknown mkt {mkt}. support [AG|US_AK]")
if action not in ['run','report']:
    print(f"unknown action {action}, support [run|report]")




if action == 'run':
    # quekou_dn(mkt=mkt)
    quekou_up(mkt=mkt)

if action == 'report':
    quekou_show_dn(mkt=mkt,report_today=report_today)
    quekou_show_up(mkt=mkt,report_today=report_today)

exit()