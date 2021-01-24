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
import logging
import tabulate
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#this script comparing a stock's data in history, to get the lower PE/PB/ROE in the history (cheap & high grow rate) stock.


def save_df(df,csv,note,sortby,ascending):
    df = df.sort_values(sortby, ascending=ascending, inplace=False)
    df = df.reset_index().drop('index', axis=1)
    df.to_csv(csv, encoding='UTF-8', index=False)
    logging.info("\n"+str(note)+", " + csv + " len " + str(df.__len__()))
    if df.__len__() > 5:
        logging.info(tabulate.tabulate(df[0:5], headers='keys', tablefmt='psql'))
    else:
        logging.info(tabulate.tabulate(df, headers='keys', tablefmt='psql'))

   

def select_from_result():
    base_dir = "/home/ryan/DATA/result"
    input_csv = base_dir+"/pe_pb_rank.csv"
    
    output_low_pe = base_dir+"/low_pe_top30p.csv"
    output_low_pb = base_dir+"/low_pb_top30p.csv"
    output_low_ps = base_dir+"/low_ps_top30p.csv"

    output_high_pe = base_dir+"/high_pe_top30p.csv"
    output_high_pb = base_dir+"/high_pb_top30p.csv"
    output_high_ps = base_dir+"/high_ps_top30p.csv"

    output_uv = base_dir+"/under_valued.csv"
    output_mb = base_dir+"/market_bless.csv"
    


    output_high_roe = base_dir+"/high_roe_top30.csv"

    df_uv = pd.DataFrame()
    df_mb = pd.DataFrame()

    df = pd.read_csv(input_csv, converters={'end_date': str})
    
    df_low_pe = df[df['pe_ttm_perc'] < 0.3]
    df_low_pb = df[df['pb_perc'] < 0.3]
    df_low_ps = df[df['ps_ttm_perc'] < 0.3]
    
    df_high_pe = df[df['pe_ttm_perc'] > 0.7]
    df_high_pb = df[df['pb_perc'] > 0.7]
    df_high_ps = df[df['ps_ttm_perc'] > 0.7]
    df_high_roe  = df[df['roe'] > 12 ]
    
    save_df(df=df_low_pe, csv=output_low_pe, note="low pe, pe_ttm_perc<0.3",sortby=['pe_ttm_perc','pe'], ascending=True)
    save_df(df=df_low_pb, csv=output_low_pb, note="low pb, pb_ttm_perc<0.3",sortby='pb', ascending=True)
    save_df(df=df_low_ps, csv=output_low_ps, note="low ps, ps_ttm_perc<0.3",sortby=['ps_ttm_perc','ps_ttm'], ascending=True)
 
    save_df(df=df_high_pe, csv=output_high_pe, note="high pe, pe_ttm_perc>0.7",sortby=['pe_ttm_perc','pe'], ascending=False)
    save_df(df=df_high_pb, csv=output_high_pb, note="high pb, pb_ttm_perc>0.7",sortby='pb', ascending=False)
    save_df(df=df_high_ps, csv=output_high_ps, note="high ps, ps_ttm_perc>0.7",sortby=['ps_ttm_perc','ps_ttm'], ascending=False)
    
    save_df(df=df_high_roe, csv=output_high_roe, note="high roe, roe>12",sortby='roe', ascending=False)
 
    
    df_uv = df[(df['pe_ttm_perc'] < 0.15) & (df['pb_perc'] < 0.15) & (df['roe'] > 12)]
    df_mb = df[(df['pe_ttm_perc'] > 0.6) & (df['roe'] > 12)]
        
    save_df(df=df_uv, csv=output_uv, note="PE/PB/ROE under valued,pe_ttm_perc<0.15, pb_perc<0.15, roe>12 ",sortby='roe', ascending=False)
    save_df(df=df_mb, csv=output_mb, note="PE/PB/ROE market blessed, pe_ttm_perc>0.6, roe>12",sortby='roe', ascending=False)

 
######
#
######
def get_indicator_history(output_csv):

    df_result = pd.DataFrame()

    #extract roe from fina_indicator.csv
    file = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_indicator.csv"

    df_fina_indicator = pd.read_csv(file, converters={'end_date': str})

    stock_list = finlib.Finlib().get_A_stock_instrment()  #600519
    #stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False) #SH600519
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  #600519.SH

    #stock_list = stock_list[stock_list['code']=='SH600519'] #ryan debug

    #for code_m in stock_list['code']: #code_m: SH600519
    for i in range(stock_list.__len__()):  #code_m: SH600519
        code_m = stock_list.iloc[i]['ts_code']
        stock_name = stock_list.iloc[i]['name']

        #extract PE, PB, PE_TTM
        file_code = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/" + code_m + "_basic.csv"

        if not os.path.exists(file_code):
            print("\nno such file " + file_code)
            continue

        if os.stat(file_code).st_size < 10:
            print('empty input file ' + file_code)
            continue

        logging.info(__file__+" "+code_m)

        df_sub = pd.read_csv(file_code, converters={'code': str})

        if not 'pe_ttm' in df_sub.columns:
            print("no column pe_ttm in the csv, " + file_code)
            continue

        pe_ttm_all = df_sub['pe_ttm']

        if pe_ttm_all.__len__() == 0:
            print("empty df, pe_ttm_all")
            continue

        pe_ttm_today = pe_ttm_all[-1:].values[0]
        pe_ttm_perc = stats.percentileofscore(pe_ttm_all, pe_ttm_today) / 100
        pe_ttm_perc = round(pe_ttm_perc, 2)

        pe_all = df_sub['pe']
        pe_today = pe_all[-1:].values[0]
        pe_perc = stats.percentileofscore(pe_all, pe_today) / 100
        pe_perc = round(pe_perc, 2)

        pb_all = df_sub['pb']
        pb_today = pb_all[-1:].values[0]
        pb_perc = stats.percentileofscore(pb_all, pb_today) / 100
        pb_perc = round(pb_perc, 2)

        ps_ttm_all = df_sub['ps_ttm']
        ps_ttm_today = ps_ttm_all[-1:].values[0]
        ps_ttm_perc = stats.percentileofscore(ps_ttm_all, ps_ttm_today) / 100
        pb_perc = round(pb_perc, 2)

        ps_all = df_sub['ps']
        ps_today = ps_all[-1:].values[0]
        ps_perc = stats.percentileofscore(ps_all, ps_today) / 100
        ps_perc = round(ps_perc, 2)

        latest_date = df_sub['trade_date'].iloc[-1]

        reg = re.match("(\d{4})(\d{2})(\d{2})", str(latest_date))

        if reg:
            yyyy = reg.group(1)
            mm = reg.group(2)
            dd = reg.group(3)
            latest_date = yyyy + "-" + mm + "-" + dd

        close = df_sub['close'].iloc[-1]

        dict = {
            'code': code_m,
            'name': stock_name,
            'date': latest_date,
            'close': close,
            'pe_ttm_perc': pe_ttm_perc,
            'pe_perc': pe_perc,
            'ps_ttm_perc': ps_ttm_perc,
            'ps_perc': ps_perc,
            'pb_perc': pb_perc,
            'pe': pe_today,
            'pe_ttm': pe_ttm_today,
            'ps': ps_today,
            'ps_ttm': ps_ttm_today,
            'pb': pb_today,
        }

        ################

        code_ts = code_m

        df_tmp = df_fina_indicator[df_fina_indicator['ts_code'] == code_ts]

        if df_tmp.__len__() == 0:
            pass
        else:
            roe = float(df_tmp.iloc[0]['roe'])
            dict['roe'] = roe

        ###==================

        df_tmp = pd.DataFrame(dict, index=[i])

        df_result = df_result.append(df_tmp)

        pass

    df_result.to_csv(output_csv, encoding='UTF-8', index=False)
    print("\nPE/PB/ROE self percent result saved to " + output_csv + " len " + str(df_result.__len__()))


def main():
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))
    output_csv = "/home/ryan/DATA/result/pe_pb_rank.csv"
    days = 3
    if not finlib.Finlib().is_cached(file_path=output_csv, day=days):
            get_indicator_history(output_csv)
    else:
        logging.info("output updated in "+str(days)+" days, not calculate again.")

    select_from_result()
    print('script completed')


### MAIN ####
if __name__ == '__main__':
    main()


