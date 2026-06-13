# coding: utf-8

import finlib
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
import datetime
from optparse import OptionParser
import sys
import os
import logging
import signal

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)

def get_beneish_element(ann_date):
    f = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_" + ann_date + ".csv"
    df = pd.read_csv(f, converters={'end_date': str})

    # Net Sales  净销售额  | c_fr_sale_sg, 销售商品、提供劳务收到的现金
    # Cost of Goods  货品的成本 |  total_cogs, 营业总成本 |  cogs_of_sales, 销售成本率
    # Net Receivables  净应收账款 | accounts_receiv, 应收账款| acc_receivable, 应收款项, payables, 	应付款项
    # Current Assets  流动资产|total_cur_assets, 流动资产合计
    # Property, Plant and Equipment 物业，厂房及设备 | fix_assets, 固定资产
    # Depreciation  折旧|daa,折旧与摊销, depr_fa_coga_dpba, 固定资产折旧、油气资产折耗、生产性生物资产折旧
    # Total Assets 总资产|total_assets, 资产总计
    # Selling, General and Administrative Expenses, 销售，一般和行政费用| sell_exp 减:销售费用  admin_exp	减:管理费用, fin_exp 减:财务费用, rd_exp 研发费用?
    # Net Income 净收入 | n_income 净利润(含少数股东损益)
    # Cash Flow from Operations 运营现金流| n_cashflow_act, 经营活动产生的现金流量净额
    # Current Liabilities 流动负债 | total_cur_liab	 流动负债合计
    # Long-Term Debt  长期债务 | total_ncl, 非流动负债合计

    df['sga_exp'] = df['sell_exp']+df['admin_exp']+df['fin_exp']
    #df['sga_exp'] = df['sell_exp']+df['admin_exp']+df['fin_exp']+df['rd_exp']

    #cols = ['name','ts_code','end_date','c_fr_sale_sg','total_cogs','accounts_receiv','total_cur_assets','fix_assets','daa','depr_fa_coga_dpba','total_assets', 'sga_exp', 'sell_exp','admin_exp','fin_exp','rd_exp','n_income','n_cashflow_act','total_cur_liab','total_ncl']

    cols = ['name', 'ts_code','end_date','c_fr_sale_sg','total_cogs','accounts_receiv','total_cur_assets','fix_assets','depr_fa_coga_dpba','total_assets','sga_exp','sell_exp','admin_exp','fin_exp','n_income','n_cashflow_act','total_cur_liab','total_ncl']

    df = df[cols]

    return(df)

def beneish_calc(ts_code,name, ann_date, dt, dt_1):
    dict_rtn = {'ts_code':ts_code,
                'name':name,
                'ann_date':ann_date,
               }

    #turn_days,营业周期


 #    dt_1 = {'c_fr_sale_sg': 93823,
 # 'total_cogs': 52155,
 # 'accounts_receiv': 1174,
 # 'total_cur_assets': 73717,
 # 'fix_assets': 2532,
 # 'daa': 1696,
 # 'total_assets': 86291,
 # 'sga_exp': 32426,
 # 'n_income': 5741,
 # 'n_cashflow_act': 8416,
 # 'total_cur_liab': 26297,
 # 'total_ncl': 1232}
 #
 #
 #    dt = {'c_fr_sale_sg': 93685,
 # 'total_cogs': 49193,
 # 'accounts_receiv': 1373,
 # 'total_cur_assets': 67991,
 # 'fix_assets': 2058,
 # 'daa': 1716,
 # 'total_assets': 84832,
 # 'sga_exp': 33013,
 # 'n_income': 9888,
 # 'n_cashflow_act': 2877,
 # 'total_cur_liab': 26275,
 # 'total_ncl': 1470}


    #DSRI Days Sales in Receivables Index, 应收账款周转指数
    # DSRI = (Net Receivablest / Salest) / Net Receivablest-1 / Salest-1)
    if (dt['c_fr_sale_sg'] == 0.0 ) or (dt_1['c_fr_sale_sg'] == 0.0 ):
        DSRI = 0
    else:
        _1 = dt['accounts_receiv']/dt['c_fr_sale_sg']
        _2 = dt_1['accounts_receiv']/dt_1['c_fr_sale_sg']
        if _2 == 0:
            DSRI = 0
        else:
            DSRI = _1 / _2
    dict_rtn['DSRI'] = DSRI

    #GMI Gross Margin Index, 毛利率指数
    #GMI = [(Salest-1 - COGSt-1) / Salest-1] / [(Salest - COGSt) / Salest]
    if dt['c_fr_sale_sg'] == 0.0 or dt_1['c_fr_sale_sg'] ==0.0:
        GMI=0
    else:
        _1 = (dt['c_fr_sale_sg'] - dt['total_cogs'])/dt['c_fr_sale_sg']
        _2 = (dt_1['c_fr_sale_sg'] - dt_1['total_cogs'])/dt_1['c_fr_sale_sg']
        if _2 == 0:
            GMI = 0
        else:
            GMI = _1 / _2
    dict_rtn['GMI'] = GMI

    #AQI Asset Quality Index,  资产质量指数
    #AQI = [1 - (Current Assetst + PP&Et + Securitiest) / Total Assetst] / [1 - ((Current Assetst-1 + PP&Et-1 + Securitiest-1) / Total Assetst-1)]
    if dt['total_assets'] == 0.0 or dt_1['total_assets'] ==0.0:
        AQI=0
    else:
        _1 = (dt['total_assets']-dt['fix_assets']-dt['total_cur_assets'])/dt['total_assets']
        _2 = (dt_1['total_assets']-dt_1['fix_assets']-dt_1['total_cur_assets'])/dt_1['total_assets']
        if _2 == 0:
            AQI = 0
        else:
            AQI = _1 / _2
    dict_rtn['AQI'] = AQI


    #SGI Sales Growth Index, 销售增长指数
    #SGI = Salest / Salest-1
    if dt_1['c_fr_sale_sg'] == 0.0:
        SGI = 0
    else:
        SGI = dt['c_fr_sale_sg']/dt_1['c_fr_sale_sg']
    dict_rtn['SGI'] = SGI

    #DEPI Depreciation Index, 折旧指数
    #DEPI = (Depreciationt-1/ (PP&Et-1 + Depreciationt-1)) / (Depreciationt / (PP&Et + Depreciationt))
    # _1 = dt['daa']/(dt['daa']+dt['fix_assets'])
    if (dt['depr_fa_coga_dpba']+dt['fix_assets'])==0.0 or (dt_1['depr_fa_coga_dpba']+dt_1['fix_assets']) ==0:
        DEPI=0
    else:
        _1 = dt['depr_fa_coga_dpba']/(dt['depr_fa_coga_dpba']+dt['fix_assets'])
        # _2 = dt_1['daa']/(dt_1['daa']+dt_1['fix_assets'])
        _2 = dt_1['depr_fa_coga_dpba']/(dt_1['depr_fa_coga_dpba']+dt_1['fix_assets'])
        if _2 == 0:
            DEPI = 0
        else:
            DEPI = _1 / _2
    dict_rtn['DEPI'] = DEPI

    #SGAI Sales, General and Administrative Expenses Index,销售费用指数
    #SGAI = (SG&A Expenset / Salest) / (SG&A Expenset-1 / Salest-1)
    if dt['c_fr_sale_sg'] == 0 or dt_1['c_fr_sale_sg']==0:
        SGAI=0
    else:
        _1 = dt['sga_exp']/dt['c_fr_sale_sg']
        _2 = dt_1['sga_exp']/dt_1['c_fr_sale_sg']
        if _2 ==0:
            SGAI = 0
        else:
            SGAI = _1/_2
    dict_rtn['SGAI'] = SGAI

    #TATA Total Accruals to Total Assets, 应计总数与总资产比率
    #TATA = (Income from Continuing Operationst - Cash Flows from Operationst) / Total Assetst

    if dt_1['total_assets']  ==0:
        TATA=0
    else:
        TATA = (dt_1['n_income']-dt_1['n_cashflow_act'])/dt_1['total_assets']
        dict_rtn['TATA'] = TATA

    #LVGI  Leverage Index,
    #LVGI = [(Current Liabilitiest + Total Long Term Debtt) / Total Assetst] / [(Current Liabilitiest-1 + Total Long Term Debtt-1) / Total Assetst-1]
    if dt['total_assets']==0 or dt['total_assets']==0:
        LVGI=0
    else:
        _1 = (dt['total_ncl']+dt['total_cur_liab'])/dt['total_assets']
        _2 = (dt_1['total_ncl']+dt_1['total_cur_liab'])/dt_1['total_assets']
        if _2 ==0:
            LVGI = 0
        else:
            LVGI = _1/_2
    dict_rtn['LVGI'] = LVGI

    #
    # logging.info("DSRI:"+str(round(DSRI,2)))
    # logging.info("GMI:"+str(round(GMI,2)))
    # logging.info("AQI:"+str(round(AQI,2)))
    # logging.info("SGI:"+str(round(SGI,2)))
    # logging.info("DEPI:"+str(round(DEPI,2)))
    # logging.info("SGAI:"+str(round(SGAI,2)))
    # logging.info("TATA:"+str(round(TATA,2)))
    # logging.info("LVGI:"+str(round(LVGI,2)))
    # logging.info("          ")

    M_5v= -6.065+ .823*DSRI + .906*GMI + .593*AQI + .717*SGI + .107*DEPI
    M_8v= -4.84 + .920* DSRI + .528* GMI + .404* AQI + .892* SGI + .115* DEPI -.172* SGAI  + 4.679*TATA - .327* LVGI
    dict_rtn['M_5v'] = M_5v
    dict_rtn['M_8v'] = M_8v

    # logging.info(str(ts_code)+ " "+name+" "+str(ann_date)   +" M_5v:"+str(round(M_5v,2)))
    logging.info(str(ts_code)+ " "+name+" "+str(ann_date)   +" M_8v:"+str(round(M_8v,2)))

    threshold = -1.78
    if M_8v > threshold :
         logging.info("WARNNING: M_8v great than 3"+ str(ts_code) + " "+str(ann_date) + " "+str(round(M_8v,2)))

    if M_5v > threshold :
         logging.info("WARNNING: M_5v great than 3"+ str(ts_code) + " "+str(ann_date) + " "+str(round(M_5v,2)))

    return(dict_rtn)



def benneish_start(ann_date):
    ##### Benneish start

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(df=stock_list, dot_f=True, tspro_format=True)  # 603999.SH


    out_f = "/home/ryan/DATA/result/ag_beneish.csv"  # /home/ryan/DATA/result/ag_beneish.csv

    df = get_beneish_element(ann_date=ann_date)
    current_date = datetime.datetime.strptime(ann_date, "%Y%m%d")
    df_1 = get_beneish_element(ann_date=str(current_date.year - 1) + '1231')
    df_rst = pd.DataFrame()

    i = 0

    for index, row in stock_list.iterrows():
        i += 1
        name, ts_code = row['name'], row['code']
        logging.info(str(i) + " of " + str(stock_list.__len__()) + " "+str(ts_code)+" "+name)

        #ts_code: 600519.SH

        #ts_code = '600036.SH' #ryan debug
        if ts_code in df['ts_code'].to_list():
            dt =  df[df['ts_code'] == ts_code].reset_index().drop('index', axis=1).iloc[0].to_dict()
        else:
            continue

        if ts_code in df_1['ts_code'].to_list():
            dt_1 =  df_1[df_1['ts_code'] == ts_code].reset_index().drop('index', axis=1).iloc[0].to_dict()
        else:
            continue

        dict_rtn = beneish_calc(ts_code=ts_code,name=name,ann_date=ann_date,dt=dt, dt_1=dt_1)
        df_new = pd.DataFrame().from_dict({0:dict_rtn}).T.reset_index().drop('index',axis=1)
        df_rst = df_rst.append(df_new).reset_index().drop('index',axis=1)

    df_rst.head(2)
    cols = ['ts_code', 'name', 'ann_date', 'M_8v','M_5v','DSRI', 'GMI', 'AQI', 'SGI','DEPI','SGAI','TATA','LVGI']
    df_rst = df_rst[cols]
    df_rst = df_rst.sort_values(by='M_8v')

    #rename and format ts_code to code, 600519.SH --> SH600519
    df_rst = finlib.Finlib().ts_code_to_code(df_rst)


    df_rst.to_csv(out_f, encoding='UTF-8', index=False)
    logging.info("saved beneish output to "+out_f)



def main():
    ########################
    #
    #########################

    pro = ts.pro_api()


    logging.info(__file__ + " " + "\n")
    logging.info(__file__ + " " + "SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    (options, args) = parser.parse_args()

    ann_date = finlib.Finlib().get_year_month_quarter()['ann_date_1y_before']
    benneish_start(ann_date=ann_date)

    logging.info('script completed')
    os._exit(0)


### MAIN ####
#An M-Score of less than -2.22 suggests that the company will not be a manipulator. An M-Score of greater than -2.22 signals that the company is likely to be a manipulator.
if __name__ == '__main__':
    main()
