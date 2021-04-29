# coding: utf-8
# encoding= utf-8

import pandas as pd
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



###########################
# fund_all = finlib.Finlib().load_all_ts_pro()

df_selected = finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')['stock_list']


df_a_stock_report = df_target.merge(df_selected, on='code',how='inner', suffixes=('', '_x'))
print(finlib.Finlib().pprint(df_a_stock_report))

#########################################
np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)

for market in ['AG','HK','US']:
    logging.info("\n==== "+market+" ====")
    high_field='52 Week High'
    low_field='52 Week Low'

    # all_column = False
    all_column = True
    (df_52week,df_g_n4,df_g_n3_52week,df_g_n2,df_g_n1,df_g_p1_52week,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)

    cols = ['code','name', 'eq_pos','roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)','atr_14','vola_month','volatility']
    print("==========  P1 "+ str(high_field)+"  "+str(low_field)+" ========")
    print(finlib.Finlib().pprint(df_g_p1_52week[cols].head(2)))  #grid == 1

    print("==========  N3 "+ str(high_field)+"  "+str(low_field)+" ========")
    print(finlib.Finlib().pprint(df_g_n3_52week[cols].head(2)))  #grid == -3



    ###################
    high_field='3-Month High'
    low_field ='3-Month Low'

    # all_column = False
    all_column = True
    (df_3month,df_g_n4,df_g_n3_3month,df_g_n2,df_g_n1,df_g_p1_3month,df_g_p2,df_g_p3,df_g_p4) = finlib_indicator.Finlib_indicator().grid_market_overview(market=market,high_field=high_field, low_field=low_field, all_columns=all_column)

    print("==========  P1 "+ str(high_field)+"  "+str(low_field)+" ========")
    print(finlib.Finlib().pprint(df_g_p1_3month[cols].head(2)))  #grid == 1

    print("==========  N3 "+ str(high_field)+"  "+str(low_field)+" ========")
    print(finlib.Finlib().pprint(df_g_n3_3month[cols].head(2)))  #grid == -3

    df = pd.merge(df_52week, df_3month, on='code', how='inner', suffixes=("", "_3M"))
    df_llrh = df[(df.grid==-3) & (df.grid_3M==1)][cols]  #long low, recent high
    df_lhrl = df[(df.grid==1) & (df.grid_3M==-3)][cols]  #long high, recent low

    finlib.Finlib().get_ts_field(ts_code='601995.SH', ann_date='20201231', field='roe', big_memory=False)

    #
    print("roe_ttm", "pe_ttm",'Operating Margin (TTM)','Gross Margin (TTM)')
    _ = df_g_p4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]
    print('p4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])


    _ = df_g_p3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_p1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('p1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])

    _ = df_g_n1[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n1: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n2[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n2: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n3[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n3: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])
    _ = df_g_n4[['roe_ttm','pe_ttm','Operating Margin (TTM)','Gross Margin (TTM)']].describe().iloc[1]; print('n4: ',end='') ;print(_['roe_ttm'], _['pe_ttm'],_['Operating Margin (TTM)'],_['Gross Margin (TTM)'])


    df_g_p4.sort_values('grid_perc_resis_spt_dist', ascending=False, inplace=False).head(5)

    logging.info("")

exit(0)

#########################################
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

#########################################
#time to market
# df = finlib.Finlib().load_all_ts_pro()

df = finlib.Finlib().get_A_stock_instrment()
df = finlib.Finlib().add_market_to_code(df=df)

print(1)


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
