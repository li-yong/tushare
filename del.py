import pandas as pd
import finlib

import finlib_indicator

import tushare as ts
import datetime

import talib 
import logging
import time
import os

import math


##############
def load_hs300():
    token = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'

    pro = ts.pro_api()
    pro = ts.pro_api(token=token)

    ts.set_token(token=token)

    csv_hs300 = "/home/ryan/DATA/pickle/hs300.csv"

    if finlib.Finlib().is_cached(file_path=csv_hs300, day = 7):
        df_hs300 = pd.read_csv(csv_hs300)
        logging.info("loading hs300 from "+csv_hs300+" ,len "+str(df_hs300.__len__()))
    else:
        df_hs300 = pro.index_weight(index_code='399300.SZ') #HS300
        df_hs300.to_csv(csv_hs300, encoding='UTF-8', index=False)
        logging.info("hs300 save to " + csv_hs300 + " ,len " + str(df_hs300.__len__()))


    df_hs300.columns = ['index_code', 'code', 'date', 'weight']
    df_hs300 = df_hs300[['code', 'date', 'weight']]
    df_hs300 = finlib.Finlib().ts_code_to_code(df=df_hs300)
    df_hs300 = finlib.Finlib().add_stock_name_to_df(df=df_hs300)

    index_latest_period = df_hs300.date.unique()[0] #'20201201
    df_hs300_latest = df_hs300[df_hs300['date']==index_latest_period]
    logging.info("got hs300 list of period "+str(index_latest_period))
    return(df_hs300_latest)

df_hs300 = load_hs300()


# df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=365, debug=False)
df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=5, debug=True)

# df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=365,debug=False)
df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=5, debug=True)

my_hs300 = finlib.Finlib()._df_sub_by_code(df=df_mktcap, df_sub=df_amt.tail(math.ceil(df_amt.__len__()/2)), byreason="daily average amount less than 50% stocks.")
my_hs300 = my_hs300.head(300)[['code','name','total_mv']]

df_merged = my_hs300.merge(df_hs300,indicator = True, how='outer')
len_merged = df_merged.__len__()

df_both = df_merged[df_merged['_merge']=="both"]
len_both = df_both.__len__()

df_myonly = df_merged[df_merged['_merge']=="left_only"]
len_myonly = df_myonly.__len__()
logging.info(str(len_myonly)+ " out of "+str(len_both)+" in my hs300, they possible will be added to hs300 next time")
logging.info(finlib.Finlib().pprint(df=df_myonly))

df_hs300only = df_merged[df_merged['_merge']=="right_only"] #possible will be removed from hs300 index next time
len_hs300only = df_hs300only.__len__()
logging.info(str(len_hs300only)+ " out of "+str(len_both)+" in offical hs300, they possible will be removed from hs300 next time")
logging.info(finlib.Finlib().pprint(df=df_hs300only))



logging.info(str())


df_szcz = pro.index_weight(index_code='399001.SZ') #深证成指

'''
df_szcz[df_szcz['con_code'].str.contains('300')]
     index_code   con_code trade_date  weight
377   399001.SZ  300773.SZ   20200228  0.0440
378   399001.SZ  300770.SZ   20200228  0.0711

df_szcz[df_szcz['con_code'].str.contains('300001')]
Out[42]: 
     index_code   con_code trade_date  weight
497   399001.SZ  300001.SZ   20200228  0.1392
591   399001.SZ  300001.SZ   20190830  0.1212
1118  399001.SZ  300001.SZ   20190731  0.1433
1614  399001.SZ  300001.SZ   20190628  0.1404
'''

df = pro.index_weight(index_code='399300.SZ', start_date='20180901', end_date='20180930')
df = pro.index_basic(market='SZSE')

df = ts.pro_bar(ts_code='600519.SH', asset='E',freq='1', start_date='20201116', end_date='20201116')



print(1)

exit(0)



################ Filter price under 30 weeks MA,
################ Screen price growth steadly, e.g increase 2 point every days.
################  Looks duplicate with VCP.py at some level,
################  Consider if VCP.py output can be reused.
out_f = "/home/ryan/DATA/result/price_quality.csv"
stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')

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
