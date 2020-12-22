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

### MAIN ####
if __name__ == '__main__':
    debug =False
    # debug =True
    ndays = 365
    ndays = 36

    df_hs300 = load_hs300()
    hs300_candiate_csv = "/home/ryan/DATA/result/hs300_candidate_list.csv"

    # df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays, debug=debug)

    # df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=ndays, debug=debug)

    # my_hs300_mktcap_amt = finlib.Finlib()._df_sub_by_code(df=df_mktcap, df_sub=df_amt.tail(math.ceil(df_amt.__len__() / 2)),  byreason="daily average amount less than 50% stocks.")

    #### filter with HS300 critiria
    df_all = finlib.Finlib().get_A_stock_instrment(code_name_only=False)
    df_all = finlib.Finlib().add_market_to_code(df_all)

    print("all lens "+str(df_all.__len__()))

    df_hs300_filted_on_market_days=pd.DataFrame()

    mkt = df_all.market.unique()
    calc_len = 0
    # Out[6]: array(['主板', '中小板', '创业板', '科创板', 'CDR'], dtype=object)
    for m in mkt:
        df_tmp = df_all[df_all['market']==m]
        # print(m + " "+str(df_tmp.__len__()))
        len_df_tmp_ori = df_tmp.__len__()
        calc_len += len_df_tmp_ori

        if m == "科创板": #688xxx
            df_tmp = df_tmp[df_tmp['list_date_days_before'] > 365]  #科创板证券：上市时间超过一年。
        elif m == "创业板": #300xxx
            df_tmp = df_tmp[df_tmp['list_date_days_before'] > 365*3]  #创业板证券：上市时间超过三年
        elif m == "主板" or  m == "中小板" or m == 'CDR':  # 主板 000xxx, 600xxx,  中小板 002xxx, CDR 689
            df_tmp = df_tmp[df_tmp['list_date_days_before'] > 90]  #其他证券：上市时间超过一个季度，除非该证券自上市以来日均总市值排在前 30 位。。

        len_removed_for_a_mkt = len_df_tmp_ori - df_tmp.__len__()

        df_hs300_filted_on_market_days = df_hs300_filted_on_market_days.append(df_tmp)
        logging.info("appended df of "+m+", len removed "+ str(len_df_tmp_ori)+ " - "+str(df_tmp.__len__()) +" = "+str(len_removed_for_a_mkt))

    logging.info("after filted by HS300 on market days, df len is "+str(df_hs300_filted_on_market_days.__len__()))

    # apply amount filter
    df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays, debug=debug, df_parent=df_hs300_filted_on_market_days)
    df_hs300_filted_on_market_days_amt = finlib.Finlib()._df_sub_by_code(df=df_hs300_filted_on_market_days, df_sub=df_amt.tail(math.ceil(df_amt.__len__() / 2)),  byreason="daily average amount less than 50% stocks.")

    df_hs300_filted_on_market_days_amt_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=ndays, debug=debug,df_parent=df_hs300_filted_on_market_days_amt)

    # df_space = finlib.Finlib()._df_sub_by_code(df=df_all, df_sub=df_amt.tail(math.ceil(df_amt.__len__() / 2)))
    # my_hs300_amt_mktcap = finlib.Finlib()._df_sub_by_code(df=df_space, df_sub=df_amt.tail(math.ceil(df_amt.__len__() / 2)),  byreason="daily average amount less than 50% stocks.")

    my_hs300 = df_hs300_filted_on_market_days_amt_mktcap.head(300)[['code','name']]

    ####
    # my_hs300 = my_hs300_mktcap_amt
    # my_hs300 = my_hs300.head(300)[['code']]

    df_merged = my_hs300.merge(df_hs300, indicator=True, on='code', how='outer',suffixes=('','_x')).reset_index().drop('index', axis=1)
    len_merged = df_merged.__len__()
    df_merged.to_csv(hs300_candiate_csv, encoding='UTF-8', index=False)
    logging.info("saved " + hs300_candiate_csv + " len " + str(len_merged))

    df_both = df_merged[df_merged['_merge'] == "both"]
    len_both = df_both.__len__()
    logging.info("\n"+str(len_both) + " out of " + str(len_merged) + " in both myhs300 and officalhs300, they should will be keep in the hs300.")
    print(finlib.Finlib().pprint(df=df_both.head(2)))

    df_myonly = df_merged[df_merged['_merge'] == "left_only"].reset_index().drop('index', axis=1)
    df_myonly['new_candidate'] = True

    len_myonly = df_myonly.__len__()
    logging.info("\n"+str(len_myonly) + " out of " + str(len_merged) + " in my hs300, they possible will be added to hs300 next time")
    print(finlib.Finlib().pprint(df=df_myonly.head(2)))


    df_hs300only = df_merged[df_merged['_merge'] == "right_only"].reset_index().drop('index', axis=1)  # possible will be removed from hs300 index next time
    df_hs300only['to_be_removed'] = True
    len_hs300only = df_hs300only.__len__()
    logging.info("\n"+str(len_hs300only) + " out of " + str(
        len_merged) + " in offical hs300, they possible will be removed from hs300 next time")
    print(finlib.Finlib().pprint(df=df_hs300only.head(2)))

    logging.info("result saved to " + hs300_candiate_csv + " len " + str(len_merged))


    exit(0)
