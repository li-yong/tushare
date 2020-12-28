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
from optparse import OptionParser
import sys


##############

def compare_with_official_index_list(df_my_index,df_offical_index, index_name):

    df_merged = pd.merge(df_my_index,df_offical_index, indicator=True, on='code', how='outer',suffixes=('','_x')).reset_index().drop('index', axis=1)

    #replace with name_x if name is None
    df_merged.loc[df_merged['name'].isna(), ['name']] = df_merged['name_x']
    df_merged.loc[df_merged['total_mv_perc'].isna(), ['total_mv_perc']] = df_merged['total_mv_perc_x']
    df_merged.loc[df_merged['amount_perc'].isna(), ['amount_perc']] = df_merged['amount_perc_x']
    df_merged.loc[df_merged['list_date_days_before'].isna(), ['list_date_days_before']] = df_merged['list_date_days_before_x']
    df_merged = df_merged.drop('name_x', axis=1)
    df_merged = df_merged.drop('total_mv_perc_x', axis=1)
    df_merged = df_merged.drop('amount_perc_x', axis=1)
    df_merged = df_merged.drop('list_date_days_before_x', axis=1)

    df_merged = df_merged.drop('date', axis=1)
    df_merged = df_merged.drop('list_status_x', axis=1)
    df_merged = df_merged.drop('list_status', axis=1)
    df_merged = df_merged.drop('amount', axis=1)
    df_merged = df_merged.drop('total_mv', axis=1)

    df_merged['predict'] = None
    df_merged.loc[df_merged['_merge'] == 'both', ['predict']] = 'To_Be_Kept'
    df_merged.loc[df_merged['_merge'] == 'right_only', ['predict']] = 'To_Be_Removed'
    df_merged.loc[df_merged['_merge'] == 'left_only', ['predict']] = 'To_Be_Added'
    df_merged = df_merged.drop('_merge', axis=1)

    len_merged = df_merged.__len__()
    index_candiate_csv = "/home/ryan/DATA/result/"+index_name+"_candidate_list.csv"

    df_merged.to_csv(index_candiate_csv, encoding='UTF-8', index=False)
    logging.info("saved " + index_candiate_csv + " len " + str(len_merged))

    df_both = df_merged[df_merged['predict'] == "To_Be_Kept"]
    df_both = df_both.sort_values(by="total_mv_perc", ascending=False, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_both.__len__()) + " out of " + str(len_merged) + " in both myhs300 and officalhs300, they should will be kept in the "+index_name)
    logging.info(finlib.Finlib().pprint(df=df_both.head(2)))


    df_offlical_only = df_merged[df_merged['predict'] == "To_Be_Removed"].reset_index().drop('index', axis=1)  # possible will be removed from hs300 index next time
    df_offlical_only = df_offlical_only.sort_values(by="total_mv_perc", ascending=True, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_offlical_only.__len__()) + " out of " + str(
        len_merged) + " in offical list, period_end "+ period_end +", period days "+ str(ndays) +". They possible will be removed from "+index_name+" next time. Top 32")
    logging.info(finlib.Finlib().pprint(df=df_offlical_only.head(32)))

    df_myonly = df_merged[df_merged['predict'] == "To_Be_Added"].reset_index().drop('index', axis=1)
    df_myonly = df_myonly.sort_values(by="total_mv_perc", ascending=False, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_myonly.__len__()) + " out of " + str(len_merged) + " in my list, period_end "+ period_end +", period days "+ str(ndays) +". They possible will be added to "+index_name+" next time. Top 32")
    #print(finlib.Finlib().pprint(df=df_myonly.head(32)))
    logging.info(finlib.Finlib().pprint(df=df_myonly.head(32)))

    logging.info("result saved to " + index_candiate_csv + " len " + str(len_merged))


def hs300_on_market_days_filter():
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
    return(df_hs300_filted_on_market_days[['code','name','list_status','list_date_days_before']])


def get_hs300_total_share_weighted():
    basic_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily"
    df_basic = pd.read_csv(basic_dir + "/basic_" + finlib.Finlib().get_last_trading_day() + ".csv")
    df_basic['free_ration'] = round(df_basic['free_share']*100.0/df_basic['total_share'], 4)

    df_basic['weight_calc'] = None
    df_basic.loc[df_basic['free_ration']<=15, ['weight_calc']] = df_basic['free_ration'].apply(lambda _d: math.ceil(_d) )
    df_basic.loc[(df_basic['free_ration']>15) & (df_basic['free_ration']<=20) , ['weight_calc']] = 20
    df_basic.loc[(df_basic['free_ration']>20) & (df_basic['free_ration']<=30) , ['weight_calc']] = 30
    df_basic.loc[(df_basic['free_ration']>30) & (df_basic['free_ration']<=40) , ['weight_calc']] = 40
    df_basic.loc[(df_basic['free_ration']>40) & (df_basic['free_ration']<=50) , ['weight_calc']] = 50
    df_basic.loc[(df_basic['free_ration']>50) & (df_basic['free_ration']<=60) , ['weight_calc']] = 60
    df_basic.loc[(df_basic['free_ration']>60) & (df_basic['free_ration']<=70) , ['weight_calc']] = 70
    df_basic.loc[(df_basic['free_ration']>70) & (df_basic['free_ration']<=80) , ['weight_calc']] = 80
    df_basic.loc[(df_basic['free_ration']>80), ['weight_calc']] = 100

    df_basic['hs300_total_share_weighted']=df_basic['total_share']*df_basic['weight_calc']*0.01
    df_basic = finlib.Finlib().ts_code_to_code(df=df_basic)
    df_basic = finlib.Finlib().add_stock_name_to_df(df=df_basic)

    return(df_basic[['code','name','hs300_total_share_weighted']])


### MAIN ####
if __name__ == '__main__':


    ndays = 365

    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)
    # logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()
    parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="debug, only check 1st 10 stocks in the list")
    parser.add_option("-f", "--force_run", action="store_true", default=False, dest="force_run", help="always check, regardless output updated in 3 days.")

    parser.add_option("-e", "--period_end", dest="period_end", help="the END date of checking scope. default is last trading day. fmt yyyymmdd. yyyy0430, yyyy1031")
    parser.add_option("-n", "--ndays",default=365, dest="ndays",type="int", help="N days before the period_end. Use to define the start of checking period. HS300:365 Days, SZCZ:183 Days")
    parser.add_option("-i", "--index_name",default="hs300", dest="index_name",type="str", help="index name. [hs300|zz100|zz500|szcz]")
    parser.add_option("-g", "--get_hs300", action="store_true", default=False, dest="get_hs300",  help="get hs300 index list, saved to ")


    (options, args) = parser.parse_args()
    debug = options.debug
    force_run = options.force_run
    ndays = options.ndays
    index_name = options.index_name
    period_end = options.period_end
    get_hs300 = options.get_hs300

    idict = {
        'zz100':'000903.SH',
        'zz200':'000904.SH',
        'zz500':'000905.SH',
        'hs300':'000300.SH',
    }

    if get_hs300:
        finlib.Finlib().load_hs300(index_code=idict[index_name],index_name=index_name, force_run=force_run)
        exit()


    if period_end is None:
        period_end = finlib.Finlib().get_last_trading_day()
        period_end = datetime.datetime.today().strftime("%Y%m%d")




    #df_amt len 4117, df_mktcap len 4144, df_total_share_weighted len 4104
    df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays,period_end=period_end, debug=debug,force_run=force_run) #output  /home/ryan/DATA/result/average_daily_amount_sorted.csv
    df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=ndays,period_end=period_end, debug=debug,force_run=force_run) #output: /home/ryan/DATA/result/average_daily_mktcap_sorted.csv
    df_total_share_weighted = get_hs300_total_share_weighted()
    df_amt_mktcap = pd.merge(df_amt[['code','name', 'amount','amount_perc']],df_mktcap[['code','name', 'total_mv','total_mv_perc']], on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)
    df_amt_mktcap_weight = pd.merge(df_amt_mktcap,df_total_share_weighted, on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)


    #filter by (on_market_date len 3744) and (top 50% daily_amount)
    my_hs300 = pd.merge(df_amt_mktcap_weight[df_amt_mktcap_weight['amount_perc']>=0.5],hs300_on_market_days_filter(),on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)
    my_hs300 = my_hs300.sort_values(by=['total_mv'], ascending=[False]).head(300).reset_index().drop('index', axis=1)
    my_hs300['my_index_weight'] = round(my_hs300['hs300_total_share_weighted']*100.0 /my_hs300['hs300_total_share_weighted'].sum(), 2)
    my_hs300 = my_hs300.drop('hs300_total_share_weighted', axis=1)

    ####
    df_offical_index = finlib.Finlib().load_index(index_code=idict[index_name], index_name=index_name)
    df_offical_index = pd.merge(df_offical_index,df_amt_mktcap[['code','total_mv_perc','amount_perc']],on='code', how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)
    df_offical_index = pd.merge(df_offical_index,hs300_on_market_days_filter(),on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)

    compare_with_official_index_list(df_my_index=my_hs300, df_offical_index=df_offical_index, index_name=index_name)


    exit(0)
