# coding: utf-8

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
import finlib
import datetime
from optparse import OptionParser
import sys
import os
import logging
import signal
import akshare as ak
import tabulate
import finlib

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)


def fetch_ak(api, note, parms):

    base_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/akshare/source"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    csv_f = base_dir + "/" + api + "_" + note + ".csv"

    if finlib.Finlib().is_cached(csv_f, day=2):
        logging.info("file updated in 2 days, not fetch again. "+csv_f)
        return
    
    logging.info("\nfetching " + api + " " + note)

    try:
        cmd = "ak."+api+"("+parms+")"
        df = eval(cmd)
        if df is not None:
            df.to_csv(csv_f, encoding='UTF-8')
            logging.info("fetched "+api+" "+note+", "+csv_f)
            logging.info(tabulate.tabulate(df.head(1), headers='keys', tablefmt='psql'))
        else:
            logging.warning("df is None")
    except Exception as e:
        logging.warning("Exception on "+ api + ", "+str(e))



def get_individual_min():
    df = finlib.Finlib().get_A_stock_instrment()
    df = finlib.Finlib().add_market_to_code(df)
    df = finlib.Finlib().remove_garbage(df=df, code_field_name='code', code_format='C2D6', b_m_score=-1)

    finlib.Finlib().pprint(df[['name', 'code']])

    # print(tabulate.tabulate(pd.DataFrame(df['name'].value_counts()), headers='keys', tablefmt='psql'))
    # print(tabulate.tabulate(pd.DataFrame(df['code'].value_counts()), headers='keys', tablefmt='psql'))

    todayS_ymd = datetime.datetime.today().strftime('%Y%m%d')


    # save daily minutes start
    for i in range(df.__len__()):
        code = df.iloc[i]['code']
        name = df.iloc[i]['name']

        # 分时数据 单次返回指定股票或指数的指定频率的所有历史分时行情数据
        # @todo: verify if the data canbe realtime, e.g at 14:30 show 14:29 numbers
        # run after market closing.
        stock_zh_a_minute_df = ak.stock_zh_a_minute(symbol='sh600519', period='1')
        finlib.Finlib().pprint(stock_zh_a_minute_df)

        # 历史分笔数据 腾讯财经. 单次返回具体某个 A 上市公司的近 2 年历史分笔行情数据. 每个交易日 16:00 提供当日数据,
        stock_zh_a_tick_tx_df = ak.stock_zh_a_tick_tx(code="sh600519", trade_date="20201116")
        finlib.Finlib().pprint(stock_zh_a_tick_tx_df)

        # 历史分笔数据 网易财经 单次返回具体某个 A 上市公司的近 5 个交易日的历史分笔行情数据
        stock_zh_a_tick_163_df = ak.stock_zh_a_tick_163(code="sh600848", trade_date="20201116")
        finlib.Finlib().pprint(stock_zh_a_tick_163_df)

        # A股-CDR, 历史行情数据. 单次返回指定 CDR 的日频率数据, 分钟历史行情数据可以通过 stock_zh_a_minute 获取
        stock_zh_a_cdr_daily_df = ak.stock_zh_a_cdr_daily(symbol='sh600519')
        finlib.Finlib().pprint(stock_zh_a_cdr_daily_df)

        print(1)

    # save daily minutes ends



def wei_pan_la_sheng():
    ########################
    #
    #################
    b = "/home/ryan/DATA/pickle/Stock_Fundamental/akshare/source"
    # f = b + "/" + "stock_zh_a_scr_report_企业社会责任.csv"

    # f = b+"/"+"stock_em_jgdy_detail_机构调研-详细.csv"
    # f = b+"/"+"stock_em_gpzy_pledge_ratio_上市公司质押比例.csv"
    # f = b+"/"+"stock_institute_recommend_机构推荐池-股票综合评级.csv"
    # f = b+"/"+"stock_em_sy_list_个股商誉明细.csv"
    # f = b+"/"+"stock_em_sy_jz_list_个股商誉减值明细.csv"
    # f = b+"/"+""

    # f = b + "/" + "stock_em_jgdy_tj_机构调研-统计.csv"
    # df = finlib.Finlib().regular_read_akshare_to_stdard_df(data_csv=f, add_market=False)
    # df['NoticeDate'] = df['NoticeDate'].apply(lambda _d: str(_d).replace('-',''))
    # df['StartDate'] = df['StartDate'].apply(lambda _d: str(_d).replace('-',''))
    # df['EndDate'] = df['EndDate'].apply(lambda _d: str(_d).replace('-',''))

    nowS = datetime.datetime.now().strftime('%Y%m%d_%H%M')       #'20201117_2003'



    # 获取 A 股实时行情数据. 单次返回所有 A 股上市公司的实时行情数据
    # A 股数据是从新浪财经获取的数据, 重复运行本函数会被新浪暂时封 IP, 建议增加时间间隔
    # run at 14:55 (run_time). Find the stocks increase fastly since 14:00 or 14:30 to run_time.
    a_spot_csv = b + "/ag_spot_"+nowS+".csv"
    a_spot_csv_link = b + "/ag_spot_link.csv"
    a_spot_csv_link_old = b + "/ag_spot_link_old.csv"
    if finlib.Finlib().is_cached(file_path=a_spot_csv_link, day=1 / 24 / 60 * 1):  # cached in 5 minutes
        stock_zh_a_spot_df = pd.read_csv(a_spot_csv_link, encoding="utf-8")
        logging.info("loading ag spot df from " + a_spot_csv_link)
    else:
        stock_zh_a_spot_df = ak.stock_zh_a_spot().drop_duplicates()
        finlib.Finlib().pprint(stock_zh_a_spot_df.head(3))
        stock_zh_a_spot_df.to_csv(a_spot_csv, encoding='UTF-8', index=False)
        logging.info("ag spot saved to " + a_spot_csv)

        if os.path.lexists(a_spot_csv_link_old):
            os.unlink(a_spot_csv_link_old)
            logging.info("removed previous old link " + a_spot_csv_link_old)

        if os.path.lexists(a_spot_csv_link):
            os.rename(a_spot_csv_link, a_spot_csv_link_old)
            logging.info("renamed previous new link to old link, to " + a_spot_csv_link_old)


        os.symlink(a_spot_csv, a_spot_csv_link)
        logging.info(__file__ + ": " + "symbol link created  " + a_spot_csv_link + " -> " + a_spot_csv)

    # 获取科创板实时行情数据. 单次返回所有科创板上市公司的实时行情数据
    # 从新浪财经获取科创板股票数据

    ag_kcb_csv = b + "/ag_kcb_spot_"+nowS+".csv"
    ag_kcb_csv_link = b + "/ag_kcb_spot_link.csv"
    ag_kcb_csv_link_old = b + "/ag_kcb_spot_link_old.csv"
    if finlib.Finlib().is_cached(file_path=ag_kcb_csv_link, day=1 / 24 / 60 * 1):  # cached in 5 minutes
        stock_zh_kcb_spot_df = pd.read_csv(ag_kcb_csv_link, encoding="utf-8")
        logging.info("loading ag kcb df from " + ag_kcb_csv_link)

    else:
        stock_zh_kcb_spot_df = ak.stock_zh_kcb_spot().drop_duplicates()
        finlib.Finlib().pprint(stock_zh_kcb_spot_df.head(3))
        stock_zh_kcb_spot_df.to_csv(ag_kcb_csv, encoding='UTF-8', index=False)
        logging.info("ag spot saved to " + ag_kcb_csv)

        if os.path.lexists(ag_kcb_csv_link_old):
            os.unlink(ag_kcb_csv_link_old)
            logging.info("removed previous old link " + ag_kcb_csv_link_old)

        if os.path.lexists(ag_kcb_csv_link):
            os.rename(ag_kcb_csv_link, ag_kcb_csv_link_old)
            logging.info("renamed previous new link to old link, to " + ag_kcb_csv_link_old)


        os.symlink(ag_kcb_csv, ag_kcb_csv_link)
        logging.info(__file__ + ": " + "symbol link created  " + ag_kcb_csv_link + " -> " + ag_kcb_csv)

    # find stocks increased at the end of the market time
    # dfmt = stock_zh_a_spot_df[stock_zh_a_spot_df['symbol'] == 'sh600519']
    # finlib.Finlib().pprint(dfmt)
    # +-----+----------+--------+----------+---------+---------------+-----------------+---------+--------+--------------+--------+---------+-------+------------+-------------+------------+--------+--------+------------+------------+-----------------+
    # | | symbol | code | name | trade | pricechange | changepercent | buy | sell | settlement | open | high | low | volume | amount | ticktime | per | pb | mktcap | nmc | turnoverratio |
    # | -----+----------+--------+----------+---------+---------------+-----------------+---------+--------+--------------+--------+---------+-------+------------+-------------+------------+--------+--------+------------+------------+----------------- |
    # | 415 | sh600519 | 600519 | 贵州茅台 | 1729.9 | -0.15 | -0.009 | 1728.01 | 1729.9 | 1730.05 | 1740 | 1742.35 | 1722 | 1.4981e+06 | 2.59765e+09 | 11: 30:00 | 52.741 | 14.638 | 2.1731e+08 | 2.1731e+08 | 0.11926 |
    # +-----+----------+--------+----------+---------+---------------+-----------------+---------+--------+--------------+--------+---------+-------+------------+-------------+------------+--------+--------+------------+------------+-----------------+
    #
    # settlement: 昨收
    # trade: 现价
    # open: 开
    # pricechange: trade - settlement
    # changepercent = pricechange / settlement * 100
    if finlib.Finlib().is_cached(a_spot_csv_link_old, day=1):
        old_df = pd.read_csv(a_spot_csv_link_old, encoding="utf-8")
        old_df_small_change = old_df[old_df['changepercent'] < 1]  # changes smaller than 1%
        logging.info(" number of small change df in old " + str(old_df_small_change.__len__()) + ", data at " +
                     old_df_small_change['ticktime'].iloc[0])

        # new_df = stock_zh_a_spot_df
        new_df = pd.read_csv(a_spot_csv_link, encoding="utf-8")
        new_df_large_change = new_df[new_df['changepercent'] > -30]
        logging.info(" number of large change df in new " + str(new_df_large_change.__len__()) + ", data at " +
                     new_df_large_change['ticktime'].iloc[0])

        merged_inner = pd.merge(left=old_df_small_change, right=new_df_large_change, how='inner', left_on='symbol',
                                right_on='symbol',
                                suffixes=('_o', '')).drop('name_o', axis=1)
        merged_inner['increase_diff'] = merged_inner['changepercent'] - merged_inner['changepercent_o']

        merged_inner_rst = merged_inner[
            ['symbol', 'code', 'name', 'trade', 'increase_diff', 'volume', 'amount', 'per', 'pb', 'mktcap', 'nmc',
             'turnoverratio', 'ticktime_o', 'changepercent_o', 'ticktime', 'changepercent']]
        merged_inner_rst = merged_inner_rst.sort_values(by=['increase_diff'], ascending=[False], inplace=False)
        logging.info("The top10 Rapid Change Stock List:")
        finlib.Finlib().pprint(merged_inner_rst.head(10))

        merged_inner_rst.to_csv(b+'/wei_pan_la_sheng_'+nowS+'.csv', encoding='UTF-8', index=False)
        logging.info("ag wei_pan_la_sheng output list saved"  )

### kcb
    if finlib.Finlib().is_cached(ag_kcb_csv_link_old, day=1):
        old_df = pd.read_csv(ag_kcb_csv_link_old, encoding="utf-8")
        old_df_small_change = old_df[old_df['changepercent'] < 1]  # changes smaller than 1%
        logging.info(" number of small change df in old " + str(old_df_small_change.__len__()) + ", data at " +
                     old_df_small_change['ticktime'].iloc[0])

        new_df = pd.read_csv(ag_kcb_csv_link, encoding="utf-8")
        new_df_large_change = new_df[new_df['changepercent'] > -30]
        logging.info(" number of large change df in new " + str(new_df_large_change.__len__()) + ", data at " +
                     new_df_large_change['ticktime'].iloc[0])

        merged_inner = pd.merge(left=old_df_small_change, right=new_df_large_change, how='inner', left_on='symbol',
                                right_on='symbol',
                                suffixes=('_o', '')).drop('name_o', axis=1)
        merged_inner['increase_diff'] = merged_inner['changepercent'] - merged_inner['changepercent_o']

        merged_inner_rst = merged_inner[
            ['symbol', 'code', 'name', 'trade', 'increase_diff', 'volume', 'amount', 'per', 'pb', 'mktcap', 'nmc',
             'turnoverratio', 'ticktime_o', 'changepercent_o', 'ticktime', 'changepercent']]
        merged_inner_rst = merged_inner_rst.sort_values(by=['increase_diff'], ascending=[False], inplace=False)
        logging.info("KCB top 10 Rapid Change Stock List:")
        finlib.Finlib().pprint(merged_inner_rst.head(10))
        merged_inner_rst.to_csv(b+'/wei_pan_la_sheng_kcb_'+nowS+'.csv', encoding='UTF-8', index=False)
        logging.info("ag kcb wei_pan_la_sheng output list saved" )





def fetch_after_market_close():
    todayS_ymd = datetime.datetime.today().strftime('%Y%m%d')

    if finlib.Finlib().is_a_trading_day_ag(dateS=todayS_ymd) and finlib.Finlib().is_market_open_ag():
        logging.info("AG market is open, please capture after market closing.")
        return ()


    # maket is closed now


    # stock_szse_summary_df = ak.stock_szse_summary(date=todayS_ymd)
    # finlib.Finlib().pprint(stock_szse_summary_df)
    fetch_ak(api='stock_szse_summary', note='深圳证券交易所市场总貌', parms="date='"+todayS_ymd+"'")

    # stock_sse_summary_df = ak.stock_sse_summary()
    # finlib.Finlib().pprint(stock_sse_summary_df)
    fetch_ak(api='stock_sse_summary', note='上海证券交易所市场总貌', parms='')

    # 企业社会责任
    fetch_ak(api='stock_zh_a_scr_report', note='企业社会责任', parms='report_year=2019,page=1')

    # 机构调研
    fetch_ak(api='stock_em_jgdy_tj', note='机构调研-统计', parms='')
    fetch_ak(api='stock_em_jgdy_detail', note='机构调研-详细', parms='')

    # 股票质押
    fetch_ak(api='stock_em_gpzy_profile', note='股权质押市场概况', parms='')
    fetch_ak(api='stock_em_gpzy_pledge_ratio', note='上市公司质押比例', parms='')
    fetch_ak(api='stock_em_gpzy_pledge_ratio_detail', note='重要股东股权质押明细', parms='')
    fetch_ak(api='stock_em_gpzy_distribute_statistics_company', note='质押机构分布统计证券公司', parms='')
    fetch_ak(api='stock_em_gpzy_distribute_statistics_bank', note='质押机构分布统计银行', parms='')
    fetch_ak(api='stock_em_gpzy_industry_data', note='上市公司质押比例', parms='')

    # 商誉专题
    fetch_ak(api='stock_em_sy_profile', note='A股商誉市场概况', parms='')
    fetch_ak(api='stock_em_sy_yq_list', note='商誉减值预期明细', parms='')
    fetch_ak(api='stock_em_sy_jz_list', note='个股商誉减值明细', parms='symbol="沪市主板", trade_date="2019-12-31"')
    fetch_ak(api='stock_em_sy_list', note='个股商誉明细', parms='symbol="沪市主板", trade_date="2019-12-31"')
    fetch_ak(api='stock_em_sy_hy_list', note='行业商誉明细', parms='trade_date="2019-12-31"')

    # 分析师指数
    fetch_ak(api='stock_em_analyst_rank', note='分析师指数最新排行', parms='')

    fetch_ak(api='stock_em_comment', note='千股千评', parms='')

    # 沪深港通持股
    fetch_ak(api='stock_em_hsgt_north_net_flow_in', note='北向净流入', parms='indicator="北上"')
    fetch_ak(api='stock_em_hsgt_north_cash', note='北向资金余额', parms='indicator="北上"')
    fetch_ak(api='stock_em_hsgt_north_acc_flow_in', note='北向累计净流入', parms='indicator="北上"')
    fetch_ak(api='stock_em_hsgt_south_net_flow_in', note='南向净流入', parms='indicator="南下"')
    fetch_ak(api='stock_em_hsgt_south_cash', note='南向资金余额', parms='indicator="南下"')
    fetch_ak(api='stock_em_hsgt_south_acc_flow_in', note='南向累计净流入', parms='indicator="南下"')
    fetch_ak(api='stock_em_hsgt_hold_stock', note='个股排行', parms='market="北向", indicator="3日排行"')
    fetch_ak(api='stock_em_hsgt_stock_statistics', note='每日个股统计', parms='market="北向持股"')

    # 资金流向
    fetch_ak(api='stock_individual_fund_flow_rank', note='个股资金流排名', parms='indicator="3日"')
    fetch_ak(api='stock_market_fund_flow', note='大盘资金流', parms='')
    fetch_ak(api='stock_sector_fund_flow_rank', note='板块资金流排名', parms='indicator="5日", sector_type="概念资金流"')

    fetch_ak(api='stock_history_dividend', note='历史分红', parms='')
    fetch_ak(api='stock_sector_spot', note='板块行情', parms='indicator="概念"')
    fetch_ak(api='stock_info_a_code_name', note='股票列表-A股', parms='')
    fetch_ak(api='stock_info_sh_name_code', note='股票列表-上证-主板A股', parms='indicator="主板A股"')
    fetch_ak(api='stock_info_sh_name_code', note='股票列表-上证-主板B股', parms='indicator="主板B股"')
    fetch_ak(api='stock_info_sh_name_code', note='股票列表-上证-科创板', parms='indicator="科创板"')

    fetch_ak(api='stock_info_sz_name_code', note='股票列表-深证-A股列表', parms='indicator="A股列表"')
    fetch_ak(api='stock_info_sz_name_code', note='股票列表-深证-B股列表', parms='indicator="B股列表"')
    fetch_ak(api='stock_info_sz_name_code', note='股票列表-深证-中小企业板', parms='indicator="中小企业板"')
    fetch_ak(api='stock_info_sz_name_code', note='股票列表-深证-创业板', parms='indicator="创业板"')
    fetch_ak(api='stock_info_sz_name_code', note='股票列表-深证-上市公司列表', parms='indicator="上市公司列表"')

    fetch_ak(api='stock_institute_hold', note='机构持股一览表', parms='quarter="20203"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-行业关注度', parms='indicator="行业关注度"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-机构关注度', parms='indicator="机构关注度"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-最新投资评级', parms='indicator="最新投资评级"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-上调评级股票', parms='indicator="上调评级股票"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-下调评级股票', parms='indicator="下调评级股票"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-股票综合评级', parms='indicator="股票综合评级"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-首次评级股票', parms='indicator="首次评级股票"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-目标涨幅排名', parms='indicator="目标涨幅排名"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-投资评级选股', parms='indicator="投资评级选股"')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-', parms='indicator=""')
    fetch_ak(api='stock_institute_recommend', note='机构推荐池-', parms='indicator=""')

    fetch_ak(api='stock_js_price', note='美港目标价', parms='category="us"')
    fetch_ak(api='stock_js_price', note='美港目标价', parms='category="hk"')
    fetch_ak(api='stock_em_qsjy', note='券商业绩月报-202006', parms='trade_date="2020-06-01"')
    fetch_ak(api='stock_em_qsjy', note='券商业绩月报-202007', parms='trade_date="2020-07-01"')

    fetch_ak(api='stock_a_high_low_statistics', note='创新高和新低的股票数量', parms='market="all"')
    fetch_ak(api='stock_a_below_net_asset_statistics', note='破净股统计', parms='')

    fetch_ak(api='stock_report_fund_hold', note='基金持股', parms='symbol="基金持仓", date="20200630"')
    fetch_ak(api='stock_sina_lhb_detail_daily', note='龙虎榜-每日详情', parms='trade_date="20200730", symbol="涨幅偏离值达7%的证券"')
    fetch_ak(api='stock_sina_lhb_ggtj', note='龙虎榜-个股上榜统计', parms='recent_day="5"')
    fetch_ak(api='stock_sina_lhb_yytj', note='龙虎榜-营业上榜统计', parms='recent_day="5"')
    fetch_ak(api='stock_sina_lhb_jgzz', note='龙虎榜-机构席位追踪', parms='recent_day="5"')
    fetch_ak(api='stock_sina_lhb_jgmx', note='龙虎榜-机构席位成交明细', parms='')


def main():
    parser = OptionParser()

    parser.add_option("-f","--fetch_after_market", action="store_true", dest="fetch_after_market", default=False,
                      help="fetch market data after market closure.")

    parser.add_option("-i","--wei_pan_la_sheng", action="store_true", dest="wei_pan_la_sheng", default=False,
                      help="get stocks price roaring. Need run twice then get the comparing increase")

    (options, args) = parser.parse_args()


    #find the rapid up stocks in market. Comparing the increase percent with the number got in previous run.
    #set up crontabs, run at 2:00, and 2:45.  Check the output of 2.45 run.
    if options.wei_pan_la_sheng:
        wei_pan_la_sheng()
    if options.fetch_after_market:
        fetch_after_market_close()

    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
