# coding: utf-8

import sys, traceback, threading
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
import finlib_indicator
import tushare as ts

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)


def fetch_ak(api, note, parms, cache_day=1, force_fetch=False):

    base_dir = "/home/ryan/DATA/pickle/Stock_Fundamental/akshare/source"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    csv_f = base_dir + "/" + api + "_" + note + ".csv"

    if finlib.Finlib().is_cached(csv_f, day=cache_day, use_last_trade_day=False) and (not force_fetch):
        logging.info(f"file updated in {str(cache_day)} days, not fetch again. " + csv_f)
        return(pd.read_csv(csv_f))

    logging.info("\nfetching " + api + " " + note)

    try:
        cmd = "ak." + api + "(" + parms + ")"
        df = eval(cmd)
        if df is not None:
            df.to_csv(csv_f, encoding='UTF-8',index=False)
            logging.info("fetched " + api + " " + note + ", " + csv_f)
            logging.info(tabulate.tabulate(df.head(1), headers='keys', tablefmt='psql'))
            return(df)
        else:
            logging.warning("df is None")
    except Exception as e:
        logging.warning("Exception on " + api + ", " + str(e))


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


def wei_pan_la_sheng(stock_market='AG'):

    finlib.Finlib().get_ak_live_price(stock_market='AG')  #ryan debug commented

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

    stock_market = stock_market.upper()
    b = "/home/ryan/DATA/result/wei_pan_la_sheng"

    if not os.path.isdir(b):
        os.mkdir(b)

    nowS = datetime.datetime.now().strftime('%Y%m%d_%H%M')  # '20201117_2003'

    a_spot_csv_link = b + "/" + stock_market + "_spot_link.csv"
    a_spot_csv_link_old = b + "/" + stock_market + "_spot_link_old.csv"

    result_csv = b + "/"+stock_market+'_wei_pan_la_sheng_' + nowS + '.csv'
    result_csv_link = b+"/"+ stock_market + "_latest_result.csv"


    if finlib.Finlib().is_cached(a_spot_csv_link_old, day=1) or True:
    # if finlib.Finlib().is_cached(a_spot_csv_link_old, day=1):
        old_df = pd.read_csv(a_spot_csv_link_old, encoding="utf-8")
        old_df_small_change = old_df
        # old_df_small_change = old_df[old_df['changepercent'] < 1]  # changes smaller than 1%
        logging.info("number of small change df in old " + str(old_df_small_change.__len__()))

        # new_df = stock_zh_a_spot_df
        new_df = pd.read_csv(a_spot_csv_link, encoding="utf-8")
        new_df_large_change = new_df
        # new_df_large_change = new_df[new_df['changepercent'] > 0.3] # all new_df, as we sort the delta increase later.
        logging.info("number of large change df in new " + str(new_df_large_change.__len__()))

        merged_inner = pd.merge(left=old_df_small_change, right=new_df_large_change, how='inner', left_on='code', right_on='code', suffixes=('_o', '')).drop('name_o', axis=1)
        merged_inner['increase_diff'] = round(merged_inner['changepercent'] - merged_inner['changepercent_o'],1)

        tmp = merged_inner

        # df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
        # df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())
        # tmp = finlib.Finlib().add_amount_mktcap(df=merged_inner)
        # tmp = finlib.Finlib().add_tr_pe(df=tmp, df_daily=df_daily, df_ts_all=df_ts_all)
        # tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')

        merged_inner_rst = tmp[['code', 'name', 'close', 'increase_diff','volume', 'amount', 'changepercent_o',  'changepercent']]
        # merged_inner_rst = tmp[['code', 'name', 'close', 'increase_diff','tr_pe','total_mv', 'volume', 'amount', 'changepercent_o',  'changepercent']]
        merged_inner_rst = merged_inner_rst.drop_duplicates()
        merged_inner_rst = merged_inner_rst.sort_values(by=['increase_diff','changepercent'], ascending=[False,False], inplace=False).reset_index().drop('index', axis=1)

        merged_inner_rst = finlib.Finlib().add_market_to_code(merged_inner_rst).head(300)
        logging.info("The top10 Rapid Change Stock List:")
        print(finlib.Finlib().pprint(merged_inner_rst.head(10)))

        merged_inner_rst.to_csv(result_csv, encoding='UTF-8', index=False)
        logging.info(f"ag wei_pan_la_sheng output list saved to {result_csv}, len {str(merged_inner_rst.__len__())}" )

        #update symbol link
        if os.path.lexists(result_csv_link):
            os.unlink(result_csv_link)

        os.symlink(result_csv, result_csv_link)
        logging.info(__file__ + ": " + "symbol link created  " + result_csv_link + " -> " + result_csv)


def fetch_after_market_close():
    todayS_ymd = datetime.datetime.today().strftime('%Y%m%d')

    if finlib.Finlib().is_a_trading_day_ag(dateS=todayS_ymd) and finlib.Finlib().is_market_open_ag():
        logging.info("AG market is open, please capture after market closing.")
        return ()

    # maket is closed now

    # stock_szse_summary_df = ak.stock_szse_summary(date=todayS_ymd)
    # finlib.Finlib().pprint(stock_szse_summary_df)
    fetch_ak(api='stock_szse_summary', note='深圳证券交易所市场总貌', parms="date='" + todayS_ymd + "'")

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



def fetch_em_concept():
    cache_days = 1
    base_bar_dir = '/home/ryan/DATA/DAY_Global/AG_concept_bars'
    base_concept_dir = '/home/ryan/DATA/DAY_Global/AG_concept'

    concept_consist_csv = base_concept_dir+"/em_concept_consist.csv"
    concept_consist_df = pd.DataFrame()

    if not os.path.exists(base_bar_dir):
        os.makedirs(base_bar_dir)
    if not os.path.exists(base_concept_dir):
        os.makedirs(base_concept_dir)

    if not os.path.exists(base_bar_dir+"/EM"):
        os.makedirs(base_bar_dir+"/EM")
    if not os.path.exists(base_concept_dir+"/EM"):
        os.makedirs(base_concept_dir+"/EM")

    ''' len 373
          排名    板块名称    板块代码     最新价    涨跌额   涨跌幅            总市值  换手率  上涨家数  下跌家数  领涨股票  领涨股票-涨跌幅
    0      1    刀片电池  BK0951 3220.22 154.10  5.03  1261148896000 6.81     9     0  常铝股份      9.97
    1      2     有机硅  BK0961 1876.91  74.00  4.10   541315328000 5.05    25     1  集泰股份     10.04
    '''
    concept_list_f = f"{base_concept_dir}/em_concept.csv"

    if finlib.Finlib().is_cached(concept_list_f, day=cache_days, use_last_trade_day=False):
        em_concepts = pd.read_csv(concept_list_f)
        logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + concept_list_f)
    else:
        em_concepts = ak.stock_board_concept_name_em()
        em_concepts.to_csv(concept_list_f, encoding='UTF-8', index=False)
        logging.info(f"fetched {concept_list_f}, len {str(em_concepts.__len__())}")

    # logging.info("")
    # t = em_concepts.groupby(by="领涨股票")['领涨股票'].count().sort_values()

    # em_concepts[em_concepts['板块名称'].str.contains('国')]
    # a = em_concepts.head(100)
    # b = em_concepts.sort_values(by='总市值', ascending=False).head(100).reset_index().drop('index', axis=1)
    # print(finlib.Finlib().pprint(b))
    # c = a.append(b).reset_index().drop('index', axis=1)

    # for name in c['板块名称']:
    for name in em_concepts['板块名称']:
        name_path = name.replace("/", "_")
        name_path = name_path.replace(" ", "_")
        print(name)

        '''
        Fetch BARS
        '''

        ''' len 430
                     日期      开盘      收盘      最高      最低   涨跌幅    涨跌额      成交量            成交额   振幅  换手率
        0    2020-09-03  999.63  964.52 1001.50  960.74 -3.55 -35.48  1485636  5091721216.00 4.08 2.47
        1    2020-09-04  934.28  963.54  964.92  927.42 -0.10  -0.98  1155607  3853309952.00 3.89 1.92
        '''
        csv_f = f"{base_bar_dir}/EM/{name_path}.csv"
        if finlib.Finlib().is_cached(csv_f, day=cache_days, use_last_trade_day=False):
            logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + csv_f)
        else:
            e = ak.stock_board_concept_hist_em(symbol=name, adjust="")
            e['concept'] = name_path
            e = e.rename(columns={'日期':'date', '开盘': 'open', '收盘':'close', '最高':'high','最低':'low',
                                  '涨跌幅':'pct_change', '涨跌额': 'change', '成交量': 'vol', '成交额': 'amount',
                                  '振幅': 'swing','换手率':'turnover_rate',
                                  })

            e.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info(f"fetched bar of {csv_f}, len {str(e.__len__())}")

        '''
        Fetch Concept compositives
        '''
        ''' len 9
        序号      代码    名称    最新价   涨跌幅  涨跌额      成交量           成交额    振幅     最高     最低     今开     昨收   换手率  市盈率-动态   市净率
        0   1  002824  和胜股份  57.30 10.00 5.21   104467  575253744.00 13.30  57.30  50.37  51.50  52.09  8.02   52.48  9.56
        1   2  002160  常铝股份   6.40  9.97 0.58  2067917 1286699120.00 13.57   6.40   5.61   5.68   5.82 27.17   53.56  1.60
        '''
        csv_f = f"{base_concept_dir}/EM/{name_path}.csv"
        if finlib.Finlib().is_cached(csv_f, day=cache_days, use_last_trade_day=False):
            logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + csv_f)
            df = pd.read_csv(csv_f)
            df['concept']=name_path
            # concept_consist_df = concept_consist_df.append(df)
            concept_consist_df = pd.concat([concept_consist_df,df])
        else:
            e = ak.stock_board_concept_cons_em(symbol=name)
            e['concept'] = name_path
            e = e.rename(columns={'序号':'rank', '代码': 'code', '名称':'name', '最新价':'close',
                                  '涨跌幅':'pct_change','涨跌额':'change','成交量':'vol','成交额':'amount','振幅':'swing',
                                  '最高':'high','最低':'low','今开':'open','昨收':'pre_close',
                                  '换手率':'turnover_rate','市盈率-动态':'pe_ttm','市净率':'pb',
                                  })
            e['code'] = e['code'].astype('str')
            e = finlib.Finlib().add_market_to_code(e)
            e.to_csv(csv_f, encoding='UTF-8', index=False)

            logging.info(f"fetched concept compsitives of {csv_f}, len {str(e.__len__())}")

            # concept_consist_df = concept_consist_df.append(e)
            concept_consist_df = pd.concat([concept_consist_df,e])


    concept_consist_df.to_csv(concept_consist_csv, encoding='UTF-8', index=False)
    logging.info(f"all EM concept consist {concept_consist_csv}, len {str(concept_consist_df.__len__())}")


def fetch_ths_concept():
    cache_days = 1

    base_bar_dir = '/home/ryan/DATA/DAY_Global/AG_concept_bars'
    base_concept_dir = '/home/ryan/DATA/DAY_Global/AG_concept'

    concept_consist_csv = base_concept_dir+"/ths_concept_consist.csv"
    concept_consist_df = pd.DataFrame()

    if not os.path.exists(base_bar_dir):
        os.makedirs(base_bar_dir)
    if not os.path.exists(base_concept_dir):
        os.makedirs(base_concept_dir)

    if not os.path.exists(base_bar_dir+"/THS"):
        os.makedirs(base_bar_dir+"/THS")
    if not os.path.exists(base_concept_dir+"/THS"):
        os.makedirs(base_concept_dir+"/THS")

    ''' len 482
    日期    概念名称 成分股数量                                             网址      代码
    0    2022-06-13   F5G概念    21  http://q.10jqka.com.cn/gn/detail/code/308977/  308977
    1    2022-06-08   比亚迪概念   216  http://q.10jqka.com.cn/gn/detail/code/308972/  308972
    '''
    concept_list_f = f"{base_concept_dir}/ths_concept.csv"

    if finlib.Finlib().is_cached(concept_list_f, day=cache_days, use_last_trade_day=False):
        ths_concepts = pd.read_csv(concept_list_f)
        logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + concept_list_f)
    else:
        ths_concepts = ak.stock_board_concept_name_ths()
        ths_concepts.to_csv(concept_list_f, encoding='UTF-8', index=False)
        logging.info(f"fetched {concept_list_f}, len {str(ths_concepts.__len__())}")

    # logging.info("")
    # t = em_concepts.groupby(by="领涨股票")['领涨股票'].count().sort_values()

    # em_concepts[em_concepts['板块名称'].str.contains('国')]
    # a = em_concepts.head(100)
    # b = em_concepts.sort_values(by='总市值', ascending=False).head(100).reset_index().drop('index', axis=1)
    # print(finlib.Finlib().pprint(b))
    # c = a.append(b).reset_index().drop('index', axis=1)

    for name in ths_concepts['概念名称']:
        name_path = name.replace("/", "_")
        name_path = name_path.replace(" ", "_")
        print(name)

        '''
        Fetch BARS
        '''
        '''
        日期,开盘价,最高价,最低价,收盘价,成交量,成交额
        2022-06-14,988.806,1005.655,969.054,1005.365,379512920,4411749300.0
        '''
        csv_f = f"{base_bar_dir}/THS/{name_path}.csv"
        if finlib.Finlib().is_cached(csv_f, day=cache_days, use_last_trade_day=False):
            logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + csv_f)
        else:
            try:
                e = ak.stock_board_concept_hist_ths(symbol=name,start_year='2021')

                e = e.rename(columns={'日期': 'date', '开盘价': 'open', '最高价': 'high',
                                      '最低价': 'low', '收盘价': 'close',
                                      '成交量': 'vol', '成交额': 'amount',
                                      })

                e['concept'] = name_path


                e.to_csv(csv_f, encoding='UTF-8', index=False)
                logging.info(f"fetched bar of {csv_f}, len {str(e.__len__())}")
            except:
                logging.fatal("exception stock_board_concept_hist_ths")

        '''
        Fetch Concept compositives
        '''
        '''
          序号      代码    名称    现价   涨跌幅    涨跌    涨速    换手   量比    振幅     成交额     流通股      流通市值      市盈率
        0      1  300916  朗特智能 54.92 14.75  7.06  0.37 18.57 3.58 19.75   1.70亿   0.17亿     9.54亿    43.90
        '''

        csv_f = f"{base_concept_dir}/THS/{name_path}.csv"
        if finlib.Finlib().is_cached(csv_f, day=cache_days, use_last_trade_day=False):
            df = pd.read_csv(csv_f)
            df['concept'] = name_path
            # concept_consist_df = concept_consist_df.append(df)
            concept_consist_df = pd.concat([concept_consist_df,df])
            logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + csv_f)
        else:
            try:
                e = ak.stock_board_concept_cons_ths(symbol=name)
                e['concept'] = name_path
                e = e.rename(columns={'序号': 'rank', '代码': 'code', '名称': 'name', '现价': 'close',
                                      '涨跌幅':'pct_change','涨跌':'change','振幅':'swing','换手':'turnover_rate','成交额':'amount',
                                      '流通股':'float_share','市盈率':'pe','流通市值':'float_mv','涨速':'growth_rate','量比':'volume_ratio',
                                      })

                e['code'] = e['code'].astype('str')
                e = finlib.Finlib().add_market_to_code(e)

                e.to_csv(csv_f, encoding='UTF-8', index=False)
                logging.info(f"fetched concept compsitives of {csv_f}, len {str(e.__len__())}")
                # concept_consist_df = concept_consist_df.append(e)
                concept_consist_df = pd.concat([concept_consist_df,e])
            except:
                logging.fatal("exception stock_board_concept_cons_ths")


    concept_consist_df.to_csv(concept_consist_csv, encoding='UTF-8', index=False)
    logging.info(f"all THS concept consist {concept_consist_csv}, len {str(concept_consist_df.__len__())}")


def fetch_etf():
    cache_days = 2

    base_dir = '/home/ryan/DATA/DAY_Global'
    etf_list_csv = base_dir + "/etf_list.csv"

    etf_data_dir = f'{base_dir}/AG_etf'

    df_etf_list = pd.DataFrame()


    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    if not os.path.exists(etf_data_dir):
        os.makedirs(etf_data_dir)


    if finlib.Finlib().is_cached(etf_list_csv, day=cache_days, use_last_trade_day=False):
        df_etf_list = pd.read_csv(etf_list_csv)
        logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + etf_list_csv)
    else:
        df_etf_list = ak.fund_etf_category_sina(symbol="ETF基金")

        df_etf_list = df_etf_list.rename(columns={'代码': 'code', '名称': 'name', '最新价': 'close',
                              '涨跌额': 'chg', '涨跌幅': 'chg_pct',
                              '买入': 'buy', '卖出': 'sell',
                              '昨收': 'pre_close', '今开': 'open',
                              '最高': 'high', '最低': 'low',
                              '成交量': 'vol', '成交额': 'amount',
                              })

        df_etf_list.to_csv(etf_list_csv, encoding='UTF-8', index=False)
        logging.info(f"fetched {etf_list_csv} , len {str(etf_list_csv.__len__())}")

    df_eft_all_data = pd.DataFrame()

    for index,row in df_etf_list.iterrows():
        code = row['code']
        name = row['name']
        print(f'fetching {code} {name}')

        csv_f = f"{etf_data_dir}/{code}.csv"
        if finlib.Finlib().is_cached(csv_f, day=cache_days, use_last_trade_day=False):
            e = pd.read_csv(csv_f)
            logging.info(f"file updated in {str(cache_days)} days, not fetch again. " + csv_f)
        else:
            try:
                e = ak.fund_etf_hist_sina(symbol=code)
                e['date'] = e['date'].apply(lambda _d: _d.strftime("%Y%m%d")) #2020-01-01 ===> 20200101
                e['code']=code
                e['name']=name
                e = finlib.Finlib().adjust_column(df=e,col_name_list=['code','name'])
                e.to_csv(csv_f, encoding='UTF-8', index=False)
                logging.info(f"fetched bar of {csv_f} , len {str(e.__len__())}")
            except:
                logging.fatal("exception fetch_etf")

        # e['date'] = e['date'].apply(lambda _d: datetime.datetime.strptime(_d, '%Y-%m-%d').strftime("%Y%m%d"))

        # df_eft_all_data = df_eft_all_data.append(e)
        df_eft_all_data = pd.concat([df_eft_all_data,e])

    # df_eft_all_data['date'] = df_eft_all_data['date'].apply(lambda _d: datetime.datetime.strptime(_d, '%Y-%m-%d').strftime("%Y%m%d"))

    etf_all_data_csv = base_dir + "/etf_all_data.csv"
    df_eft_all_data.to_csv(etf_all_data_csv, encoding='UTF-8', index=False)

    logging.info(f"etf fetch completed. etf all data saved to {etf_all_data_csv}")

def analyze_etf():
    cache_days = 1

    base_dir = '/home/ryan/DATA/DAY_Global'
    etf_list_csv = base_dir + "/etf_list.csv"

    etf_data_dir = f'{base_dir}/AG_etf'

    df_etf_list = pd.read_csv(etf_list_csv)

    df_inc = finlib.Finlib().get_stock_increase(increase_only=True, etf=True)

    df_etf_inc = pd.merge(left=df_etf_list[['code','name','close']], right=df_inc[['code','inc2','inc5','inc30','inc90','inc180']], on=['code'], how='left')

    #increase most
    df_etf_inc.sort_values(by='inc180',ascending=False).head(10).reset_index().drop('index',axis=1)

    #decrease most
    df_etf_inc.sort_values(by='inc180',ascending=True).head(10).reset_index().drop('index',axis=1)

    rtn_csv = '/home/ryan/DATA/result/etf.csv'
    if finlib.Finlib().is_cached(rtn_csv):
        df_rtn = pd.read_csv(rtn_csv)
    else:
        df_etf_describe = pd.DataFrame()
        for index, row in df_etf_list.iterrows():
            code= row['code']
            name= row['name']
            print(f'{code},{name}')

            csv = f"/home/ryan/DATA/DAY_Global/AG_etf/{code}.csv"
            df = pd.read_csv(csv).tail(100)
            describe = df.tail(100)['close'].describe()

            _df = finlib_indicator.Finlib_indicator().add_ma_ema(df,short=4,middle=27)[['date','code','name', 'close','close_4_sma','close_27_sma']].iloc[-1]
            _df['std'] = round(100*describe['std'],2)
            _df['zuida_huiche'] = round((describe['min'] - describe['max'])*100/describe['max'],2)

            _df['max'] = round(describe['max'],2)
            _df['min'] = round(describe['min'],2)
            _df['mean'] = round(describe['mean'],2)
            _df['date'] = _df['date'].astype('str')

            # df_etf_describe = df_etf_describe.append(_df)
            df_etf_describe = pd.concat([df_etf_describe,_df])

        df_rtn = pd.merge(left= df_etf_describe,
                        right=df_etf_inc[['code', 'inc2', 'inc5', 'inc30', 'inc90', 'inc180']], on=['code'],
                        how='left')


        df_rtn = finlib.Finlib().adjust_column(df=df_rtn,col_name_list=['code', 'name',  'date','std', 'zuida_huiche','close_4_sma', 'close_27_sma','max', 'mean', 'min', 'close'])
        df_rtn['ma4-27'] = round((df_rtn['close_4_sma']- df_rtn['close_27_sma'])*100/df_rtn['close_27_sma'],1)

        df_rtn.to_csv(rtn_csv, encoding='UTF-8', index=False)
        logging.info(f"saved to {rtn_csv}  len {str(df_rtn.__len__())}")


    #print sorted output
    #most stable, smallest std
    df_rtn.sort_values(by='std',ascending=True).head(10).reset_index().drop('index',axis=1)[['code','name','date','std']]

    #smallest huiche
    df_rtn.sort_values(by='zuida_huiche',ascending=False).head(10).reset_index().drop('index',axis=1)[['code','name','zuida_huiche']]

    # largest std
    df_rtn.sort_values(by='std',ascending=False).head(10).reset_index().drop('index',axis=1)[['code','name','std']]

    #largest huiche
    df_rtn.sort_values(by='zuida_huiche',ascending=True).head(10).reset_index().drop('index',axis=1)[['code','name','zuida_huiche']]



    logging.info(f"etf analyze completed.")




def generate_stock_concept():
    csv_f = '/home/ryan/DATA/DAY_Global/AG_concept/stock_to_concept_map.csv'
    csv_f_concept_to_stock = '/home/ryan/DATA/DAY_Global/AG_concept/concept_to_stock_map.csv'

    f_em='/home/ryan/DATA/DAY_Global/AG_concept/em_concept_consist.csv'
    f_ths='/home/ryan/DATA/DAY_Global/AG_concept/ths_concept_consist.csv'

    df_em = pd.read_csv(f_em)[['rank','code','name','concept']]
    df_em['concept']=df_em['concept']+".em"
    df_em_map = df_em.groupby( by=['code'], as_index=False).agg( {'concept': ','.join})

    df_ths = pd.read_csv(f_ths)[['rank','code','name','concept']]
    df_ths['concept'] = df_ths['concept'] + ".ths"
    df_ths_map = df_ths.groupby( by=['code'], as_index=False).agg( {'concept': ','.join})


    '''
    in 招商证券，　齿轮，　数据导出，　板块导出，　ｗｉｎｄｏｗ　用ｎｏｔｅｐａｄ打开，另存为ｕｔｆ－８．　保存到ｕｂｕｎｔｕ
    '''
    f1 = '/home/ryan/DATA/DAY_Global/AG_TDX_Tag/地区板块.txt'
    f2 = '/home/ryan/DATA/DAY_Global/AG_TDX_Tag/风格板块.txt'
    f3 = '/home/ryan/DATA/DAY_Global/AG_TDX_Tag/概念板块.txt'
    f4 = '/home/ryan/DATA/DAY_Global/AG_TDX_Tag/指数板块.txt'
    f5 = '/home/ryan/DATA/DAY_Global/AG_TDX_Tag/行业板块.txt'

    df1 = pd.read_csv(f1, converters={'code': str}, header=None,  names=['id', 'concept', 'code', 'name'])
    df2 = pd.read_csv(f2, converters={'code': str}, header=None,  names=['id', 'concept', 'code', 'name'])
    df3 = pd.read_csv(f3, converters={'code': str}, header=None,  names=['id', 'concept', 'code', 'name'])
    df4 = pd.read_csv(f4, converters={'code': str}, header=None,  names=['id', 'concept', 'code', 'name'])
    df5 = pd.read_csv(f5, converters={'code': str}, header=None,  names=['id', 'concept', 'code', 'name'])

    # df_tdx = df1.append(df2).append(df3).append(df4).append(df5)
    df_tdx = pd.concat([df1,df2,df3,df4,df5])


    df_tdx['concept']=df_tdx['concept']+".tdx"
    df_tdx = finlib.Finlib().add_market_to_code(df=df_tdx)
    # df_tdx.rename(columns={'concept':'concept_tdx'},inplace=True)

    '''len 99608
               code   name   concept
    0      SZ301098   金埔园林  新型城镇化.em
    '''
    # df_cpt_stk_lst = df_em.append(df_ths).append(df_tdx)[['code', 'name', 'concept']]
    df_cpt_stk_lst = pd.concat([df_em,df_ths,df_tdx])[['code', 'name', 'concept']]
    df_cpt_stk_lst = finlib.Finlib().add_amount_mktcap(df=df_cpt_stk_lst)

    ''' len 9609
         code  cpt_cnt_of_code
    0  SZ000001               20
    '''
    df_cnt  = df_cpt_stk_lst.groupby( by=['code'], as_index=False)['concept'].count().rename(columns={'concept':'cpt_cnt_of_code'})

    ''' len 9609
             code                                            concept
    0  SZ000001  深圳板块.tdx,破净资产.tdx,大盘股.tdx,低市盈率.tdx,低市净率.tdx,保险...
    '''
    df_cpt  = df_cpt_stk_lst.groupby( by=['code'], as_index=False).agg({'concept': ','.join})
    df_rtn = pd.merge(left=df_cnt, right=df_cpt, on='code', how='outer')

    ''' len 4895
              code  cpt_cnt_of_code                                            concept
    4892  SZ301289                6  创业板综.em,次新股.em,注册制次新股.em,新股与次新股.ths,上海板块.tdx,综...
    '''
    df_rtn = finlib.Finlib().add_stock_name_to_df(df_rtn)
    df_rtn = finlib.Finlib().add_amount_mktcap(df_rtn)
    df_rtn.to_csv(csv_f, encoding='UTF-8', index=False)
    logging.info(f"THX,THS,EM Stock_to_Concept map saved.  {csv_f}, len {str(df_rtn.__len__())}")


    ######### Concept to Stock Map #####
    # df_cpt_stk_lst = df_em.append(df_ths).append(df_tdx)[['code', 'name', 'concept']]

    df_cpt_stk = df_cpt_stk_lst.groupby( by=['concept'], as_index=False).agg(
        {
            'code': ','.join,
            'name': ','.join,
            'amount': sum,
            'total_mv': sum,
            'circ_mv': sum
         })
    df_cnt  = df_cpt_stk_lst.groupby( by=['concept'], as_index=False)['name'].count().rename(columns={'name':'code_cnt_of_cpt'})
    df_rtn = pd.merge(left=df_cnt, right=df_cpt_stk, on='concept', how='inner')

    df_rtn.to_csv(csv_f_concept_to_stock, encoding='UTF-8', index=False)
    logging.info(f"THX,THS,EM Concept_to_Stock map saved.  {csv_f_concept_to_stock}, len {str(df_rtn.__len__())}")


def main():
    parser = OptionParser()

    parser.add_option("-f", "--fetch_after_market", action="store_true", dest="fetch_after_market", default=False, help="fetch market data after market closure.")
    parser.add_option("--fetch_em_concept", action="store_true", dest="fetch_em_concept", default=False, help="fetch east money concept board daily price history and concept compositives.")
    parser.add_option("--fetch_ths_concept", action="store_true", dest="fetch_ths_concept", default=False, help="fetch tong_hua_shun concept board daily price history and concept compositives.")
    parser.add_option("--fetch_etf", action="store_true", dest="fetch_etf", default=False, help="fetch eft")
    parser.add_option("--analyze_etf", action="store_true", dest="analyze_etf", default=False, help="analyze eft")
    parser.add_option("--generate_stock_concept", action="store_true", dest="generate_stock_concept", default=False, help="generate_stock_concept")

    parser.add_option("-i", "--wei_pan_la_sheng", action="store_true", dest="wei_pan_la_sheng", default=False, help="get stocks price roaring. Need run twice then get the comparing increase")
    parser.add_option("-c", "--fetch_cb", action="store_true", dest="fetch_cb", default=False, help="get convertable bond")

    (options, args) = parser.parse_args()

    # df = fetch_ak(api='bond_zh_hs_cov_spot', note='龙虎榜-机构席位追踪')

    #
    # df = fetch_ak(api='stock_zh_a_hist_min_em', note='000001_5_min',
    #               parms=f"symbol='000001',start_date='2022-06-10 09:30:00',end_date='2021-06-10 15:00:00',period='5',adjust=''"
    #               )
    #
    # stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2022-06-10 09:32:00",
    #                                                       end_date="2022-06-10 15:00:00", period='1', adjust='')


    #find the rapid up stocks in market. Comparing the increase percent with the number got in previous run.
    #set up crontabs, run at 2:00, and 2:45.  Check the output of 2.45 run.
    if options.wei_pan_la_sheng:
        wei_pan_la_sheng()

    if options.fetch_em_concept:
        fetch_em_concept()

    if options.fetch_ths_concept:
        fetch_ths_concept()


    if options.fetch_etf:
        fetch_etf()

    if options.analyze_etf:
        analyze_etf()

    if options.generate_stock_concept:
        generate_stock_concept()

    if options.fetch_after_market:
        fetch_after_market_close()
    if options.fetch_cb:
        df_cb = fetch_ak(api='bond_zh_hs_cov_spot',
                         note='债券-沪深可转债-实时行情数据',
                         parms='',
                         # cache_day=1/24/4,
                         cache_day=1,
                         force_fetch=True)


        df_cb = df_cb.rename(columns={"code": "cb_code"}, inplace=False)
        df_cb = df_cb.rename(columns={"symbol": "symbol", "trade": "cb_trade" }, inplace=False)
        df_cb['symbol'] = df_cb['symbol'].str.upper()
        df_cb['cb_code'] = df_cb['cb_code'].astype('str')


        df_cb_cmp = fetch_ak(api='bond_cov_comparison',
                             note='可转债比价表',
                             parms='',
                             # cache_day=1/24/4,
                             cache_day=1,
                             force_fetch=True)


        df_cb_cmp = df_cb_cmp.rename(columns={
            "转债代码": "cb_code", "转债名称": "cb_name","正股代码":"code","正股名称":"name",
            "转债最新价": "cb_price", "转债涨跌幅": "cb_change","正股最新价":"stock_price","正股涨跌幅":"stock_change",
            "转股价": "zhuan_gu_jia", "转股价值": "zhuan_gu_jia_zhi","转股溢价率":"zhuan_gu_yi_jia","纯债溢价率":"zhuan_zhai_yi_jia",
        }, inplace=False)
        df_cb_cmp = df_cb_cmp[df_cb_cmp['stock_price'] != '-']

        df_cb_cmp['cb_code'] = df_cb_cmp['cb_code'].astype('str')
        # df_cb_cmp['zhuan_gu_jia'] = df_cb_cmp['zhuan_gu_jia'].astype('float')
        # df_cb_cmp['zhuan_gu_jia_zhi'] = df_cb_cmp['zhuan_gu_jia_zhi'].astype('float')
        # df_cb_cmp['zhuan_gu_yi_jia'] = df_cb_cmp['zhuan_gu_yi_jia'].astype('float')
        # df_cb_cmp['zhuan_zhai_yi_jia'] = df_cb_cmp['zhuan_zhai_yi_jia'].astype('float')
        # df_cb_cmp['stock_price'] = df_cb_cmp['stock_price'].astype('float')
        # df_cb_cmp['stock_change'] = df_cb_cmp['stock_change'].astype('float')
        # df_cb_cmp['cb_change'] = df_cb_cmp['cb_change'].astype('float')
        # df_cb_cmp['cb_price'] = df_cb_cmp['cb_price'].astype('float')

        df_cb_cmp = finlib.Finlib().add_market_to_code(df=df_cb_cmp)

        df_cb = pd.merge(left=df_cb,right=df_cb_cmp,on='cb_code', how='inner',suffixes=('', '_cmp'))


        df_stock = pd.read_csv("/home/ryan/DATA/result/wei_pan_la_sheng/AG_latest_result.csv")

        df_rst = pd.merge(left=df_stock,right=df_cb, on='code',how='inner',suffixes=('_stock', '_cb'))

        df_rst_show = df_rst[['code','name_stock','close','stock_price',
                         'changepercent_stock','stock_change',
                         'cb_code','cb_name','cb_price','cb_change','zhuan_gu_jia']]


        df_rst_show['price_zhuangujia'] = 100*(df_rst_show['stock_price']-df_rst_show['zhuan_gu_jia'])/df_rst_show['stock_price']

        stock_value_after_conver = (1000/df_rst_show['zhuan_gu_jia']*df_rst_show['stock_price'])
        df_rst_show['yijia'] = 100*(df_rst_show['cb_price']*10 - stock_value_after_conver ) /  df_rst_show['cb_price']/10

        #because strage is to buy CB, the higher stock price, the cheaper the cb.
        df_rst_show = df_rst_show[df_rst_show['price_zhuangujia'] > 0 ]
        df_rst_show = df_rst_show[df_rst_show['yijia'] < 10]


        # df_rst_show = df_rst_show[(df_rst_show['close']-df_rst_show['zhuan_gu_jia'])/df_rst_show['close']] # close is yesterday close


        print(finlib.Finlib().pprint(df_rst_show.head(5)))

        print(1)

    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
