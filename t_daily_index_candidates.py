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

import math
from optparse import OptionParser
import sys
import constant
from scipy import stats

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# reduce webdriver session log for every request.
logging.getLogger("urllib3").setLevel(logging.WARNING)  # This supress post/get log in console.

#
# from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER
# from selenium.webdriver.remote.remote_connection import logging as SELENIUM_logging
# SELENIUM_LOGGER.setLevel(SELENIUM_logging.ERROR)

import shutil
import pickle

##############

def tv_source(index_name,idict,period_start,period_end, ndays):
    # index file is get by t_daily_get_us_index.py from WikiPedia.
    df_nas100 = pd.read_csv(os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/INDEX_US_HK/nasdqa100.csv'))
    df_spx500 = pd.read_csv(os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/INDEX_US_HK/sp500.csv'))

    # the file is downloaded manually in Chrome Save Page WE addon. Contains all US market stocks (7000+) and all columns (200+)
    df_mkt_us = pd.read_csv(os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/Stock_Fundamental/TradingView/america_latest.csv')).sort_values(
        by='Market Capitalization', ascending=False)
    df_mkt_cn = pd.read_csv(os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/Stock_Fundamental/TradingView/china_latest.csv'),converters={'Ticker': str}).sort_values(
        by='Market Capitalization', ascending=False)

    if index_name == 'nasdaq100':
        df_mkt = df_mkt_us[df_mkt_us['Exchange'] == 'NASDAQ']
        df_idx = df_nas100
    elif index_name == 'spx500':
        df_idx = df_spx500
        df_mkt = df_mkt_us
    elif index_name == 'cn_sse':
        # df_idx = finlib.Finlib().load_index(index_code=idict['hs300'], index_name='hs300')
        df_idx = index_weight_wg(index_name = 'hs300')
        df_mkt = df_mkt_cn[df_mkt_cn['Exchange'] == 'SSE']
    elif index_name == 'cn_szse':
        # df_idx = finlib.Finlib().load_index(index_code=idict['szcz'], index_name='szcz')
        df_idx = index_weight_wg(index_name = 'szcz')
        df_mkt = df_mkt_cn[df_mkt_cn['Exchange'] == 'SZSE']
    elif index_name == 'cn':
        # df_idx = finlib.Finlib().load_index(index_code=idict['hs300'], index_name='hs300')
        df_idx = index_weight_wg(index_name = 'hs300')
        df_mkt = df_mkt_cn

    df_mkt = df_mkt[['Ticker', 'Market Capitalization', 'Volume*Price','Simple Moving Average (10)','Average Volume (10 day)']]
    df_mkt.columns = ['code', 'circ_mv', 'amount','sma10','vol_avg10']

    #using 10 days average value.  instead of the day's Volume*Price.
    df_mkt['amount'] = df_mkt['sma10']*df_mkt['vol_avg10']
    df_mkt = df_mkt.drop('sma10', axis=1)
    df_mkt = df_mkt.drop('vol_avg10', axis=1)

    if index_name in ['cn_sse', 'cn_szse', 'cn']:
        df_mkt = finlib.Finlib().add_market_to_code(df_mkt)
        df_mkt = finlib.Finlib().add_stock_name_to_df(df_mkt)
    else:
        df_mkt['name'] = df_mkt['code']
    df_mkt['circ_mv_perc'] = df_mkt['circ_mv'].apply(
        lambda _d: round(stats.percentileofscore(df_mkt['circ_mv'], _d) / 100, 4))
    df_mkt['amount_perc'] = df_mkt['amount'].apply(
        lambda _d: round(stats.percentileofscore(df_mkt['amount'], _d) / 100, 4))

    df_idx = df_idx.merge(df_mkt, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)

    df_mkt['date']=str(finlib.Finlib().get_last_trading_day())


    compare_with_official_index_list(df_my_index=df_mkt.head(100), df_offical_index=df_idx, index_name=index_name,
                                     period_start=period_start,
                                     period_end=period_end, ndays=ndays)

def compare_with_official_index_list(df_my_index,df_offical_index, index_name,period_start,period_end, ndays):
    df_merged = pd.merge(df_my_index,df_offical_index, indicator=True, on='code', how='outer',suffixes=('','_x')).reset_index().drop('index', axis=1)

    #replace with name_x if name is None
    if 'name_x' in df_merged.columns:
        df_merged.loc[df_merged['name'].isna(), ['name']] = df_merged['name_x']
        df_merged = df_merged.drop('name_x', axis=1)

    if 'circ_mv_perc_x' in df_merged.columns:
        df_merged.loc[df_merged['circ_mv_perc'].isna(), ['circ_mv_perc']] = df_merged['circ_mv_perc_x']
        df_merged = df_merged.drop('circ_mv_perc_x', axis=1)

    if 'total_mv_perc_x' in df_merged.columns:
        df_merged.loc[df_merged['total_mv_perc'].isna(), ['total_mv_perc']] = df_merged['total_mv_perc_x']
        df_merged = df_merged.drop('total_mv_perc_x', axis=1)

    if 'amount_perc_x' in df_merged.columns:
        df_merged.loc[df_merged['amount_perc'].isna(), ['amount_perc']] = df_merged['amount_perc_x']
        df_merged = df_merged.drop('amount_perc_x', axis=1)

    if  'list_date_days_before_x' in df_merged.columns:
        df_merged.loc[df_merged['list_date_days_before'].isna(), ['list_date_days_before']] = df_merged['list_date_days_before_x']
        df_merged = df_merged.drop('list_date_days_before_x', axis=1)


    if 'date_x' in df_merged.columns:
        df_merged.loc[df_merged['date'].isna(), ['date']] = df_merged['date_x']
        df_merged = df_merged.drop('date_x', axis=1)

    if 'list_date_days_before_x' in df_merged.columns:
        df_merged = df_merged.drop('list_status_x', axis=1)

    if 'list_status' in df_merged.columns:
        df_merged = df_merged.drop('list_status', axis=1)

    if 'list_status_x' in df_merged.columns:
        df_merged = df_merged.drop('list_status_x', axis=1)

    if 'circ_mv_x' in df_merged.columns:
        df_merged = df_merged.drop('circ_mv_x', axis=1)

    if 'total_mv_x' in df_merged.columns:
        df_merged = df_merged.drop('total_mv_x', axis=1)

    if 'amount_x' in df_merged.columns:
        df_merged = df_merged.drop('amount_x', axis=1)

    if 'founded' in df_merged.columns:
        df_merged = df_merged.drop('founded', axis=1)

    if 'cik' in df_merged.columns:
        df_merged = df_merged.drop('cik', axis=1)

    # df_merged = df_merged.drop('amount', axis=1)
    # df_merged = df_merged.drop('circ_mv', axis=1)

    df_merged['predict'] = None
    df_merged.loc[df_merged['_merge'] == 'both', ['predict']] = 'To_Be_Kept'
    df_merged.loc[df_merged['_merge'] == 'right_only', ['predict']] = 'To_Be_Removed'
    df_merged.loc[df_merged['_merge'] == 'left_only', ['predict']] = 'To_Be_Added'
    df_merged = df_merged.drop('_merge', axis=1)

    df_merged = finlib.Finlib().adjust_column(df=df_merged, col_name_list=['code','name','date','close',
                                                                           'total_mv_perc','circ_mv_perc','amount_perc',
                                                                           'my_index_weight','weight','mkt_cap',
                                                                           'list_date_days_before','predict'
                                                                           ])

    len_merged = df_merged.__len__()
    index_candiate_csv = os.path.abspath(os.path.expanduser("~")+"/DATA/result/"+index_name+"_candidate_list.csv")

    sort_col_name = ''
    if "total_mv_perc" in df_merged.columns:
        sort_col_name = "total_mv_perc"
    elif "circ_mv_perc" in df_merged.columns:
        sort_col_name = "circ_mv_perc"

    df_merged = df_merged.sort_values(by=sort_col_name, ascending=False, inplace=False).reset_index().drop('index',axis=1)
    df_merged.to_csv(index_candiate_csv, encoding='UTF-8', index=False)
    logging.info("saved " + index_candiate_csv + " len " + str(len_merged))

    df_both = df_merged[df_merged['predict'] == constant.TO_BE_KEPT]
    df_both = df_both.sort_values(by=sort_col_name, ascending=False, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_both.__len__()) + " out of " + str(len_merged) + " in both myhs300 and officalhs300, they should will be kept in the "+index_name)
    logging.info(finlib.Finlib().pprint(df=df_both.head(32)))


    df_offlical_only = df_merged[df_merged['predict'] == constant.TO_BE_REMOVED].reset_index().drop('index', axis=1)  # possible will be removed from hs300 index next time
    df_offlical_only = df_offlical_only.sort_values(by=sort_col_name, ascending=True, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_offlical_only.__len__()) + " out of " + str(
        len_merged) + " in offical list,"+ period_start+" to " + period_end +", period days "+ str(ndays) +". They possible will be removed from "+index_name+" next time. Top 32")
    logging.info(finlib.Finlib().pprint(df=df_offlical_only.head(32)))

    df_myonly = df_merged[df_merged['predict'] == constant.TO_BE_ADDED].reset_index().drop('index', axis=1)
    df_myonly = df_myonly.sort_values(by=sort_col_name, ascending=False, inplace=False).reset_index().drop('index', axis=1)
    logging.info("\n"+str(df_myonly.__len__()) + " out of " + str(len_merged) +" in my list, "+ period_start+" to "     + period_end +", period days "+ str(ndays) +". They possible will be added to "+index_name+" next time. Top 32")
    #print(finlib.Finlib().pprint(df=df_myonly.head(32)))
    logging.info(finlib.Finlib().pprint(df=df_myonly.head(32)))

    logging.info("result saved to " + index_candiate_csv + " len " + str(len_merged))

def szcz_on_market_days_filter():
    df_basic = finlib.Finlib().get_today_stock_basic()

    today = datetime.datetime.today()
    y = str(today.year)
    m = today.month

    if m <= 5:
        to_date =  datetime.datetime.strptime(y+'0430', "%Y%m%d")
    else:
        to_date = datetime.datetime.strptime(y+'1031', "%Y%m%d")

    df_basic['on_market_days_to_next_index_resample_date']=df_basic['list_date'].apply(lambda _d: (to_date - datetime.datetime.strptime(str(_d), "%Y%m%d") ).days)

    df_basic = df_basic[df_basic['on_market_days_to_next_index_resample_date'] > 180]
    return(df_basic)


def hs300_on_market_days_filter():
    #### filter with HS300 critiria
    df_all = finlib.Finlib().add_market_to_code(finlib.Finlib().get_A_stock_instrment(code_name_only=False))
    # df_all_basic = finlib.Finlib().get_today_stock_basic()
    print("all lens "+str(df_all.__len__()))

    today = datetime.datetime.today()
    y = str(today.year)
    m = today.month

    to_date_first = datetime.datetime.strptime(y + '0430', "%Y%m%d")
    to_date_second = datetime.datetime.strptime(y + '1031', "%Y%m%d")

    # "每年 5 月下旬: 上一年5.1 到今年4.30 (期间新上市证券为上市第四个交易日以来)
    #
    #  11 月下旬:  上一
    # 年度 11 月 1 日至审核年度 10 月 31 日（期间新上市证券为上市第四
    # 个交易日以来）"

    if m <= 5:
        to_date = to_date_first
    else:
        to_date = to_date_second

    df_all['on_market_days_to_next_index_resample_date'] = df_all['list_date'].apply(
        lambda _d: (to_date - datetime.datetime.strptime(str(_d), "%Y%m%d")).days)

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
            df_tmp = df_tmp[df_tmp['on_market_days_to_next_index_resample_date'] > 365]  #科创板证券：上市时间超过一年。
        elif m == "创业板": #300xxx
            df_tmp = df_tmp[df_tmp['on_market_days_to_next_index_resample_date'] > 365*3]  #创业板证券：上市时间超过三年
        elif m == "主板" or  m == "中小板" or m == 'CDR':  # 主板 000xxx, 600xxx,  中小板 002xxx, CDR 689
            df_tmp = df_tmp[df_tmp['on_market_days_to_next_index_resample_date'] > 90]  #其他证券：上市时间超过一个季度，除非该证券自上市以来日均总市值排在前 30 位。。

        len_removed_for_a_mkt = len_df_tmp_ori - df_tmp.__len__()

        df_hs300_filted_on_market_days = df_hs300_filted_on_market_days.append(df_tmp)
        logging.info("appended df of "+m+", len removed "+ str(len_df_tmp_ori)+ " - "+str(df_tmp.__len__()) +" = "+str(len_removed_for_a_mkt))

    logging.info("after filted by HS300 on market days, df len is "+str(df_hs300_filted_on_market_days.__len__()))
    return(df_hs300_filted_on_market_days[['code','name','list_status','list_date_days_before']])


def get_hs300_total_share_weighted():
    #沪深300指数是按照自由流通量加权计算指数，样本股在指数中的权重由其自由流通量决定
    basic_dir = os.path.abspath(os.path.expanduser("~")+"/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily")
    df_basic = pd.read_csv(basic_dir + "/basic_" + finlib.Finlib().get_last_trading_day() + ".csv")

    #free_share 自由流通股本 （万）.   自由流通量 =样本总股本 - 非自由流通股本
    #total_share  总股本 （万股）
    #free_ration 自由流通比例 = 自由流通量 /样本总股本
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

    # hs300_total_share_weighted approximatly equal free_share 自由流通股本 （万). Has nothing related to Price.in my list, period_end
    # hs300_total_share_weighted: means 'total share after weighter' 调整股本数, used by 调整市值 = ∑(证券价格×调整股本数)。
    # 调整股本数 = 样本总股本× 加权比例
    df_basic['hs300_total_share_weighted']=df_basic['total_share']*df_basic['weight_calc']*0.01
    df_basic = finlib.Finlib().ts_code_to_code(df=df_basic)
    df_basic = finlib.Finlib().add_stock_name_to_df(df=df_basic)

    return(df_basic[['code','name','hs300_total_share_weighted']])



############
def fetch_stock_industry_wugui_selenium(browser):
    csv_o = "/home/ryan/DATA/pickle/ag_stock_industry_wg.csv"

    if finlib.Finlib().is_cached(file_path=csv_o,day=30):
        logging.info("wglh industry fetched in 30 days, skip fetching. "+csv_o)
        return()

    ### fetch industry start
    u="https://wglh.com/sector/"
    browser.get(u)
    obj_tbl = browser.find_element_by_id("table1")

    df_all = pd.DataFrame()
    industry_links = []
    for l in obj_tbl.find_elements_by_partial_link_text(''):
        industry_links.append(l.get_property('href'))

    for l in industry_links:
        # print(l.get_property('href'))
        browser.get(l)

        industry_name = browser.find_elements_by_class_name('clearfix')[0].text.split("_")[0]
        industry_code = browser.find_elements_by_class_name('clearfix')[0].text.split("_")[1].splitlines()[0].split(".")[1]
        # print(industry_name)

        tbl = browser.find_element_by_class_name('bootstrap-table')
        s_list = tbl.find_elements_by_class_name('text-left')

        for i in s_list:
            code = i.text.split(". ")[0]
            name = i.text.split(". ")[1]
            logging.info(code+" "+name+" "+industry_name)
            df = pd.DataFrame({'code':[code],'name_wg':[name],'industry_name_wg':[industry_name],'industry_code_wg':[industry_code]})
            df_all = df_all.append(df)

        df_all.to_csv(csv_o, encoding='UTF-8', index=False)

    df_all = pd.read_csv(csv_o, converters={'code':str})
    df_all = finlib.Finlib().add_market_to_code(df_all)
    df_all = finlib.Finlib().add_stock_name_to_df(df_all)
    df_all.to_csv(csv_o, encoding='UTF-8', index=False)
    logging.info("completed. fetched stock's industry from wugui, save to "+csv_o+" len "+str(df_all.__len__()))
    return(df_all)



def wglh_login():
    opts = webdriver.ChromeOptions()
    # opts.add_argument("user-data-dir=/home/ryan/.config/google-chrome")
    # opts.add_argument("profile-directory=Default")
    browser = webdriver.Chrome(options=opts)
    browser.get('https://wglh.com/user/account/')

    #Manually login. Previous login GUI changed.
    logging.info("Please manually login in 60 sec")
    WebDriverWait(browser, 60).until(EC.title_contains("我的账号信息"))
    return(browser)


def wglh_download():
    b = wglh_login()
    fetch_index_wugui_selenium(b)
    fetch_stock_industry_wugui_selenium(b)


############
def fetch_index_wugui_selenium(browser):
    wg_d = os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/Stock_Fundamental/WuGuiLiangHua')

    wg_index_dict = {
        'hs300': {'c':'SH000300', 'sheet': '沪深300的成分股', },  # 沪深300的历史估值和成分股估值权重下载
        'zz100': {'c':'SH000903', 'sheet': '中证100的成分股', },  # 中证100的历史估值和成分股估值权重下载
        'zz500': {'c': 'SH000905', 'sheet': '中证500的成分股', },  # 中证500的历史估值和成分股估值权重下载
        'szcz': {'c': 'SZ399001', 'sheet': '深证成指的成分股', },  # 深证成指的历史估值和成分股估值权重下载
        'sz100': {'c': 'SZ399330', 'sheet': '深证100的成分股', },  # 深证100的历史估值和成分股估值权重下载
        'tech_advance': {'c': 'CSI931087', 'sheet': '科技龙头的成分股', },  # 科技龙头
    }

    for index_name in wg_index_dict.keys():
        code = wg_index_dict[index_name]['c']
        u = 'https://androidinvest.com/chinaindicesdodown/'+code+'/'
        f = os.path.abspath(os.path.expanduser("~")+"/Downloads/"+datetime.datetime.today().strftime("%Y%m%d")+"_IndexData_"+code+".xls")

        # 20210125_IndexData_CSI931087.xls

        f2 = wg_d+"/"+datetime.datetime.today().strftime("%Y%m%d")+"_IndexData_"+code+".xls"

        if finlib.Finlib().is_cached(file_path=f2,day=30):
            logging.info("wglh index fetched in 30 days, not fetching again. "+f2)
            continue


        f_sl = wg_d+"/"+code+".xls"
        logging.info("Download from wugui, index_name "+index_name+", url "+u)
        browser.get(u)

        #20210111_IndexData_SH000300.xls
        while not os.path.exists(f):
            logging.info("waiting download complete, expecting "+f)
            time.sleep(1)

        shutil.move(f,f2)
        logging.info(index_name+" downloaded. "+f)

        if os.path.exists(f_sl):
            os.unlink(f_sl)

        os.symlink(f2, f_sl)
        logging.info(index_name+",symbol link created. "+f_sl+" --> "+f2)

        #### save the sheet to csv
        logging.info("loading index from wugui, " + index_name)
        df = pd.read_excel(f2, sheet_name=wg_index_dict[index_name]['sheet'])

        df_rtn = df.rename(columns={
            '股票代码': 'code',
            '股票名': 'name',
            '日期': 'date',
            '收盘价_前复权': 'close',
            'A股市值(亿)': 'mkt_cap',
            '权重%': 'weight', })

        df_rtn = df_rtn[['code', 'name', 'date', 'close', 'mkt_cap', 'weight', ]]
        df_rtn.to_csv(wg_d+"/"+code+".csv", encoding='UTF-8', index=False)
        logging.info("extracted the index "+index_name+" to csv. "+wg_d+"/"+code+".csv")

        print(finlib.Finlib().pprint(df_rtn.head(2)))
        #### save the sheet to csv

    print("end of webdriver wugui index download.")
    browser.quit()





############
def fetch_index_tradingview_selenium():
    os.environ['CHROME_TMP_DOWNLOAD_DIR'] = os.path.abspath(os.path.expanduser("~")+"/Downloads/chrome_tmp_del")

    browser = finlib_indicator.Finlib_indicator().newChromeBrowser(headless=False)

    ######################################
    # Login TV and go to screener page
    ######################################
    browser = finlib_indicator.Finlib_indicator().tv_login(browser=browser)

    browser.get('https://tradingview.com/screener/')

    finlib_indicator.Finlib_indicator().tv_wait_page_to_ready(browser,timeout=10)
    logging.info("page load complete")
    WebDriverWait(browser, 10).until(EC.title_contains("Screener"))
    logging.info("Title ready")
    # time.sleep(10)

    ######################################
    # Set Filters
    ######################################
    tv_d = os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/Stock_Fundamental/TradingView')


   # have to fetch in US->CN->HK sequence. only support scroll down. not up.

    for interval in ['1D','1M', '1W']:
        market = 'US'
        finlib_indicator.Finlib_indicator().tv_screener_start(browser=browser, column_filed='ALL',interval=interval,market='US', filter='ALL_of_The_market_US' )
        f_us = finlib_indicator.Finlib_indicator().tv_screener_export(browser=browser, to_dir=tv_d, interval=interval, symbol_link_f=tv_d+'/america_latest_'+interval+'.csv')

    for interval in ['1D', '1M', '1W']:
        market = 'CN'
        finlib_indicator.Finlib_indicator().tv_screener_start(browser=browser, column_filed='ALL',interval=interval,market='CN', filter='ALL_of_The_market')
        f_cn = finlib_indicator.Finlib_indicator().tv_screener_export(browser=browser, to_dir=tv_d, interval=interval, symbol_link_f=tv_d+'/china_latest_'+interval+'.csv')

    for interval in ['1D', '1M', '1W']:
        market = 'HK'
        finlib_indicator.Finlib_indicator().tv_screener_start(browser=browser, column_filed='ALL',interval=interval,market=market, filter='ALL_of_The_market' )
        f_hk = finlib_indicator.Finlib_indicator().tv_screener_export(browser=browser, to_dir=tv_d,interval=interval, symbol_link_f=tv_d+'/hongkong_latest_'+interval+'.csv')

    browser.quit()

    logging.info("fetch_index_tradingview_selenium completed")
    return()



def index_weight_wg(index_name):
    #files are manually downloaded from https://androidinvest.com/chinaindicesdown/SH000300/

    wg_d = os.path.abspath(os.path.expanduser("~")+'/DATA/pickle/Stock_Fundamental/WuGuiLiangHua')

    wg_index_dict = {
        'hs300': {'f': wg_d + '/SH000300.csv' },  # 沪深300的历史估值和成分股估值权重下载
        'zz100': {'f': wg_d + '/SH000903.csv'  },  # 中证100的历史估值和成分股估值权重下载
        'zz500': {'f': wg_d + '/SH000905.csv'  },  # 中证500的历史估值和成分股估值权重下载
        'szcz': {'f': wg_d + '/SZ399001.csv'  },  # 深证成指的历史估值和成分股估值权重下载
        'sz100': {'f': wg_d + '/SZ399330.csv' },  # 深证100的历史估值和成分股估值权重下载
    }
    return(pd.read_csv(wg_index_dict[index_name]['f']))


def main():
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)
    # logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="debug, only check 1st 10 stocks in the list")
    parser.add_option("--force_run", action="store_true", default=False, dest="force_run", help="always check, regardless output updated in 3 days.")
    parser.add_option("--daily_update", action="store_true", default=False, dest="daily_update", help="update the symbol link to the latest day, only use at daily running.")

    parser.add_option( "--period_start", dest="period_start", help="the start date of checking scope. default is last trading day. fmt yyyymmdd. yyyy0430, yyyy1031")
    parser.add_option( "--period_end", dest="period_end", help="the END date of checking scope. default is last trading day. fmt yyyymmdd. yyyy0430, yyyy1031")
    parser.add_option("-n", "--ndays",default=0, dest="ndays",type="int", help="N days before the period_end. Use to define the start of checking period. HS300:365 Days, SZCZ:183 Days")
    parser.add_option("-i", "--index_name",default="hs300", dest="index_name",type="str", help="index name. [hs300|zz100|zz500|szcz|nasdaq100|spx500|cn_sse|cn_szse|cn]")
    parser.add_option("-s", "--index_source",default="index_source", dest="index_source",type="str", help="index source. [tushare|wugui]")
    parser.add_option("--fetch_index_tv", action="store_true", default=False, dest="fetch_index_tv",  help="fetch index list from tradingview, saved to DATA/pickle/{index_name}.csv")
    parser.add_option("--fetch_index_wg", action="store_true", default=False, dest="fetch_index_wg",  help="fetch index list from wglh, saved to /home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/{index_name}.xls")

    #

    (options, args) = parser.parse_args()
    debug = options.debug
    force_run = options.force_run
    daily_update = options.daily_update

    index_source = options.index_source
    index_name = options.index_name
    period_start = options.period_start
    period_end = options.period_end
    ndays = options.ndays
    fetch_index_tv = options.fetch_index_tv
    fetch_index_wg = options.fetch_index_wg




    idict = {
        'zz100':'000903.SH', #zhong zheng 100
        'zz200':'000904.SH',
        'zz500':'000905.SH',
        'hs300':'000300.SH',
        'szcz':'399001.SZ', #深圳成指
        'sz100':'399330.SZ', #深圳100
        'nasdaq100':'nasdaq100', #nasdaq100 source is prepared manually.
        'spx500':'spx500', #SPX/SP500 source is prepared manually.

    }

    if fetch_index_tv:
        fetch_index_tradingview_selenium()
        exit()

    if fetch_index_wg:
        #requirement:
        # pip install selenium
        # download chromedriver_linux64.zip
        # sudo mv chromedriver  /usr/local/bin/

        wglh_download()
        exit()

    if period_end is None:
        period_end = datetime.datetime.today().strftime("%Y%m%d")

    if (period_start is not None) and (period_end is not None):
        ndays = (datetime.datetime.strptime(period_end, "%Y%m%d") - datetime.datetime.strptime(period_start, "%Y%m%d")).days + 1
        logging.info("Calculated ndays " + str(ndays))

    # Handling with TradingView source for US and AG(by different index_name).
    if index_name in ['nasdaq100','spx500','cn_sse','cn_szse','cn']:
        logging.info("using tradingview source to check index "+index_name)
        tv_source(index_name,idict,period_start,period_end, ndays)
        exit(0)
    elif index_name not in ['zz100','zz200','zz500','hs300','sz100','szcz', 'nsadaq100', 'spx500','cn','cn_sse','cn_szse']:
        logging.error("unsupported index_name "+index_name)
        exit(0)


    # Following Handling with tushare source for AG

    ###############################
    # Calc amt, mktcap, weight among entire stock list.
    # Result df_amt_mktcap_weight is BASE Dataframe.
    # df_amt len 4117, df_mktcap len 4144, df_total_share_weighted len 4104
    ###############################
    # df_list_days = finlib.Finlib().add_market_to_code(finlib.Finlib().get_A_stock_instrment(code_name_only=False))[['code','name','list_status','list_date_days_before']]
    df_list_days = finlib.Finlib().add_market_to_code(finlib.Finlib().get_A_stock_instrment(code_name_only=False))[['code','list_status','list_date_days_before']]

    # df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=ndays,period_start=period_start, period_end=period_end, debug=debug,force_run=force_run) #output  /home/ryan/DATA/result/average_daily_amount_sorted.csv
    df_amt = finlib.Finlib().sort_by_amount_since_n_days_avg(ndays=None,period_start=period_start, period_end=period_end, daily_update=daily_update, debug=debug,force_run=force_run) #output  /home/ryan/DATA/result/average_daily_amount_sorted.csv
    # df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=ndays,period_start=period_start, period_end=period_end, debug=debug,force_run=force_run) #output: /home/ryan/DATA/result/average_daily_mktcap_sorted.csv
    df_mktcap = finlib.Finlib().sort_by_market_cap_since_n_days_avg(ndays=None,period_start=period_start, period_end=period_end,daily_update=daily_update, debug=debug,force_run=force_run) #output: /home/ryan/DATA/result/average_daily_mktcap_sorted.csv
    df_total_share_weighted = get_hs300_total_share_weighted()
    df_amt_mktcap = pd.merge(df_amt[['code','name', 'amount','amount_perc']],df_mktcap[['code','name', 'circ_mv','circ_mv_perc','circ_mv_portion','total_mv','total_mv_perc','total_mv_portion','date']], on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)
    df_amt_mktcap_weight = pd.merge(df_amt_mktcap,df_total_share_weighted, on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)

    ###############################
    # calc HS300 always
    ###############################
    # filter by (on_market_date len 3744) and (top 50% daily_amount)
    my_hs300 = pd.merge(df_amt_mktcap_weight[df_amt_mktcap_weight['amount_perc'] >= 0.5], hs300_on_market_days_filter(),
                        on=['code', 'name'], how='inner', suffixes=('', '_x')).reset_index().drop('index', axis=1)

    #对样本空间内剩余证券，按照过去一年的日均总市值由高到低排名，选取前 300 名的证券作为指数样本。
    my_hs300_circ_mv = my_hs300.sort_values(by=['circ_mv'], ascending=[False]).head(300).reset_index().drop('index', axis=1)
    my_hs300_total_mv = my_hs300.sort_values(by=['total_mv'], ascending=[False]).head(300).reset_index().drop('index', axis=1)
    my_hs300 = my_hs300_total_mv

    # 沪深300指数是按照自由流通量加权计算指数，样本股在指数中的权重由其自由流通量决定
    my_hs300['my_index_weight'] = round(my_hs300['hs300_total_share_weighted'] * 100.0 / my_hs300['hs300_total_share_weighted'].sum(), 2)
    my_hs300 = my_hs300.drop('hs300_total_share_weighted', axis=1)

    ###############################
    # calc SZCZ always
    ###############################
    all = finlib.Finlib().add_market_to_code(finlib.Finlib().get_A_stock_instrment())
    all_sz = all[all['code'].str.contains('SZ')]
    all_st = all[all['name'].str.contains('ST')]

    my_szcz = pd.merge(all_sz, szcz_on_market_days_filter(),
                        on=['code', 'name'], how='inner', suffixes=('', '_x')).reset_index().drop('index', axis=1)

    my_szcz = finlib.Finlib()._df_sub_by_code(df=my_szcz, df_sub=all_st,
                                              byreason='removing ST(special trade) from SZ mkt')


    # 对入围股票在最近半年的 A 股日均成交金额按从高到低排序，剔除排名后 10%的股票； Approximiately in all stock scope.
    my_szcz = pd.merge(my_szcz, df_amt_mktcap_weight[df_amt_mktcap_weight['amount_perc'] >= 0.7],
                       on=['code', 'name'], how='inner', suffixes=('', '_x')).reset_index().drop('index', axis=1)

    my_szcz = my_szcz.drop('date_x',axis=1)
    # 对选样空间剩余股票按照最近半年的 A 股日均总市值从高到低排序，选取前 500 名股票构成指数样本股
    my_szcz = my_szcz.sort_values(by=['circ_mv'], ascending=[False]).head(500).reset_index().drop('index', axis=1)
    my_szcz['my_index_weight'] = round(
        my_szcz['hs300_total_share_weighted'] * 100.0 / my_szcz['hs300_total_share_weighted'].sum(), 2)
    my_szcz = my_szcz.drop('hs300_total_share_weighted', axis=1)

    ###############################
    # Compare my_index with offical
    ###############################
    if index_name == 'hs300':
        my_index = my_hs300
    elif index_name == 'zz100':
        my_index = my_hs300.head(100) #zz100 is top maket cap of hs300
    elif index_name == 'zz500':
        all = finlib.Finlib().add_market_to_code(finlib.Finlib().get_A_stock_instrment())
        reduct_hs300 = finlib.Finlib().load_index(index_code=idict['hs300'], index_name='hs300')
        my_zz500 = finlib.Finlib()._df_sub_by_code(df=all,df_sub=reduct_hs300,byreason='removing stocks in HS300 official list')
        my_zz500 = finlib.Finlib()._df_sub_by_code(df=my_zz500,df_sub=my_hs300,byreason='removing stocks in my HS300 list')
        my_zz500 = pd.merge(my_zz500,df_amt_mktcap,on=['code','name'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)

        my_zz500 = my_zz500[my_zz500['amount_perc']>0.8]
        my_zz500 = my_zz500.sort_values(by='total_mv_perc', ascending=False).head(500).reset_index().drop('index', axis=1)
        my_index = my_zz500
    elif index_name == 'szcz':#深圳成指. --ndays should be half year. 365/2
        my_index = my_szcz
    elif index_name == 'sz100':
        my_index = my_szcz.head(100)

    if index_source == 'wugui':
        df_offical_index = index_weight_wg(index_name = index_name)
    else:
        df_offical_index = finlib.Finlib().load_index(index_code=idict[index_name], index_name=index_name)

    df_offical_index = pd.merge(df_offical_index,df_amt_mktcap[['code','total_mv_perc','circ_mv_perc','amount_perc']],on='code', how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)
    df_offical_index = pd.merge(df_offical_index,df_list_days,on=['code'], how='inner',suffixes=('','_x')).reset_index().drop('index', axis=1)

    compare_with_official_index_list(df_my_index=my_index, df_offical_index=df_offical_index, index_name=index_name, period_start=period_start, period_end=period_end, ndays=ndays)


    exit(0)

### MAIN ####
if __name__ == '__main__':
    main()
