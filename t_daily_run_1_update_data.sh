#!/usr/bin/env bash
#source /home/ryan/.bashrc
#source /home/ryan/.profile
#source /etc/bash.bashrc





#this script runs on haha_data_source

full_or_daily=$1

if [ $full_or_daily == "FULL" ]; then
    echo "$0: FULL RUN"
elif [ $full_or_daily == "DAILY" ]; then
    echo "$0: DAILY RUN"
else
    echo "Have to specify FULL or DAILY. Quiting";
    exit
fi


sleep 2



cd /home/ryan/tushare_ryan
git pull


#更新当天股票数据
######################################
#Output:
# /home/ryan/DATA/DAY_Global/AG/*csv
# /home/ryan/DATA/pickle/instrument_A.csv
######################################
# These two lines prepare data used by t_daily_pattern_Hit_Price_Volume_2.py.
python t_daily_fundamentals_2.py --fetch_basic_daily

python /home/ryan/tushare_ryan/t_daily_update_csv_from_tushare.py;



#### update the HK stock ####
echo "updating HK/US selected stock daily from tushare "
#python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK  --force_fetch;
python /home/ryan/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK;
#python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US  --force_fetch;
python /home/ryan/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US;


######################################
##update SH000001 上证指数.
#
######################################
# python /home/ryan/tushare_ryan/t_get_index.py --fetch_index #script file no longer exist



######################################
##update index
# input: None
# output:
#   /home/ryan/DATA/pickle/hs300.csv
#   /home/ryan/DATA/pickle/zz100.csv
######################################
python t_daily_index_candidates.py --fetch_index_ts --index_name hs300
python t_daily_index_candidates.py --fetch_index_ts --index_name zz100
python t_daily_index_candidates.py --fetch_index_ts --index_name szcz
python t_daily_index_candidates.py --fetch_index_ts --index_name sz100

##############
# update
# input: none
# output: /home/ryan/DATA/pickle/INDEX_US_HK/*.csv
# dow.csv  nasdqa100.csv  sp400.csv  sp500.csv
python t_daily_get_us_index.py


# Need a running X server. Download 15 times a day.
env DISPLAY=:0  python t_daily_index_candidates.py --fetch_index_wg



######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/hsgt_top10.csv
#
######################################
python t_daily_hsgt.py --fetch_hsgt_top_10

if [ $full_or_daily == "FULL" ]; then
  rm -f /home/ryan/DATA/DAY_Global/AG_MoneyFlow/*.csv
  python t_daily_hsgt.py --fetch_moneyflow_all --force_run  #time consuming
fi


if [ $full_or_daily == "FULL" ]; then
  python t_daily_hsgt.py --fetch_moneyflow_daily  #time consuming (a little)
fi

######################################
#
#output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/basic_todayS.csv, eg. basic_2019-03-04.csv
#
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/yyyy/feature_yyyy_q.csv. eg. 2018/cashflow_2018_4.csv
#   features:  cashflow_2018_4.csv    growth_2018_4.csv      profit_2018_4.csv
#              debtpaying_2018_4.csv  operation_2018_4.csv   report_2018_4.csv
#
#/home/ryan/DATA/pickle/gem.csv  #创业板
#                      hs300.csv ## 沪深300成份股
#                      sme.csv  #中小板
#                      sz50.csv # SZ50成份股
#                      trading_day_2019.csv
#                      ZZ500.csv #中证500成份股
#
######################################
echo "updating quarter fund data, update /home/ryan/DATA/pickle/*.pickle"
echo "updating fundamentals/daily/basic_today.csv"
#python t_daily_fundamentals.py  --fetch_data_all;

######################################
#

######################################
if [ $full_or_daily == "FULL" ]; then
  echo "updating market, security, instrument, A_stock"
  python t_daily_fundamentals.py  --update_get_market;  #/home/ryan/DATA/pickle/market.csv
  python t_daily_fundamentals.py  --update_get_security;   #saved to /home/ryan/DATA/pickle/security.csv #stop work. 20200513

  python t_daily_fundamentals.py  --update_get_instrument;  # /home/ryan/DATA/pickle/instrument.csv
fi

# update forex, get forex yesterday data from td.
#bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN
#scp root@td:/home/ryan/DATA/DAY_Forex_local.tar.gz   /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz
#echo "saved to DAY_Forex_local.tar.gz from td to /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz"

echo "AG stock daily updated!"




######################################
### To update Fenghong required data, have to be run in home/public network on laptop daily.
### BestTV-W works. EMC-Open not work, EMC not work
## Input:
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/forecast/fd_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/fund_hold/fh_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/restricted_lifted/rl_*.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/new_stock/new_stock.pickle
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sh_margin/sh_margin.csv
## /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/sz_margin/sz_margin.csv

## Output:
## profit_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_profit_all.csv"
## forecast_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_forecast_all.csv"
## fundhold_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_fundholding_all.csv"
## xsg_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_xsg_all.csv"
## new_stock_csv = "/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/_new_stock.csv"
######################################
if [ $full_or_daily == "FULL" ]; then
  python t_fenghong.py  --fetch_reference
fi



######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/pro_basic.csv
# #ts_code,symbol,name,area,industry,list_date
######################################
python t_daily_fundamentals_2.py --fetch_pro_basic


######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/pledge/pledge_stat.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/pledge/pledge_detail.csv
# #ts_code,symbol,name,area,industry,list_date
######################################
if [ $full_or_daily == "FULL" ]; then
  python t_daily_fundamentals_2.py --fetch_pledge_stat_detail
fi

######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual/*.csv
#
######################################
#----
# a full fetch. fetch everything.
#----
#python t_daily_fundamentals_2.py --fetch_data_all --force_run #<<< force_run is MUST to fetch history data.

#----
#full fetch. but files that has been updated in 5 days
# (which should also be the most recent Quarter) will not be check again.
#----
# python t_daily_fundamentals_2.py --fetch_data_all  #time consuming.  <<<< DON"T USE THIS. USE force_run

#----
# # fast_fetch fetch
# 1.good stock.  <<< commented now, as #fast_fetch doesn't have content.
# 2.this_q and last_q for ts.express
#----
if [ $full_or_daily == "FULL" ]; then
  echo "daily update tushare pro fundamentals";
  #python t_daily_fundamentals_2.py --fetch_data_all --force_run   #if everything stock/period need be refetch.
  #python t_daily_fundamentals_2.py --fetch_data_all --fast_fetch  #most time, update most recent q.  #time consuming.

  #separate --fetch_data_all
  #output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_*.csv
  #ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv
  python t_daily_fundamentals_2.py --fetch_basic_daily --force_run
  python t_daily_fundamentals_2.py --fetch_info_daily --force_run

  #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/pledge_detail.csv  pledge_stat.csv
  python t_daily_fundamentals_2.py --fetch_pledge_stat_detail --force_run

  #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/changed_name_stocks.csv
  #ts_code,name,start_date,end_date,ann_date,change_reason
  python t_daily_fundamentals_2.py  --fetch_change_name --force_run

  # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/pro_concept.csv
  #ts_code,name,cat_code,cat_name
  python t_daily_fundamentals_2.py --fetch_pro_concept --force_run

  # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/new_share.csv
  #ts_code,sub_code,name,ipo_date,issue_date,amount,market_amount,price,pe,limit_amount,funds,ballot
  python t_daily_fundamentals_2.py --fetch_new_share

  # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/pro_repurchase.csv
  #ts_code,ann_date,end_date,proc,exp_date,vol,amount,high_limit,low_limit
  python t_daily_fundamentals_2.py --fetch_pro_repurchase --force_run

  # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/cctv_news.csv
  #date,title,content
  python t_daily_fundamentals_2.py --fetch_cctv_news --force_run

  #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/pro_basic.csv
  #ts_code,symbol,name,area,industry,list_date
  python t_daily_fundamentals_2.py --fetch_pro_basic --force_run

  #output:
  #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/*.csv
  #ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv
  python t_daily_fundamentals_2.py --fetch_basic_quarterly --force_run

  #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/*.csv
  python t_daily_fundamentals_2.py --fetch_pro_fund --force_run  #pro fundation tables. 10 tables. all stocks
fi



######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_yyyymmdd.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_20200710.csv
#
#####################################
#python t_daily_fundamentals_2.py --fetch_basic_daily
if [ $full_or_daily == "DAILY" ]; then
  python t_daily_fundamentals_2.py --fetch_basic_daily --fast_fetch
  python t_daily_fundamentals_2.py --fetch_info_daily --fast_fetch
  python t_daily_fundamentals_2.py --fetch_new_share
  python t_daily_fundamentals_2.py --fetch_industry_l123
  python t_daily_fundamentals_2.py --fetch_pro_fund --fast_fetch #pro fundation tables. 10 tables.
  python t_daily_fundamentals_2.py --fetch_change_name

  #`fetch lastest quarter fund data, all stocks
  # comment this as in Q3, audit is empty and always fetch everytime as is_cached return False\
#  python t_daily_fundamentals_2.py --fetch_pro_fund --fast_fetch  --force_run
fi


######################################
#
# output:
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_600519.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_600519.csv
#
######################################
# not run because time consuming. 20200518
if [ $full_or_daily == "DAILY" ]; then
   python t_top_10_holders.py --fetch  --stock_global AG --selected  #time consuming
fi


if [ $full_or_daily == "FULL" ]; then
   python t_top_10_holders.py --fetch  --stock_global AG 
fi




# HK Stock update along with US at evening of the day, and exam on the next day morning at 8.00 AM.


######################################
## input: /home/ryan/DATA/pickle/instrument_A.csv, get code from the file
## output: /home/ryan/DATA/DAY_No_Adj/*.csv
## BestTV-W works
######################################
# not run because time consuming. 20200518
if [ $full_or_daily == "FULL" ]; then
  python t_fenghong.py  --fetch_no_adj_data  --force_fetch_data #time consuming
fi



######################################  JAQS stopped working 20190305
# have to run on home, port 8910 not allowed on any public network in office.
# EMC-Open works.  BestTV-W works(faster)
# output:
# /home/ryan/DATA/DAY_JAQS/*.csv
#
######################################
#python t_monthly_jaqs_fundamental.py --fetch_data_all --force_fetch_data


#### 20190102 home thinkpad laptop replaced td to sync all days ###


# HK Stock update along with US at evening of the day, and exam on the next day morning at 8.00 AM.


######################################
## input: /home/ryan/DATA/pickle/instrument_A.csv, get code from the file
## output: /home/ryan/DATA/DAY_No_Adj/*.csv
## BestTV-W works
######################################
# not run because time consuming. 20200518
if [ $full_or_daily == "FULL" ]; then
  python t_fenghong.py  --fetch_no_adj_data  --force_fetch_data #time consuming
fi



######################################  JAQS stopped working 20190305
# have to run on home, port 8910 not allowed on any public network in office.
# EMC-Open works.  BestTV-W works(faster)
# output:
# /home/ryan/DATA/DAY_JAQS/*.csv
#
######################################
#python t_monthly_jaqs_fundamental.py --fetch_data_all --force_fetch_data


#### 20190102 home thinkpad laptop replaced td to sync all days ###
#### this part has been replaced by
# python t_daily_fundamentals_2.py  --fetch_data_all @ haha_data_source ###
# and
# bash -x t_daily_sync_from_ds.sh @ haha_brain

#rsync -avz /hdd2/repo/trading/tushare_ryan  root@td:/root
#rsync -avz /home/ryan/DATA/pickle/instrument_A.csv   root@td:/root/DATA/pickle/instrument_A.csv
#
#rsync -avz root@td:/root/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual
#ssh root@td reboot &


if [ $full_or_daily == "FULL" ]; then
  echo "updating HK/US all stocks daily from tushare "
  #python ~/tushare_ryan/t_fetch_us_hk_bar.py -x HK; #time consuming

  #14000 companies, and tushare no longer have updates since 2020/04/27
  #python ~/tushare_ryan/t_fetch_us_hk_bar.py -x US;
  echo "HK/US daily update completed."
fi

######################################
# /home/ryan/DATA/DAY_Global/FUTU_[HK, SH, SZ, US]/*.csv
######################################
python t_futu_trade.py --fetch_history_bar  --market AG_HOLD
python t_futu_trade.py --fetch_history_bar  --market HK_HOLD
python t_futu_trade.py --fetch_history_bar  --market AG
python t_futu_trade.py --fetch_history_bar  --market HK


######################################
#
######################################
#python ak_share.py  --fetch_after_market

echo "done, script completed"
