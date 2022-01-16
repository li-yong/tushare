#!/usr/bin/env bash

source_host=$1

if test $# -eq 0; then
  echo 'quit';
  echo "Have to specify \$source_host. Quiting";
  exit;
fi;

if [[ ${source_host} == "haha_data_source" ]]; then
    echo "haha_65";
fi

if [[ ${source_host} == "haha_power" ]]; then
    echo "haha_power";
fi



######################################
#
# haha_brain ---> haha_data_source
#
######################################
#rsync -avzt   /home/ryan/tushare_ryan/  haha_data_source:/home/ryan/tushare_ryan/
#ssh haha_data_source "cd ~/tushare_ryan;  git pull"

rsync -avzt ${source_host}:/home/ryan/DATA/pickle/daily_update_source/  /home/ryan/DATA/pickle/daily_update_source/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/

#us hk indices are get from WikiPedia. Download to haha_brain, then sync to haha_power
#haha_brain need run: python t_daily_get_us_index.py
rsync -avzt /home/ryan/DATA/pickle/INDEX_US_HK/ ${source_host}:/home/ryan/DATA/pickle/INDEX_US_HK

#TradingView are download to haha_brain manually on Chrome.
rsync -avzt /home/ryan/DATA/pickle/Stock_Fundamental/TradingView/ ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/TradingView
#rsync -avzt /home/ryan/DATA/result/*.csv ${source_host}:/home/ryan/DATA/result/
#rsync -avzt /home/ryan/DATA/result/*.txt ${source_host}:/home/ryan/DATA/result/
#rsync -avzt /home/ryan/DATA/result/today ${source_host}:/home/ryan/DATA/result/



#v: sync recursivly
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/akshare/  /home/ryan/DATA/pickle/Stock_Fundamental/akshare/
rsync -avzt /home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/ {source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/WuGuiLiangHua/

######################################
#
# haha_data_source ---> haha_brain
#
######################################

rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/AG/  /home/ryan/DATA/DAY_Global/AG/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/AG_qfq/  /home/ryan/DATA/DAY_Global/AG_qfq/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/AG_INDEX/  /home/ryan/DATA/DAY_Global/AG_INDEX/
#rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/US_INDEX/  /home/ryan/DATA/DAY_Global/US_INDEX/ #no longer there
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/AG_MoneyFlow/  /home/ryan/DATA/DAY_Global/AG_MoneyFlow/

rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/FUTU_* /home/ryan/DATA/DAY_Global/


rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/akshare/HK/  /home/ryan/DATA/DAY_Global/akshare/HK/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/akshare/US/  /home/ryan/DATA/DAY_Global/stooq/US/

rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/stooq/US/  /home/ryan/DATA/DAY_Global/stooq/US/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/stooq/US_INDEX/  /home/ryan/DATA/DAY_Global/stooq/US_INDEX/

#US/HK has too long list to fit in haha_data_source (1GB memory)
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -avzt   ${source_host}:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/
#rsync -avzt   haha_power:/home/ryan/DATA/DAY_Global/CH/  /home/ryan/DATA/DAY_Global/CH/
#rsync -avzt   haha_power/ryan/DATA/DAY_Global/KG/  /home/ryan/DATA/DAY_Global/KG/

#rsync -avzt   haha_power/ryan/DATA/DAY_Global/MG/  /home/ryan/DATA/DAY_Global/MG/

###################
# Essentical
###################
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/


#######################
#
######################
rsync -avzt ${source_host}:/home/ryan/DATA/announcement/  /home/ryan/DATA/announcement/
rsync -avzt ${source_host}:/home/ryan/DATA/DAY_No_Adj/  /home/ryan/DATA/DAY_No_Adj/
rsync -avzt ${source_host}:/home/ryan/DATA/DAY_JAQS/  /home/ryan/DATA/DAY_JAQS/

rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/  /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/

rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/


#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -E "201[7-9]"`;
#do
#    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -v 201`;
#do
#    echo "sync file to symbol link /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181232/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181231/
#rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/
#rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/
#rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20201231/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20201231/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210630/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210630/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210930/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210930/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20211231/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20211231/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20220331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20220331/
rsync -avztL ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/

#rsync -avzt ${source_host}:/home/ryan/DATA/pickle/DOW_SP/  /home/ryan/DATA/pickle/DOW_SP/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Forbes/  /home/ryan/DATA/pickle/Forbes/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/INDEX/  /home/ryan/DATA/pickle/INDEX/
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/INDEX_US_HK/  /home/ryan/DATA/pickle/INDEX_US_HK/

rsync -avzt ${source_host}:/home/ryan/DATA/pickle/gem.csv  /home/ryan/DATA/pickle/gem.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/instrument_A.csv  /home/ryan/DATA/pickle/instrument_A.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/instrument.csv  /home/ryan/DATA/pickle/instrument.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/security.csv  /home/ryan/DATA/pickle/security.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/trading_day_2019.csv  /home/ryan/DATA/pickle/trading_day_2019.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/trading_day_2021.csv  /home/ryan/DATA/pickle/trading_day_2021.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/hs300.csv  /home/ryan/DATA/pickle/hs300.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/market.csv  /home/ryan/DATA/pickle/market.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/sme.csv  /home/ryan/DATA/pickle/sme.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/sz50.csv  /home/ryan/DATA/pickle/sz50.csv
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/ZZ500.csv  /home/ryan/DATA/pickle/ZZ500.csv


rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/


rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source
rsync -avzt ${source_host}:home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest
rsync -avzt ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/
