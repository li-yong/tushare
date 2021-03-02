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
#rsync -avz   /home/ryan/tushare_ryan/  haha_data_source:/home/ryan/tushare_ryan/
ssh haha_data_source "cd ~/tushare_ryan;  git pull"

#us hk indices are get from WikiPedia. Download to haha_brain, then sync to haha_power
rsync -avz /home/ryan/DATA/pickle/INDEX_US_HK/ ${source_host}:/home/ryan/DATA/pickle/INDEX_US_HK

#TradingView are download to haha_brain manually on Chrome.
rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/TradingView/ ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/TradingView
#rsync -avz /home/ryan/DATA/result/*.csv ${source_host}:/home/ryan/DATA/result/
#rsync -avz /home/ryan/DATA/result/*.txt ${source_host}:/home/ryan/DATA/result/
#rsync -avz /home/ryan/DATA/result/today ${source_host}:/home/ryan/DATA/result/
#rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
#rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2
#rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source
#rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest
#rsync -avz /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/  ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/

#v: sync recursivly
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/akshare/  /home/ryan/DATA/pickle/Stock_Fundamental/akshare/



######################################
#
# haha_data_source ---> haha_brain
#
######################################

rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/AG/  /home/ryan/DATA/DAY_Global/AG/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/AG_INDEX/  /home/ryan/DATA/DAY_Global/AG_INDEX/
#rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/US_INDEX/  /home/ryan/DATA/DAY_Global/US_INDEX/ #no longer there
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/AG_MoneyFlow/  /home/ryan/DATA/DAY_Global/AG_MoneyFlow/


rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/akshare/HK/  /home/ryan/DATA/DAY_Global/akshare/HK/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/akshare/US/  /home/ryan/DATA/DAY_Global/stooq/US/

rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/stooq/US/  /home/ryan/DATA/DAY_Global/stooq/US/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/stooq/US_INDEX/  /home/ryan/DATA/DAY_Global/stooq/US_INDEX/

#US/HK has too long list to fit in haha_data_source (1GB memory)
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/HK/  /home/ryan/DATA/DAY_Global/HK/
rsync -avz   ${source_host}:/home/ryan/DATA/DAY_Global/US/  /home/ryan/DATA/DAY_Global/US/
#rsync -avz   haha_power:/home/ryan/DATA/DAY_Global/CH/  /home/ryan/DATA/DAY_Global/CH/
#rsync -avz   haha_power/ryan/DATA/DAY_Global/KG/  /home/ryan/DATA/DAY_Global/KG/

#rsync -avz   haha_power/ryan/DATA/DAY_Global/MG/  /home/ryan/DATA/DAY_Global/MG/


rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/



rsync -avz ${source_host}:/home/ryan/DATA/pickle/daily_update_source/  /home/ryan/DATA/pickle/daily_update_source/
rsync -avz ${source_host}:/home/ryan/DATA/announcement/  /home/ryan/DATA/announcement/
rsync -avz ${source_host}:/home/ryan/DATA/DAY_No_Adj/  /home/ryan/DATA/DAY_No_Adj/
rsync -avz ${source_host}:/home/ryan/DATA/DAY_JAQS/  /home/ryan/DATA/DAY_JAQS/

rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/  /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/


rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/


#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -E "201[7-9]"`;
#do
#    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -v 201`;
#do
#    echo "sync file to symbol link /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done



#rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181232/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20181231/
#rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/
#rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190630/
#rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190930/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20191231/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20200331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20200331/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/ /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20190331/


#rsync -avz ${source_host}:/home/ryan/DATA/pickle/DOW_SP/  /home/ryan/DATA/pickle/DOW_SP/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/Forbes/  /home/ryan/DATA/pickle/Forbes/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/INDEX/  /home/ryan/DATA/pickle/INDEX/
rsync -avz ${source_host}:/home/ryan/DATA/pickle/INDEX_US_HK/  /home/ryan/DATA/pickle/INDEX_US_HK/

rsync -avz ${source_host}:/home/ryan/DATA/pickle/gem.csv  /home/ryan/DATA/pickle/gem.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/instrument_A.csv  /home/ryan/DATA/pickle/instrument_A.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/instrument.csv  /home/ryan/DATA/pickle/instrument.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/security.csv  /home/ryan/DATA/pickle/security.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/trading_day_2019.csv  /home/ryan/DATA/pickle/trading_day_2019.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/hs300.csv  /home/ryan/DATA/pickle/hs300.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/market.csv  /home/ryan/DATA/pickle/market.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/sme.csv  /home/ryan/DATA/pickle/sme.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/sz50.csv  /home/ryan/DATA/pickle/sz50.csv
rsync -avz ${source_host}:/home/ryan/DATA/pickle/ZZ500.csv  /home/ryan/DATA/pickle/ZZ500.csv


rsync -avz ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/

