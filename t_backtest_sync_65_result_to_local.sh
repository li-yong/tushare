#!/usr/bin/env bash


source_host=$1

if test $# -eq 0; then
  echo 'quit';
  echo "Have to specify \$source_host. Quiting";
  exit;
fi;

if [[ ${source_host} == "haha_65" ]]; then
    echo "haha_65";
fi

if [[ ${source_host} == "haha_power" ]]; then
    echo "haha_power";
fi


#scp ${source_host}:/home/ryan/DATA/result/today/*.csv  /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/report_2018-11-*.txt /home/ryan/DATA/result/

#scp ${source_host}:/home/ryan/DATA/result/fenghong_score.csv /home/ryan/DATA/result/

#scp ${source_host}:/home/ryan/DATA/result/industry_top.csv /home/ryan/DATA/result/
#scp ${source_host}:/home/ryan/DATA/result/area_top.csv /home/ryan/DATA/result/

#scp ${source_host}:/home/ryan/DATA/result/today/announcement.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/talib_pattern.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/fundamentals.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/price_mfi_div.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/price_rsi_div.csv /home/ryan/DATA/result/today/

#scp ${source_host}:/home/ryan/DATA/result/today/talib_and_pv_db_buy_filtered.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/talib_and_pv_db_sell_filtered.csv /home/ryan/DATA/result/today/
#scp ${source_host}:/home/ryan/DATA/result/today/talib_and_pv_no_db_filter.csv /home/ryan/DATA/result/today/

#scp ${source_host}:/home/ryan/DATA/result/latest_fundamental_peg_selected.csv /home/ryan/DATA/result/
#scp ${source_host}:/home/ryan/DATA/result/fundamental_peg.csv /home/ryan/DATA/result/

#scp ${source_host}:/home/ryan/DATA/result/Fundamental_Quarter_Report_2018_1.csv /home/ryan/DATA/result/
#scp ${source_host}:/home/ryan/DATA/result/fundamental_peg_2018_1_selected.csv /home/ryan/DATA/result/
#scp ${source_host}:/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv /home/ryan/DATA/result/


rsync -avz   ${source_host}:/home/ryan/no_hup_run_exam_ag.log  /home/ryan/
rsync -avz   ${source_host}:/home/ryan/no_hup_run_exam_ushk.log  /home/ryan/

rsync -avz   ${source_host}:/home/ryan/DATA/result/  /home/ryan/DATA/result/
rsync -avz   ${source_host}:/home/ryan/DATA/result/*.csv  /home/ryan/DATA/result/
rsync -avz   ${source_host}:/home/ryan/DATA/result/*.txt  /home/ryan/DATA/result/
rsync -az   ${source_host}:/home/ryan/DATA/result/today/  /home/ryan/DATA/result/today/
rsync -az   ${source_host}:/home/ryan/DATA/result/wei_pan_la_sheng/  /home/ryan/DATA/result/wei_pan_la_sheng/
rsync -az   ${source_host}:/home/ryan/DATA/result/fib_plot/  /home/ryan/DATA/result/fib_plot/
rsync -az   ${source_host}:/home/ryan/DATA/result/curv_plot/  /home/ryan/DATA/result/curv_plot/
rsync -az   ${source_host}:/home/ryan/DATA/result/selected/  /home/ryan/DATA/result/selected/
rsync -az   ${source_host}:/home/ryan/DATA/result/pv_2/  /home/ryan/DATA/result/pv_2/
#rsync -az   ${source_host}:/home/ryan/DATA/result/jaqs/  /home/ryan/DATA/result/jaqs/

if [[ ${source_host} == "haha_65" ]]; then
  rsync -avz  haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/
else
  rsync -avz  ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/
fi


# sync smaller folders first
if [[ ${source_host} == "haha_65" ]]; then
  rsync -avzL  haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/
  rsync -avzL  haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/
  rsync -az   haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/
  rsync -az   haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/
  rsync -az   haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/
else
  rsync -avzL  ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/
  rsync -avzL  ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/
  rsync -azL   ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/
  rsync -azL   ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/
  rsync -azL   ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/
  rsync -azL   ${source_host}:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/
fi

###################################################################
###
### This file too big, and not necessarily used in haha_brain.
### Copy to haha_brain only if the ${source_host} need be replaced.
### too big, 6GB. Don't sync jaqs folder, it is directory in remote(${source_host}), link in local(haha)
###################################################################
#rsync -az   ${source_host}:/home/ryan/DATA/result/jaqs/ts_all.pickle  /home/ryan/DATA/result/jaqs/
#rsync -az   ${source_host}:/home/ryan/DATA/result/jaqs/jaqs_all.pickle  /home/ryan/DATA/result/jaqs/



###################################################################
#
#File list that need copy from ${source_host} --> haha_brain
#
###################################################################
#
#/home/ryan/DATA/pickle/Stock_Fundamental/fenghong/profit_analysis.csv
#/home/ryan/DATA/result/fenghong_score.csv  #code 002601
#
#
#/home/ryan/DATA/result/today/announcement.csv
#
#/home/ryan/DATA/result/fundamental.csv
#
#/home/ryan/DATA/result/latest_fundamental_quarter.csv  --> Fundamental_Quarter_Report_2018_3.csv (actually is 2018_4 content, script generated automatically)
#
#/home/ryan/DATA/result/latest_fundamental_day.csv  --> /home/ryan/DATA/result/today/fundamentals.csv
#
#
#/home/ryan/DATA/result/fundamental_peg.csv
#
#/home/ryan/DATA/result/latest_fundamental_day.csv
#/home/ryan/DATA/result/latest_fundamental_quarter.csv
#
#
#/home/ryan/DATA/result/latest_fundamental_peg.csv --> /home/ryan/DATA/result/fundamental_peg_2018_4.csv
#/home/ryan/DATA/result/latest_fundamental_peg_selected.csv --> /home/ryan/DATA/result/fundamental_peg_2018_4_selected.csv
#
#
#/home/ryan/DATA/result/industry_top.csv
#/home/ryan/DATA/result/area_top.csv
#
