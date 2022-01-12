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


rsync -avzt   ${source_host}:/home/ryan/no_hup_run_exam_ag.log  /home/ryan/
rsync -avzt   ${source_host}:/home/ryan/no_hup_run_exam_ushk.log  /home/ryan/

rsync -avzt   ${source_host}:/home/ryan/DATA/result/  /home/ryan/DATA/result/
rsync -avzt   ${source_host}:/home/ryan/DATA/result/*.csv  /home/ryan/DATA/result/
rsync -avzt   ${source_host}:/home/ryan/DATA/result/*.txt  /home/ryan/DATA/result/
rsync -azt   ${source_host}:/home/ryan/DATA/result/today/  /home/ryan/DATA/result/today/
rsync -azt   ${source_host}:/home/ryan/DATA/result/wei_pan_la_sheng/  /home/ryan/DATA/result/wei_pan_la_sheng/
rsync -azt   ${source_host}:/home/ryan/DATA/result/fib_plot/  /home/ryan/DATA/result/fib_plot/
rsync -azt   ${source_host}:/home/ryan/DATA/result/curv_plot/  /home/ryan/DATA/result/curv_plot/
rsync -azt   ${source_host}:/home/ryan/DATA/result/selected/  /home/ryan/DATA/result/selected/
rsync -azt   ${source_host}:/home/ryan/DATA/result/pv_2/  /home/ryan/DATA/result/pv_2/
#rsync -az   ${source_host}:/home/ryan/DATA/result/jaqs/  /home/ryan/DATA/result/jaqs/
