t_fetch_us_hk_bar#!/usr/bin/env bash
#exam start
#rm -rf /home/ryan/DATA/result/today.del
#mv  /home/ryan/DATA/result/today ~/DATA/result/today.del
#mkdir  /home/ryan/DATA/result/today

fetch_or_exam=$1
if [ $fetch_or_exam == "FETCH" ]; then
    echo "$0: FETCH"
elif [ $fetch_or_exam == "EXAM" ]; then
    echo "$0: EXAM"
else
    echo "Have to specify FETCH or EXAM. Quiting";
    exit
fi

if [ $fetch_or_exam == "FETCH" ]; then
  cd /home/ryan/tushare_ryan

  python /home/ryan/tushare_ryan/t_daily_update_csv_from_tushare.py;
  python /home/ryan/tushare_ryan/t_daily_get_ag_index_from_tushare.py --fetch_index;

  python /home/ryan/tushare_ryan/t_daily_update_csv_from_stooq.py;
  python /home/ryan/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK  --force_fetch;

  #python t_daily_update_csv_from_stooq.py --force_fetch;
  #python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x US  --force_fetch;
  #python ~/tushare_ryan/t_fetch_us_hk_bar.py --selected -x HK  --force_fetch;

fi


if [ $fetch_or_exam == "EXAM" ]; then
  ################################################
  #input:  /home/ryan/DATA/DAY_Global/CH|KG..|/
  #output:  /home/ryan/DATA/result/key_points_AG_today_selected.csv
  ################################################
  ## add --force_run to force run
  #python t_daily_get_key_points.py  -x CH  --calc_base #slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_CH.csv
  #python t_daily_get_key_points.py  -x CH  --calc_today  #fast. output: /home/ryan/DATA/result/key_points_CH_today.csv
  #python t_daily_get_key_points.py  -x CH  --today_selection #fast. output: /home/ryan/DATA/result/key_points_CH_today_selected.csv


  #python t_daily_get_key_points.py  -x MG  --calc_base
  #python t_daily_get_key_points.py  -x MG  --calc_today
  #python t_daily_get_key_points.py  -x MG  --today_selection

  #-----------  FIB ---------------------------#
    ########### AG, AG_INDEX fib
    # /home/ryan/DATA/result/selected/ag_index_fib.csv
    python t_fibonacci.py --begin_date "20180101"  --min_sample=500 -x AG_INDEX --selected --save_fig

    # /home/ryan/DATA/result/selected/ag_fib.csv
    python t_fibonacci.py --begin_date "20180101"  --min_sample=500 -x AG --selected --save_fig


    ############ US, US_INDEX, HK fib
    #/home/ryan/DATA/result/selected/us_index_fib.csv
    python t_fibonacci.py --begin_date "20180101"  --min_sample=500 -x US_INDEX --selected --save_fig

    #/home/ryan/DATA/result/selected/us_fib.csv
    python t_fibonacci.py --begin_date "20180101"  --min_sample=500 -x US --selected --save_fig

    #/home/ryan/DATA/result/selected/hk_fib.csv
    python t_fibonacci.py --begin_date "20180101"  --min_sample=500 -x HK --selected --save_fig



  #----------------- KP ---------------------------#
    ########## AG KP
    #output: /home/ryan/DATA/result/selected/key_points_ag.csv
    python t_daily_get_key_points.py  -x AG  --selected --calc_base
    #output: /home/ryan/DATA/result/selected/key_points_ag_today.csv
    python t_daily_get_key_points.py  -x AG  --selected --calc_today  --force
    #/home/ryan/DATA/result/selected/key_points_ag_today_selected.csv
    python t_daily_get_key_points.py  -x AG  --selected --today_selection  --force

    ########### US KP
    #output: /home/ryan/DATA/result/selected/key_points_us.csv
    python t_daily_get_key_points.py  -x US  --selected --calc_base
    #output: /home/ryan/DATA/result/selected/key_points_us_today.csv
    python t_daily_get_key_points.py  -x US  --selected --calc_today --force
    #/home/ryan/DATA/result/selected/key_points_us_today_selected.csv
    python t_daily_get_key_points.py  -x US  --selected --today_selection  --force

    ########### HK KP
    #/home/ryan/DATA/result/selected/key_points_hk.csv
    python t_daily_get_key_points.py  -x HK  --selected --calc_base
    #/home/ryan/DATA/result/selected/key_points_hk_today.csv
    python t_daily_get_key_points.py  -x HK  --selected --calc_today  --force
    python t_daily_get_key_points.py  -x HK  --selected --today_selection  --force


  #--------------- PV ---------------------------#
    ############ US pv
    #/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_us.csv
    python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x US --selected

    ############ HK pv
    #/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_hk.csv
    python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x HK --selected

    #/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_ag.csv
    python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x AG --selected



  #--------------- Yunxian ---------------------------#
    ############ US 
    #/home/ryan/DATA/result/selected/us_junxian_barstyle.csv
    python t_daily_junxian_barstyle.py -x US --selected
    python t_daily_junxian_barstyle.py -x US --selected --show_result


    ############ HK 
    #/home/ryan/DATA/result/selected/hk_junxian_barstyle.csv
    python t_daily_junxian_barstyle.py -x HK --selected
    python t_daily_junxian_barstyle.py -x HK --selected --show_result


    ############ AG
    #/home/ryan/DATA/result/selected/ag_junxian_barstyle.csv
    python t_daily_junxian_barstyle.py -x AG --selected
    python t_daily_junxian_barstyle.py -x AG --selected --show_result




 #----------- curve_shape -----------------#
    #output: /home/ryan/DATA/result/selected/us_curve_shape.csv
    python t_double_bottom.py --save_fig  -x US  --min_sample 90 --selected
    python t_double_bottom.py --save_fig  -x HK  --min_sample 90 --selected
    python t_double_bottom.py --save_fig  -x AG  --min_sample 90 --selected


  python t_summary.py  --select  --action=generate_report

  #python t_summary.py -x HK --action generate_report

  #this is local run
  #python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x MG
fi
