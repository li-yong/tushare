#!/usr/bin/env bash
#exam start
#rm -rf /home/ryan/DATA/result/today.del
#mv  /home/ryan/DATA/result/today ~/DATA/result/today.del
#mkdir  /home/ryan/DATA/result/today



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

cd /home/ryan/DATA/result
git pull

cd /home/ryan/tushare_ryan
bash -x clean_result_dir.sh

#@todo: focus on date after 2005? when the gu quan fengzhi starts
#@todo:  each fields name and meaning, adding to the code py script.

#### Process AG ########
#mysqldump -uroot -padmin888.@_@  ryan_stock_db order_tracking_stock > order_tracking_stock.sql.backup_`hostname`_`date +"%Y-%m-%d_%H_%M_%S"`

python del.py

##########################
#input:
#    /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_20200826.csv
#    /home/ryan/DATA/pickle/daily_update_source/ag_daily_20200826.csv
#output: /home/ryan/DATA/result/pv_2/20200826
# die_ting.csv	   pe_top_30p.csv    stable_price_volume.csv  vcp.csv			   volume_ratio_top_20p.csv  zhangting_volume_ration_lt_1.csv
#pe_bottom_30p.csv  pocket_pivot.csv  step1.out.csv	      volume_ratio_bottom_10p.csv  zhang_ting.csv
##########################
python t_daily_pattern_Hit_Price_Volume_2.py


########################################################  Step1 Others  ################################################

##########################
#input:
#    /home/ryan/DATA/announcement/reg/list/2019/2019-01-28.csv
#    /home/ryan/DATA/announcement/sse/list/2019/2019-01-28.csv
#output: /home/ryan/DATA/result/today/announcement.csv
##########################
if [ $full_or_daily == "FULL" ]; then
     echo "NOT RUN"
     #rm -f /home/ryan/DATA/result/today/announcement.csv
     #python t_daily_announcement.py
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "NOT RUN"
    #python t_daily_announcement.py
fi




################################################ OBSOLATED, JAQS
#input: "//home/ryan/DATA/DAY_JAQS/*.csv"
#output: "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv".
# header: year_quarter,code,name,close,symbol,ps,ps_ttm,pe,pb,price_div_dps,float_mv,net_assets,pcf_ncf,pcf_ncfttm,pcf_ocf,pcf_ocfttm
################################################
if [ $full_or_daily == "FULL" ]; then
    echo "NOT RUN"
    #rm -f /home/ryan/DATA/result/jaqs_quarterly_fundamental.csv
    #python t_monthly_jaqs_fundamental.py --calc_quartly_report  --force_run  #<<< 16 min
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "NOT RUN"
    #python t_monthly_jaqs_fundamental.py --calc_quartly_report
fi


#python t_monthly_jaqs_fundamental.py --calc_quartly_report --force_run

################################################
#input: "/home/ryan/DATA/DAY_No_Adj/*.csv"
#         /home/ryan/DATA/pickle/Stock_Fundamental/fenghong/*.csv
#output: analyze_one:"/home/ryan/DATA/result/fenghong_profit_analysis.csv"
#        analyze_two: /home/ryan/DATA/result/fenghong_score.csv'
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fenghong_profit_analysis.csv
    rm -f /home/ryan/DATA/result/fenghong_score.csv

    python t_fenghong.py --analyze_one --force_run  #<<< 3 min
    python t_fenghong.py --analyze_two --force_run  #<<< 1 min

fi

if [ $full_or_daily == "DAILY" ]; then
    echo "SKIP"
    #python t_fenghong.py --analyze_one
    #python t_fenghong.py --analyze_two
fi


######################################################## Step2 FUND2 ################################################
################################################
#input:
#output:
#
################################################
#bash -x t_quarterly_fund_2.sh  #<<< stuck

#########################
#input:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/hsgt_top10.csv
#output : /home/ryan/DATA/result/hsgt_top_10_selected.csv
#########################
python t_daily_hsgt.py --analyze_hsgt


#########################
#input:  /home/ryan/DATA/DAY_Global/AG_MoneyFlow/*.csv
#output : /home/ryan/DATA/tmp/moneyflow_ana/history_moneyflow_hit.csv
#         /home/ryan/DATA/tmp/moneyflow_ana/today_moneyflow_hit.csv

#         /home/ryan/DATA/tmp/moneyflow_ana/mf_today_snap.csv

#########################
if [ $full_or_daily == "FULL" ]; then
  #/home/ryan/DATA/tmp/moneyflow_ana/mf_today_snap.csv #len 1683
  #/home/ryan/DATA/result/today/mf_today_top5_large_amount.csv #len 5
  python t_daily_hsgt.py --analyze_moneyflow --mf_ana_pre_days 3 --mf_ana_test_hold_days 5
fi

#/home/ryan/DATA/tmp/moneyflow_ana/mf_today_snap.csv Len 63
#/home/ryan/DATA/result/today/mf_today_top5_large_amount.csv Len 5
python t_daily_hsgt.py --analyze_moneyflow --mf_ana_pre_days 3 --mf_ana_test_hold_days 5 --mf_ana_prime_stock

#/home/ryan/DATA/tmp/moneyflow_ana/mf_today_snap_selected.csv Len 35
#/home/ryan/DATA/result/selected/mf_today_top5_large_amount.csv Len 5
python t_daily_hsgt.py --analyze_moneyflow --mf_ana_pre_days 3 --mf_ana_test_hold_days 5 --selected

######################### merge_local_basic
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_*.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
#########################

if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv; # WARNNING, will spend hours to generate.
    python t_daily_fundamentals_2.py --merge_local_basic --force_run  # <<< ??? hour. update weekly.
fi

#/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
if [ $full_or_daily == "DAILY" ]; then
  python t_daily_fundamentals_2.py --merge_local_basic --fast_fetch ## only merge last trading days to. 50 sec
fi


######################### merge_local
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/yyyymmdd/*.csv  <<< updated. 20190218. This file have dup records if tushare source has.
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv [fina_mainbz_sum.csv,balancesheet.csv,dividend.csv,fina_audit.csv,fina_mainbz.csv,forecast.csv]
#########################

if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv; # WARNNING, will spend hours to generate.
    python t_daily_fundamentals_2.py --merge_local --force_run  # <<< 2.5 hour. update everyday, since its output is other matrics input.;
fi


#03_07 23:09:00 file updated in 3 days, not process. /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/income.csv
if [ $full_or_daily == "DAILY" ]; then
    echo "SKIP"
    #python t_daily_fundamentals_2.py --merge_local --fast_fetch
fi

#########################  Get ADL
# input:~/DATA/DAY_Global/AG, ~/DATA/DAY_Global/AG_INDEX
# output:
#/home/ryan/DATA/result/adl/ag_index_adl.csv
#/home/ryan/DATA/result/adl/adl_perc_top_5_stocks.csv
#/home/ryan/DATA/result/adl/vol_perc_top_5_stocks.csv
#/home/ryan/DATA/result/adl/amt_perc_top_5_stocks.csv

#########################
python t_daily_adl_trin.py -n 14


#########################  merge_individual
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*.csv
#This step is very quickly. 20190219
#########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*.csv  # dangerous, will spend hours to generate
    python t_daily_fundamentals_2.py --merge_individual --force_run  #<<<< 1 hour
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "skipped"
    #python t_daily_fundamentals_2.py --merge_individual --fast_fetch #@todo, bug: only merge the latest day, but not the days which has not been in the target.
fi



######################### fund analysis
#input:

#output :
# /home/ryan/DATA/result/basic_summary/basic_fund_fund2_20200831.csv  <<< merged fund1 and fund2 daily fund
# header: code,ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv,volume_ratio_perc_rank,total_mv_perc_rank,circ_mv_perc_rank,pe_perc_rank,pe_ttm_perc_rank,ps_ttm_perc_rank,turnover_rate_f_perc_rank,name,industry,area,pe_x,outstanding,totals,totalAssets,liquidAssets,fixedAssets,reserved,reservedPerShare,esp,bvps,pb_x,timeToMarket,undp,perundp,rev,profit,gpr,npr,holders

#/home/ryan/DATA/result/fund_analysis.csv <<<< fund2 analysis on today's basic
# head: code,symbol,name,area,industry,list_date,on_market_days,ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv,volume_ratio_perc_rank,total_mv_perc_rank,circ_mv_perc_rank,pe_perc_rank,pe_ttm_perc_rank,ps_ttm_perc_rank,turnover_rate_f_perc_rank,name_x,industry_x,area_x,pe_x,outstanding,totals,totalAssets,liquidAssets,fixedAssets,reserved,reservedPerShare,esp,bvps,pb_x,timeToMarket,undp,perundp,rev,profit,gpr,npr,holders,name_x,roe,roa,total_profit,net_profit,free_cashflow,total_revenue,total_assets,total_liab,ebit,ebitda,netdebt,fcff,fcfe,name_year1,roe_year1,roa_year1,total_profit_year1,net_profit_year1,free_cashflow_year1,total_revenue_year1,total_assets_year1,total_liab_year1,ebit_year1,ebitda_year1,netdebt_year1,fcff_year1,fcfe_year1,ev,ev_ebitda_ratio,ev_ebitda_ratio_rank,total_mv_net_profit_ratio,total_mv_net_profit_ratio_rank

#########################
python t_daily_fundamentals_2.py  --generate_today_fund1_fund2_stock_basic

######################### sum_mainbz
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_p.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_sum.csv
#########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_sum.csv  # dangerous, will spend hours to generate
    python t_daily_fundamentals_2.py  --sum_mainbz --force_run  #<<< 20 min
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "skipped"
    #python t_daily_fundamentals_2.py  --sum_mainbz
fi



######################### merge_quarterly
#merge all 9 tables to one table, "merged_all_"+end_date+".csv"
#input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20190630.csv  <<< These files removed the dup records.
#                                                                             the file has not on-market will not show up.
#        /home/ryan/DATA/result/jaqs/ts_all.pickle
########################
#merge all quarter data

#this will update jaqs_all.pickle, and generate a place holder ts_all.pickle
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/result/jaqs/jaqs_all.pickle.  JAQS is DEAD.
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/*.csv
    python t_daily_fundamentals_2.py  --merge_quarterly --big_memory  --force_run # <<<  2 hour . jaqs_all.pickle re-generated, while ts_all.pickle is invalid.

    #generate real ts_all.pickle based on the merged/*.csv just generated.
    rm -f /home/ryan/DATA/result/jaqs/ts_all.pickle
    python t_daily_fundamentals_2.py --big_memory #  <<< 5 min ,  ts_all.pickle is valid now. geneated from --merge_quarterly output.
fi

#only merge 1q before data. Use when the script runs stable.
if [ $full_or_daily == "DAILY" ]; then
    echo "SKIP"
    #python t_daily_fundamentals_2.py  --merge_quarterly --big_memory --fast_fetch  #<<< only merage current, e.g. 20181231
fi

######################### extract_latest
#
#input:source/*.csv
#output: source/latest/*.csv
##########################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/*.csv
    python t_daily_fundamentals_2.py  --extract_latest --force_run #<<< 20 min
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --extract_latest
fi

######################### disclosure_date_notify_day
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv
######################
if [ $full_or_daily == "FULL" ]; then
        #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv
    python t_daily_fundamentals_2.py  --disclosure_date_notify_day 15 --force_run # <<<< 1 sec
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py  --disclosure_date_notify_day 15 --force_run #refresh every run.
fi



######################### percent_mainbz_f
#
#input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_p.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv <<<  ts_code,name,end_date,bz_cnt,perc_cost,perc_profit,perc_sales,bz_cost,bz_item,bz_profit,bz_sales,curr_type
######################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv
    python t_daily_fundamentals_2.py  --percent_mainbz_f --force_run  #<<<< 4 min
fi

if [ $full_or_daily == "DAILY" ]; then
  echo "SKIP"
  #python t_daily_fundamentals_2.py  --percent_mainbz_f
fi


######################### industry_top
#
#input:
#         /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/fina_mainbz_p.csv <<< industry_top_mainbz_profit
#         /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_mainbz_percent.csv <<<industry_top_mv_eps

#output:  /home/ryan/DATA/result/industry_top_mainbz_profit.csv
#         /home/ryan/DATA/result/industry_top_mv_eps.csv
######################
if [ $full_or_daily == "FULL" ]; then
    python t_daily_fundamentals_2.py --industry_top --force_run
fi

if [ $full_or_daily == "DAILY" ]; then
    python t_daily_fundamentals_2.py --industry_top
fi


######################### beneish
#input: merged/*.csv
#output:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/*.csv
#########################
if [ $full_or_daily == "FULL" ]; then
  python t_yearly_beneish.py
fi


######################### analyze
#input: merged/*.csv
#output:step1: report/step1/rpt_"+end_date+".csv", "ts_code,name,, stopProcess,bonus cnt, garbage cnt, bonusReason, garbageReason,curAssliaM"
#       step2: report/step2/rpt_"+end_date+"2.csv", "ts_code,name,score,garbageReason,score*"
#       step3: report/step3/rpt_"+end_date+"3.csv", "ts_code,name,sos,score,garbageReason,score*"
#       step4: report/step4/multiple_years_score.csv, "ts_code,name,score_over_years,score_avg,number_in_top_30,201806,201803..."
#       step5: report/step5/multiple_years_score.csv. "scoreA,ts_code,name,score_over_years,score_avg,number_in_top_30,201806,201803..."
#       step6: report/step6/multiple_years_score.csv ,  multiple_years_score_selected.csv. "code,name,scoreA,V_C_P,ValuePrice,CurrentPrice"
#       step7: report/step7/verify_fund_increase.csv, code	name	score_over_years	score_avg	number_in_top_30	inc201806
#       step8: report/step8/multiple_years_score.csv ,  code,name,scoreA,V_C_P,ValuePrice,CurrentPrice,ktr_cnt_win,ktr_win_p,ktr_inc_avg,name_stock_basics,score_over_years,score_avg,number_in_top_30,inc201806,201806,
#              report/step8/multiple_years_score_selected.csv,  code,name,scoreA,V_C_P,ValuePrice,CurrentPrice,ktr_cnt_win,ktr_win_p,ktr_inc_avg,name_stock_basics,score_over_years,score_avg,number_in_top_30,inc201806,201806,
#
#########################
#run this at 01/31(Q4), 04/31(Q1), 07/31(Q2), 10/31(Q3). fully and daily are different sets.
# with --big_memory, these two files will be updated. These two files should have been update in above merge_quarterly step.
#      /home/ryan/DATA/result/jaqs/jaqs_all.pickle  << everytime when --big_memory specified.  JAQS is DEAD.
#      /home/ryan/DATA/result/jaqs/ts_all.pickle  << everytime when --big_memory specified.

if [ $full_or_daily == "FULL" ]; then
    #rm -fr /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step[1-8]
    python t_daily_fundamentals_2.py  --analyze  --fully_a --big_memory --force_run  # <<<<  18 hour !!!
fi

if [ $full_or_daily == "DAILY" ]; then
    #rm -fr /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step[4-8]
    # 30min. force_run is required.
    #force_run is required because it calculated the dymanic changing data that keep updating. eg. one 20190308, the data is not fixed
    #of 20181231, the daily run keeps calculate rpt_20181231 for every step.
    python t_daily_fundamentals_2.py  --analyze  --daily_a --big_memory
fi


###################
#input: fund_base_report+"/step1/rpt_"+year_q[ann_date]+".csv"
#
#output:
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/white_horse.csv
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/hen_cow.csv
#       /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/freecashflow_price_ratio.csv
###################
if [ $full_or_daily == "FULL" ]; then
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/white_horse.csv
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/hen_cow.csv
    #rm -f /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/freecashflow_price_ratio.csv
    python t_daily_fundamentals_2.py  --wh_hencow_fcf --force_run  # <<<< 1 min
fi


if [ $full_or_daily == "DAILY" ]; then
    echo "SKIP"
    #python t_daily_fundamentals_2.py  --wh_hencow_fcf --force_run #refresh every run.
fi




##########################
#input: #"/home/ryan/DATA/DAY_JAQS/*.csv"  <<<
#         /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/*ts_code*_basic.csv.  e.g 600519.SH_basic.csv
#        "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/fina_indicator.csv"
#output: "/home/ryan/DATA/result/pe_pb_rank.csv"
#        "/home/ryan/DATA/result/pe_pb_rank_selected.csv"
##########################7
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/pe_pb_rank.csv
    rm -f /home/ryan/DATA/result/pe_pb_rank_selected.csv
    python t_daily_pe_pb_roe_history.py --force_run  # <<< 2min
fi



if [ $full_or_daily == "DAILY" ]; then
      echo "SKIP"
      python t_daily_pe_pb_roe_history.py  #refresh every run.
fi

###############################################
# Bebeish
#
##############################################
if [ $full_or_daily == "FULL" ]; then
   python t_yearly_beneish.py
fi



######################################################## Step3 FUND1 ################################################

################################################
#need to update every quarter.
#output: "/home/ryan/DATA/result/fundamental.csv"
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental.csv
    python t_daily_fundamentals.py --process_hist_data --force_run  #<<< 1 hour 30 min
fi

if [ $full_or_daily == "DAILY" ]; then
    echo "Do Nothing on DAILY, t_daily_fundamentals.py --process_hist_data"
fi


################################################
## input "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"   "/home/ryan/DATA/result/fundamental.csv" <<< NO LONGER correct.
## input "/home/ryan/DATA/result/jaqs_quarterly_fundamental.csv"   "/home/ryan/DATA/result/fundamental.csv"
## input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/merged/merged_all_20181231.csv
## output: /home/ryan/DATA/result/fundamental_peg.csv << false
##
################################################

if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental_peg.csv
    python t_daily_fundamentals.py --calc_peg --force_run #time consuming. 5 hours?  24572 items. modifed check from 2017_1.
fi

if [ $full_or_daily == "DAILY" ]; then
      echo "SKIP"
    #python t_daily_fundamentals.py --calc_peg
fi

################################################
## need to update every quarter.
## output: /home/ryan/DATA/result/latest_fundamental_quarter.csv --> Fundamental_Quarter_Report_2018_3.csv
## insert column: result_value_quarter_fundation == esp, pe, pb, npr, roe, totalasset
#@todo: the link is not setup on haha_65 latest_fundamental_quarter.csv <<< done
#@todo: remove duplicate columns (name); code to be string; @column sequence; @drop (filter out) garbage company.
#################################################
#rm -f /home/ryan/DATA/result/Fundamental_Quarter_Report_2018_4.csv ###
#rm -f /home/ryan/DATA/result/latest_fundamental_quarter.csv  ### This is a link.

if [ $full_or_daily == "FULL" ]; then
    python t_daily_fundamentals.py --exam_quarterly --force_run #<<< 3 min
fi


if [ $full_or_daily == "DAILY" ]; then
      echo "SKIP"
    #python t_daily_fundamentals.py --exam_quarterly --force_run #refresh every run.
fi


################################################
#input: dump_csv_q."/home/ryan/DATA/result/latest_fundamental_quarter.csv --> Fundamental_Quarter_Report_2018_2.csv
##output: /home/ryan/DATA/result/latest_fundamental_day.csv --> /home/ryan/DATA/result/today/fundamentals.csv
## insert column : result_value_today ==  fvalues, turnover, amount
##				   result_value_sum == result_value_today + result_value_quarter_fundation
#@todo: mktcap	nmc  keep 0 decimal
#outstanding ?
#totals?
################################################
#rm -f /home/ryan/DATA/result/latest_fundamental_day.csv
#rm -f /home/ryan/DATA/result/today/fundamentals.csv

# no longer work. not reasonable to check fundamental everyday.  ryan-2022-01-28
# python t_daily_fundamentals.py --exam_daily  --force_run #refresh every run.  #<<< 20 sec

################################################
##update this year peg
##input: "/home/ryan/DATA/result/fundamental_peg.csv"
##output: "/home/ryan/DATA/result/fundamental_peg_2018_2.csv"
##        "/home/ryan/DATA/result/fundamental_peg_2018_2_selected.csv"
# peg1:
# peg4:
# ps:
# @todo: code need in str, it is 123 for 000123. << should be a liboffice displaying issue
# @todo: name should be clean, left only one.
# @todo: column sequenece.
# @todo: dapan Gu, not applicable to peg
################################################
if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/fundamental_peg_2019_1.csv
    rm -f /home/ryan/DATA/result/fundamental_peg_2019_1_selected.csv
    python t_daily_fundamentals.py --this_year_quarter  --force_run   #<<< 3 sec
fi


if [ $full_or_daily == "DAILY" ]; then
      echo "SKIP"
    #python t_daily_fundamentals.py --this_year_quarter --force_run #refresh every run.
fi


################################################
#use result of --calc_peg.  insert df_ps columns to csv_d, csv_q
#input: latest_fundamental_peg.csv; dump_csv_d; dump_csv_q; fund_ps_csv
#output:
#t_daily_fundamentals.py: fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_day.csv
#t_daily_fundamentals.py: fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_quarter.csv

################################################
# no longer work. 2022-01-28
# python t_daily_fundamentals.py --calc_ps --force_run # <<< 3 sec. Quick so always forcerun

################################################
#input and output: /home/ryan/DATA/result/Fundamental_Quarter_Report_2018_4.csv
#
#insert columns result_value_2, score_sum to csv_q
# result_value_2: ps_perc, peg_1_perc, peg_4_perc
## peg_1 = price_q / egr_1 , egr_1 = 100 * ((eps_this / eps_last_1) - 1)
## peg_4= price_q / egr_4, egr_4 = 100 * ( (eps_this / eps_last_4) -1 )
################################################
#if [ $full_or_daily == "FULL" ]; then
# no longer work. 2022-01-28 #KeyError: 'npr'
#    python t_daily_fundamentals.py --calc_fund_2 --force_run  #<<< 50 sec
#fi

#if [ $full_or_daily == "DAILY" ]; then
#    python t_daily_fundamentals.py --calc_fund_2 --force_run #refresh every run.
#fi


################################################
#input: csv_q: /home/ryan/DATA/result/latest_fundamental_quarter.csv, soft link to --> Fundamental_Quarter_Report_2018_2.csv
#output: "/home/ryan/DATA/result/industry_top.csv"
#@todo: calculation:
#score_sum : same as csv_q
################################################
# no longer work. 2021-01-28 KeyError: 'score_sum'
#rm -f /home/ryan/DATA/result/industry_top.csv
#python t_daily_fundamentals.py --industry_rank_f #<<<  2 sec
#python t_daily_fundamentals.py --industry_rank_f --force_run

################################################
#input: csv_q
#output: "/home/ryan/DATA/result/area_top.csv"
################################################
# no longer work. 2021-01-28 KeyError: 'score_sum'
#rm -f /home/ryan/DATA/result/area_top.csv
#python t_daily_fundamentals.py --area_rank_f  #<<<  2 sec
#python t_daily_fundamentals.py --area_rank_f --force_run

######################################################## Step4 SUMMARY ################################################

################################################
#input:  /home/ryan/DATA/DAY_Global/AG/
#output:  /home/ryan/DATA/result/key_points_AG_today_selected.csv
################################################
## add --force_run to force run

if [ $full_or_daily == "FULL" ]; then
    rm -f /home/ryan/DATA/result/key_points_AG_today_selected.csv
    python t_daily_get_key_points.py  -x AG  --calc_base --force_run  #  <<< slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_AG.csv
    python t_daily_get_key_points.py  -x AG  --calc_today  --force_run # <<<< 4 min, fast. output: /home/ryan/DATA/result/key_points_AG_today.csv
    python t_daily_get_key_points.py  -x AG  --today_selection --force_run  #fast. output: /home/ryan/DATA/result/key_points_AG_today_selected.csv
fi

if [ $full_or_daily == "DAILY" ]; then
#    python t_daily_get_key_points.py  -x AG  --calc_base  #slow (10hours).  run every 7 days. output: /home/ryan/DATA/result/key_points_AG.csv
    python t_daily_get_key_points.py  -x AG  --calc_today  #fast. output: /home/ryan/DATA/result/key_points_AG_today.csv
    python t_daily_get_key_points.py  -x AG  --today_selection #fast. output: /home/ryan/DATA/result/key_points_AG_today_selected.csv
fi


##############################
# input:
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/summary_600519.csv
# /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/detail_600519.csv
#
#
# output:
#  /home/ryan/DATA/result/top_10_holder_summary_full.csv
#  /home/ryan/DATA/result/top_10_holder_details_full.csv
#  /home/ryan/DATA/result/top_10_holder_summary_latest.csv"
#  /home/ryan/DATA/result/top_10_holder_detail_latest.csv"
###############################
if [ $full_or_daily == "FULL" ]; then
    python t_top_10_holders.py --analyze --stock_global AG
fi

# not necessory run daily
#if [ $full_or_daily == "DAILY" ]; then
#    python t_top_10_holders.py --analyze --stock_global AG --selected
#fi

################################
# input: ~/DATA/DAY_Global/AG_qfq/*.csv
# output: /home/ryan/DATA/result/zigzag_div/*.svg
#        /home/ryan/DATA/result/zigzag_kdj_div.csv  zigzag_macd_div.csv  zigzag_rsi_div.csv
if [ $full_or_daily == "FULL" ]; then
  rm -f /home/ryan/DATA/DAY_Global/AG_qfq/ag_all.csv
  python t_daily_indicator_kdj_macd.py --zigzag_div --zigzag_plt
fi


##############################
# concept top
# input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/market/pro_concept.csv
# output: /home/ryan/DATA/result/concept_top.csv
###############################
if [ $full_or_daily == "FULL" ]; then
  python t_daily_fundamentals_2.py  --concept_top
fi
################################################
#run1
#run2
#run3
#run4
#run5
#output: /home/ryan/DATA/result/today/talib_and_pv_no_db_filter_ag.csv
#output: /home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_ag.csv
################################################
rm -f /home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_AG.csv
python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x AG --selected



####################################
# output: /home/ryan/DATA/result/selected/jin_cha_si_cha_cnt.csv
python t_daily_junxian_barstyle.py -x AG_HOLD --ma_short 4 --ma_middle 27 --calc_ma_across_count --selected


####################################
# output: /home/ryan/DATA/result/jin_cha_si_cha_cnt.csv
#python t_daily_junxian_barstyle.py -x AG --ma_short 4 --ma_middle 27 --calc_ma_across_count  --remove_garbage
if [ $full_or_daily == "FULL" ]; then
  python t_daily_junxian_barstyle.py -x AG --ma_short 4 --ma_middle 27 --calc_ma_across_count
fi
#######################################
# output: /home/ryan/DATA/result/ag_junxian_barstyle.csv
# output: /home/ryan/DATA/result/ag_junxian_barstyle_w.csv
# output: /home/ryan/DATA/result/ag_junxian_barstyle_m.csv
#######################################


#if [ $full_or_daily == "DAILY" ]; then
#    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --remove_garbage --period D
#    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --remove_garbage --period W
#    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --remove_garbage --period M
#
#fi


if [ $full_or_daily == "FULL" ]; then
    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --period D
    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --period W
    python t_daily_junxian_barstyle.py -x AG  --ma_short 4 --ma_middle 27 --ma_long 60 --period M
fi

#######################################
# output: /home/ryan/DATA/result/hong_san_bin.csv
#######################################
python t_daily_junxian_barstyle.py  --hong_san_bin --stock_global AG --remove_garbage

#######################################
# output: /home/ryan/DATA/result/
#(base) ryan@haha_brain_2:~/DATA/result$ ls *bar*
#ag_junxian_barstyle.csv                ag_junxian_barstyle_jincha_major.csv  ag_junxian_barstyle_very_strong_down_trend.csv  ag_junxian_barstyle_yunxian_buy.csv
#ag_junxian_barstyle_duotou_pailie.csv  ag_junxian_barstyle_jincha_minor.csv  ag_junxian_barstyle_very_strong_up_trend.csv    ag_junxian_barstyle_yunxian_sell.csv
#######################################
python t_daily_junxian_barstyle.py -x AG --show_result

#rm -f /home/ryan/DATA/result/today/talib_and_pv_no_db_filter_AG.csv
#python t_daily_pattern_Hit_Price_Volume.py --bool_calc_std_mean --bool_perc_std_mean --bool_pv_hit -m 7 -x AG

#python t_daily_pattern_Hit_Price_Volume.py -0  -m 30 -x AG  #it was 22 ->222 ->30
#python t_daily_pattern_Hit_Price_Volume.py -1 -2 -4  -x AG
#python t_daily_pattern_Hit_Price_Volume.py -1 -2 -4  -m 22  #Only run pv test.




##############################
# indicator
# input: DAY_Global/AG
# output: ~/DATA/result/macd_selection_M.csv
#         ~/DATA/result/macd_selection_W.csv
#         ~/DATA/result/macd_selection_D.csv
# 	 /home/ryan/DATA/result/MACD_month_week_day_common.csv
#
#         ~/DATA/result/kdj_selection_M.csv
#         ~/DATA/result/kdj_selection_W.csv
#         ~/DATA/result/kdj_selection_D.csv
# 	 /home/ryan/DATA/result/KDJ_month_week_day_common.csv
# 	 /home/ryan/DATA/result/MA_CROSS_month_week_day_common.csv
#
###############################
if [ $full_or_daily == "FULL" ]; then
  python t_daily_indicator_kdj_macd.py --indicator KDJ --period M
  python t_daily_indicator_kdj_macd.py --indicator KDJ --period W
  python t_daily_indicator_kdj_macd.py --indicator MACD --period M
  python t_daily_indicator_kdj_macd.py --indicator MACD --period W
fi

python t_daily_indicator_kdj_macd.py --indicator KDJ --period D
python t_daily_indicator_kdj_macd.py --indicator KDJ --analyze
python t_daily_indicator_kdj_macd.py --indicator MACD --period D
python t_daily_indicator_kdj_macd.py --indicator MACD --analyze

########################
# MA 21 cross up MA 55
# input: DAY_Global/AG
# output: /home/ryan/DATA/result/ma_cross_over_selection_5_10.csv"
# output: /home/ryan/DATA/result/ma_cross_over_selection_10_20.csv"
# output: /home/ryan/DATA/result/ma_cross_over_selection_21_55.csv"
########################
if [ $full_or_daily == "FULL" ]; then
  python t_daily_indicator_kdj_macd.py --indicator MA_CROSS --period D --period_fast 5 --period_slow 10
  python t_daily_indicator_kdj_macd.py --indicator MA_CROSS --period D --period_fast 10 --period_slow 20
fi

python t_daily_indicator_kdj_macd.py --indicator MA_CROSS --period D --period_fast 4 --period_slow 27
python t_daily_indicator_kdj_macd.py --indicator MA_CROSS --analyze



##############################
# fibonacci indicator
# input: DAY_Global/AG,  DAY_Global/AG_INDEX
# output: ~/DATA/result/fib.csv
# 	  ~/DATA/result/selected/ag_index_fib.csv
###############################
#----------------- selected moved to t_daily_run_2_exam_ushk.sh
if [ $full_or_daily == "FULL" ]; then

  # /home/ryan/DATA/result/selected/ag_index_fib.csv
  python t_fibonacci.py --begin_date "20200101"  --save_fig --min_sample=400 -x AG_INDEX --selected

  # /home/ryan/DATA/result/ag_fib.csv
  python t_fibonacci.py --begin_date "20200101"  --min_sample=400 -x AG

  ## /home/ryan/DATA/result/selected/ag_fib.csv
  python t_fibonacci.py --begin_date "20200101"  --save_fig --min_sample=400 -x AG --selected

fi


#########################
#input: daily price
#output: /home/ryan/DATA/result/selected/ag_curve_shape.csv
#########################
if [ $full_or_daily == "FULL" ]; then
    python t_double_bottom.py -x AG --save_fig --min_sample 90
fi

python t_double_bottom.py  -x AG --save_fig --min_sample 90 --selected


################################
# input: /home/ryan/DATA/DAY_Global/AG/*.csv, ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/*.csv
# output: /home/ryan/DATA/result/
#       hs300_candidate_list.csv  sz100_candidate_list.csv  szcz_candidate_list.csv  zz100_candidate_list.csv
#       nasdaq100_candidate_list.csv, spx500_candidate_list.csv, cn_candidate_list.csv, cn_sse_candidate_list.csv,
#       cn_szse_candidate_list.csv
#
#       /home/ryan/DATA/result/average_daily_amount_sorted_"+str(period_begin)+"_"+str(period_end)+".csv"
#       /home/ryan/DATA/result/average_daily_mktcap_sorted_"+str(period_begin)+"_"+str(period_end)+".csv"
# 	/home/ryan/DATA/result/latest_ma_koudi.csv
# 	/home/ryan/DATA/result/stocks_amount_20200124_20210123.csv 
# 	/home/ryan/DATA/result/stocks_amount_365_days.csv  << symbol link to stocks_amount_20200124_20210123.csv
###############################
#if [ $full_or_daily == "FULL" ]; then
#  python t_daily_index_candidates.py --index_name hs300 --period_start 20210501 --period_end 20220430  --index_source wugui --force_run  # HS300
#  python t_daily_index_candidates.py --index_name szcz --period_start 20211101 --period_end 20220430  --index_source wugui --force_run  # SHEN_ZHEN
#fi


if [ $full_or_daily == "FULL" ]; then
# After 11.01
  python t_daily_index_candidates.py --index_name hs300 --period_start 20210501 --period_end 20220430 --index_source wugui  --daily_update
  python t_daily_index_candidates.py --index_name zz100 --period_start 20210501 --period_end 20220430 --index_source wugui  --daily_update
  python t_daily_index_candidates.py --index_name szcz  --period_start 20211101 --period_end 20220430 --index_source wugui  --daily_update
  python t_daily_index_candidates.py --index_name sz100 --period_start 20211101 --period_end 20220430 --index_source wugui  --daily_update

# After 5.1
#  python t_daily_index_candidates.py --index_name hs300 --period_start 20201101 --period_end 20211031 --index_source wugui  --daily_update --force_run
#  python t_daily_index_candidates.py --index_name zz100 --period_start 20201101 --period_end 20211031 --index_source wugui  --daily_update
#  python t_daily_index_candidates.py --index_name szcz  --period_start 20210501 --period_end 20211031 --index_source wugui  --daily_update --force_run
#  python t_daily_index_candidates.py --index_name sz100 --period_start 20210501 --period_end 20211031 --index_source wugui  --daily_update


fi

python t_daily_index_candidates.py --index_name hs300 --period_start 20210501 --period_end 20220430 --index_source wugui  --daily_update

#######################################
# run this after HS300, as this use file generated during hs300 calcuation.  /result/stocks_amount_365_days.csv
# output: /home/ryan/DATA/result/price_let_mashort_equal_malong.csv
#######################################
# no longer work, 2022-01-28  [Errno 2] No such file or directory: '/home/ryan/DATA/result/stocks_amount_365_days.csv'
#python t_daily_junxian_barstyle.py -x AG --ma_short 4 --ma_middle 27 --calc_ma_across_price


#using tradingview source.  period_start/end is not actually used, just not to broke the program
python t_daily_index_candidates.py --index_name nasdaq100 --period_start 20210101 --period_end 20210101
python t_daily_index_candidates.py --index_name spx500 --period_start 20210101 --period_end 20210101
python t_daily_index_candidates.py --index_name cn --period_start 20210101 --period_end 20210101
python t_daily_index_candidates.py --index_name cn_sse --period_start 20210101 --period_end 20210101
python t_daily_index_candidates.py --index_name cn_szse --period_start 20210101 --period_end 20210101

#/home/ryan/DATA/result/high_volumes/*.csv
python t_futu_trade.py --check_high_volume --market AG_HOLD
python t_futu_trade.py --check_high_volume --market HK_HOLD
python t_futu_trade.py --check_high_volume --market AG
python t_futu_trade.py --check_high_volume --market HK

# Set AG Price reminder at FuTu Client, based on the daily price. run after 3:00pm
python t_futu_trade.py --set_ag_reminder


#############
# output: /hdd/DATA/result/report_2019-03-29_AG.txt
#############
python t_summary.py -x AG --action generate_report


#############
# output: /home/ryan/DATA/result/report_2019-03-29_AG_short.csv
#############
python t_summary.py -x AG --action analyze_report


#############--calc_base
# output: /home/ryan/DATA/result/report_new_dev_B_20210105_AG.txt
#         /home/ryan/DATA/result/report_new_dev_S_20210105_AG.txt
#############
python t_summary_new_dev.py -x AG --operation B --action generate_report --remove_garbage

python t_summary_new_dev.py -x AG --operation B --action generate_report
python t_summary_new_dev.py -x AG --operation S --action generate_report

#### Check performance of each day's selected.
# output:  /home/ryan/DATA/result/result_new_dev_B/20210125/summary.txt
# $ grep === /home/ryan/DATA/result/result_new_dev_B/20210125/summary.txt
#=== df_szcz_add_candidate , 0126: -3.28, 0127: -0.15, 0128: -2.2, 0129: -0.98
#=== df_sz100_add_candidate , 0126: -4.3, 0127: 0.04, 0128: -3.06, 0129: -1.5
python t_summary_new_dev.py -x AG  --action analyze_post_perf --operation B
python t_summary_new_dev.py -x AG  --action analyze_post_perf --operation S


#######
# commit garbage to github
######
cd /home/ryan/DATA/result/garbage
for i in `ls latest_*`; do cat $i > /home/ryan/DATA/result/garbage_$i;  done

cd /home/ryan/DATA/result
git add report_latest*.txt
git add garbage_latest_*.csv
git commit -a -m'1'; git push;
echo "garbage_latest and report_latest were commited to github"


#Possible good candidates:
#  df_support_resist_line_today <<  SHORT term. with fund or fund_2 other fundamental indicators (whitehorse, ROE, etc)
#                                   at lower price, bottom price now?

#  df_low_price_year  <<< SHORT term
#  df_low_vol_year <<< SHORT term
#  df_pv_break  <<< SHORT
#  df_decrease_gap  <<< SHORT
#  df_disclosure_date_notify <<< SHORT


#  df_pe_pb_roe_history <<<  MIDDLE term
#  df_fund <<<  last Q status, reflect recent.   MIDDLE term

#  df_fund_2  <<<  last Y status, reflect long history. LONG term.
#  df_freecashflow_price_ratio  <<<< cheap.  LONG term.
#  df_peg_ps  <<< earn quickly. Price/(EarningsToGrowth) (PEG) Ratio.   LONG term.
#  df_disclosure_date_notify <<< news is going to announce. SHORT term.
#  df_reduced_quarter_year  <<< inner merge of df_fund and df_fund_2. standfor good value company.  LONG term.
#  df_fenghong <<< LONG term, but not as important as df_fund.
#  df_whitehorse  <<< LONG
#  df_freecashflow_price_ratio  <<< LONG
#  df_hen_cow  <<< LONG


# The stratage is :  IN LONG term scope,  FIND stocks cheap in SHORT term. ( SELECT SHORT FROM LONG)
#
#
# Example:  find CHEAP stock, and the stock are in SHORT TERM (S) and LONG TERM (L) df.
#        cat /home/ryan/DATA/result/report_2019-03-08_AG.txt |grep == | grep -vE "0 cheap.*" | grep -v "0s" | grep -v "0l"



#python t_daily_update_order_tracking_stock.py



