#!/usr/bin/env bash

#run sequence is important.

#python t_daily_fundamentals.py  --fetch_data_all #load <2018 from local, getting 2018 always.  RUN WEEEKLY
python t_daily_fundamentals.py  --process_hist_data
python t_daily_fundamentals.py  --exam_quarterly


python t_monthly_jaqs_fundamental.py --fetch_data_all  #the data is up to 2018-05-10
python t_monthly_jaqs_fundamental.py --calc_quartly_report #time comsuming

python t_daily_fundamentals.py  --calc_peg #time comsuming  423 of 97508 =
python t_daily_fundamentals.py  --this_year_quarter  #not necessary?
# 2018_1 fundmental peg result saved to /home/ryan/DATA/result/fundamental_peg_2018_1.csv
# 2018_1 fundmental peg Selectd result saved to /home/ryan/DATA/result/fundamental_peg_2018_1_selected.csv

python t_daily_fundamentals.py  --calc_ps
#fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_day.csv
# fundmental ps, peg inserted into /home/ryan/DATA/result/latest_fundamental_quarter.cs


python t_daily_fundamentals.py  --calc_fund_2
#/home/ryan/DATA/result/latest_fundamental_quarter.csv

python t_daily_fundamentals.py  --area_rank_f
#/home/ryan/DATA/result/area_top.csv

python t_daily_fundamentals.py  --industry_rank_f
# /home/ryan/DATA/result/industry_top.csv

python t_daily_fundamentals.py  --exam_daily
#/home/ryan/DATA/result/today/fundamentals.csv
