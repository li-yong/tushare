
cd ~/repo/trading/tushare_ryan

#come to a B/S csv file. use all the records in the input csv
python t_daily_pattern_Hit_Price_Volume.py -m 0 ; pkill -9 python

#analyze ptn perf and update to DB
# truncate table before insert
python t_monthly_strategy_perf_gathering.py -t ; pkill -9 python
