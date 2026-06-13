#echo -e "`date`\c"


# Must run with DATA/DAY_local, because the Dataframe.to_csv will change the source file.
rm -rf ~/DATA/DAY_local
cp -r  ~/DATA/DAY ~/DATA/DAY_local

rm -f ~/DATA/tmp/pv/*
python t_monthly_strategy_perf_gathering.py  --db_tbl=zzz_pattern_perf_debug --truncate_tbl   #empty the table


for i in {1..5}; do  #SH000001 is less than 5000
  printf "LOOP ################ $i of 10, " |tee -a del.log;
  echo `date` |tee -a del.log;

  #local, forex, 1000 lines
  printf "run ptn_hit_price_volume, " |tee -a del.log;
  echo `date`|tee -a del.log;
  python t_daily_pattern_Hit_Price_Volume.py -l  -m 1000  -0  --stock_global AG >>del.log  2>&1;
  pkill -9 python

  printf "run perf_gathering, " |tee -a del.log;
  echo `date` |tee -a del.log;

  python t_monthly_strategy_perf_gathering.py  --db_tbl=zzz_pattern_perf_debug --skip_backup_before_execute >> del.log 2>&1
  pkill -9 python
done

