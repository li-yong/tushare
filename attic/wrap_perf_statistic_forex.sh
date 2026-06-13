#echo -e "`date`\c"

rm -f ~/DATA/tmp/pv/*
python t_monthly_strategy_perf_gathering.py   --db_tbl=zzz_pattern_perf_debug --truncate_tbl  #empty the table


for i in {1..6000}; do
  printf "LOOP ################ $i of 6000, " |tee -a del.log;
  echo `date` |tee -a del.log;

  #local, forex, 1000 lines
  printf "run ptn_hit_price_volume, " |tee -a del.log;
  echo `date` |tee -a del.log ;
  python t_daily_pattern_Hit_Price_Volume.py -l -f   -m 1000  -0  >>del.log  2>&1;
  pkill -9 python

  printf "run perf_gathering, " |tee -a del.log;
  echo `date`|tee -a del.log;

  python t_monthly_strategy_perf_gathering.py  --db_tbl=zzz_pattern_perf_debug --skip_backup_before_execute  >> del.log  2>&1
  pkill -9 python
done

