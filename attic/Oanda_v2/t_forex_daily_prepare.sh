#!/bin/bash

env - `cat ~/cronenv`


POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -e|--extension)
    EXTENSION="$2"
    shift # past argument
    shift # past value
    ;;
esac
done

#echo FILE EXTENSION  = "${EXTENSION}"


###############################
# Grab Data.  Run at the HK VSP
# At HK VSP, have a cron job, $0 -e RUN_AT_HK_VSP
###############################
if [ ${EXTENSION} == "RUN_AT_HK_VSP" ]
then
    echo "RUN_AT_HK_VSP"
    rm -rf /home/ryan/DATA/DAY_Forex_local
    mkdir /home/ryan/DATA/DAY_Forex_local
    cd /home/ryan/DATA/DAY_Forex_local
    #rm -rf /home/ryan/DATA/DAY_Forex_local/*
    /root/anaconda3/bin/python /home/ryan/oandapybot-ubuntu/t_get_history_data.py
    cd /home/ryan/DATA; tar -czvf /home/ryan/DATA/DAY_Forex_local.tar.gz  /home/ryan/DATA/DAY_Forex_local
fi





###############################
# Run at HAHA_Brain (Bridge of the HK VSP and HAHA_66
# Called in t_daily_run_1_update_data.sh @haha_brain
#      bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN
#
###############################
#if [ ${EXTENSION} == "CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN" ]
#then
#    echo "CP_YESTERDAY_FOREX_DATA_FROM_HK_VSP_TO_HAHA_BRAIN"
#    scp root@td:/home/ryan/DATA/DAY_Forex_local.tar.gz   /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz
#    echo "saved to /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz"
#fi

################################
# Run at HAHA_Brain, Switch Network to EMC Corp
#
# called at t_daily_sync_to_66.sh @ haha_brain
#  bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HAHA_BRAIN_TO_HAHA_66
#################################
#if [ ${EXTENSION} == "CP_YESTERDAY_FOREX_DATA_FROM_HAHA_BRAIN_TO_HAHA_66_AND_ANALYZE" ]
#then
#    echo "CP_YESTERDAY_FOREX_DATA_FROM_HAHA_BRAIN_TO_HAHA_66_AND_ANALYZE"
#    scp  /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz  haha_66:/home/ryan/DATA/
#    #ssh haha_66 "cd /home/ryan/DATA/; tar -xzvf /home/ryan/DATA/DAY_Forex_local.tar.gz"
#    ssh haha_66 "cd ~/oandapybot-ubuntu; git pull"
#    ssh haha_66 "nohup bash +x /home/ryan/repo/trading/oandapybot-ubuntu/t_forex_daily_prepare.sh -e ANALYSE_FORX_PATTERN_PERF_UPDATE_DB >~/nohup.log 2>&1 &"
#fi


###############################
# Run at HAHA_66
###############################
if [ ${EXTENSION} == "ANALYSE_FORX_PATTERN_PERF_UPDATE_DB" ]
then
    echo "ANALYSE_FORX_PATTERN_PERF_UPDATE_DB"
    rm -rf /home/ryan/DATA/DAY_Forex_local
    cd /home/ryan/DATA; tar -xzvf /home/ryan/DATA/DAY_Forex_local.tar.gz; mv /home/ryan/DATA/home/ryan/DATA/DAY_Forex_local ./
    #rm -rf /home/ryan/DATA/result/today.del
    #mv  /home/ryan/DATA/result/today ~/DATA/result/today.del
    #mkdir  /home/ryan/DATA/result/today
    cd /home/ryan/repo/trading/tushare_ryan;
    bash /home/ryan/repo/trading/tushare_ryan/wrap_perf_statistic_forex.sh


    ##########
    #  analyse,-f -l --> /home/ryan/DATA/DAY_Forex_local, -m 0 --> check all rows (minutes),-0   --> check all rules
    ##########
    #python t_daily_pattern_Hit_Price_Volume.py -0 -f -l  -m 0

    ##########
    #  import to DB debug table
    ##########
    #python t_monthly_strategy_perf_gathering.py --truncate_tbl  --db_tbl zzz_pattern_perf_debug

    ##########
    #  merge to main table
    ##########
    python t_update_strategy_perf_db_to_db.py  --source_host=127.0.0.1  --source_table=zzz_pattern_perf_debug --dest_host=127.0.0.1 --dest_table=pattern_perf_forex

fi
