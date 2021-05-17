#!/usr/bin/env bash


full_or_daily=$1

if [ $full_or_daily == "FULL" ]; then
    echo "$0: FULL RUN"
elif [ $full_or_daily == "DAILY" ]; then
    echo "$0: DAILY RUN"
else
    echo "Have to specify FULL or DAILY. Quiting";
    exit
fi




######################################
#
# commit and sync git
#
######################################

cd ~/tushare_ryan; git commit -a -m'1'; git push;


ssh haha_65 "cd ~/tushare_ryan; git pull; "
ssh haha_66 "cd ~/tushare_ryan; git pull; "


######################################
#
# haha_66 ---> haha_brain
#
######################################

bash -x t_backtest_sync_65_result_to_local.sh

rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged.dev    haha_66:/hdd/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev   haha_66:/hdd/DATA/pickle/Stock_Fundamental/fundamentals_2
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source.dev   haha_66:/hdd/DATA/pickle/Stock_Fundamental/fundamentals_2

######################################
#
# haha_brain ---> haha_66
#
######################################
rsync -az /home/ryan/DATA/DAY_Global/AG/  haha_66:/hdd/DATA/DAY_Global/AG/
rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/daily/

if [ $full_or_daily == "FULL" ]; then
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/
fi

if [ $full_or_daily == "DAILY" ]; then
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2021/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2021/
    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2020/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals/2020/
fi



rsync -az /home/ryan/DATA/pickle/daily_update_source/  haha_66:/hdd/DATA/pickle/daily_update_source/
rsync -az /home/ryan/DATA/pickle/*.csv  haha_66:/hdd/DATA/pickle/

rsync -az /home/ryan/DATA/announcement/  haha_66:/hdd/DATA/announcement/
rsync -az /home/ryan/DATA/DAY_No_Adj/  haha_66:/hdd/DATA/DAY_No_Adj/
rsync -az /home/ryan/DATA/DAY_JAQS/  haha_66:/hdd/DATA/DAY_JAQS/


rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/  haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/top_10_holder/


rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/holdertrade/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/cctv_news/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/market/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/  haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/daily_money_flow/

#rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/

for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep 201 | grep -E 201[8-9]`;
do
    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
    rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
done


for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep 202 | grep -E 202[0-3]`;
do
    echo "sync file to normal directory /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
    rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
done


#for i in `ls /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/ | grep -v 201`;
#do
#    echo "sync file to symbol link /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/". $i
#    rsync -az /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/$i/
#done

rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20201231/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20201231/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210331/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210331/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210630/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210630/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210930/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20210930/
rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20211231/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/20211231/



if [ $full_or_daily == "FULL" ]; then
    rsync -azL /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/ haha_66:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/
fi



###################
# Refesh nfs cache on haha_65
# ryan@haha_65 are configured as sudoer without password. https://phpraxis.wordpress.com/2016/09/27/enable-sudo-without-password-in-ubuntudebian/
#At the end of the /etc/sudoers file add this line:
#username     ALL=(ALL) NOPASSWD:ALL
# ####################

#ssh haha_65 "sudo mount -o remount /hdd/DATA/DAY"  #commented out on haha_65
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/DAY_Global"
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/pickle"
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/announcement"
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/DAY_Forex_full/"
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/DAY_No_Adj/"
ssh -t haha_65 "sudo mount -o remount /hdd/DATA/DAY_JAQS/"


ssh haha_65 "nohup bash -x ~/tushare_ryan/t_daily_run_2_exam.sh $full_or_daily > ~/no_hup_run_exam_ag.log  2>&1  &"
echo "nohup bash -x ~/tushare_ryan/t_daily_run_2_exam.sh $full_or_daily > ~/no_hup_run_exam_ag.log  2>&1  &"
echo "Daily Stock exam(finding B/S points) is running on haha_65, output to ~/no_hup_run_2_exam_ag.log, db tbl order_tracking_stock will be update on haha65."

#Forex
#bash /home/ryan/oandapybot-ubuntu/t_forex_daily_prepare.sh -e CP_YESTERDAY_FOREX_DATA_FROM_HAHA_BRAIN_TO_HAHA_66_AND_ANALYZE

#RYAN: 20180405, temporarily stop the daily forex as the haha_66 is running the importing of 2016 year's forex data to db
#scp  /home/ryan/DATA/tmp/DAY_Forex_local.tar.gz  haha_66:/home/ryan/DATA/
#ssh haha_66 "nohup bash +x /home/ryan/repo/trading/oandapybot-ubuntu/t_forex_daily_prepare.sh -e ANALYSE_FORX_PATTERN_PERF_UPDATE_DB >~/no_hup.log 2>&1 &"
#echo "Daily Forex exam(ptn perf tbl update) is running on haha_66, output to ~/no_hup.log"
