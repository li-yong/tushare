#source /home/ryan/.bashrc
#source /home/ryan/.profile 
#source /etc/bash.bashrc 

cd /home/ryan/tushare_ryan
git pull

bash -x t_daily_run_1_update_data.sh FULL
bash -x t_daily_run_2_exam.sh FULL

