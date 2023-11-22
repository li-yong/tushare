#source /home/ryan/.bashrc
#source /home/ryan/.profile 
#source /etc/bash.bashrc 

cd /home/ryan/tushare_ryan

bash -x t_daily_run_1_update_data.sh MONTHLY
bash -x t_daily_run_2_exam.sh MONTHLY

