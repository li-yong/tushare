#source /home/ryan/.bashrc
#source /home/ryan/.profile 
#source /etc/bash.bashrc 


cd /home/ryan/tushare_ryan

#
#bash -x t_daily_selected_f_e.sh FETCH
#bash -x t_daily_selected_f_e.sh EXAM

bash -x t_daily_run_1_update_data.sh DAILY
bash -x t_daily_run_2_exam.sh DAILY

