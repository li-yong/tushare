scp /home/ryan/repo/trading/oandapybot-ubuntu/t_*.py root@td:/root/repo/trading/oandapybot-ubuntu/
scp /home/ryan/repo/trading/oandapybot-ubuntu/t_*.sh root@td:/root/repo/trading/oandapybot-ubuntu/
scp /home/ryan/repo/trading/oandapybot-ubuntu/logic/t_*.py root@td:/root/repo/trading/oandapybot-ubuntu/logic/
scp /home/ryan/repo/trading/tushare_ryan/t_*.py root@td:/root/repo/trading/tushare_ryan/
scp /home/ryan/.v20.conf  root@td:/root/

#ssh root@td "pkill bash; pkill python"
#ssh root@td "cd ~/oandapybot-ubuntu/; python t_close_all_trades.py ; python t_close_all_trades.py -r ;  "
ssh root@td " . ~/.bashrc; echo $PATH; bash ~/oandapybot-ubuntu/t_clean_trade_env_vps.sh"
