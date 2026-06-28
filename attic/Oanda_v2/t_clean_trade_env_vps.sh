#pkill bash; pkill python #kill the tmux session
#THIS SCRIPT run at the HK VPS server ONLY
#IT does:
#1. close all trades on oanda
#2. empty the db forex order tracking table
pkill python;
cd ~/oandapybot-ubuntu/;
~/anaconda3/bin/python t_close_all_trades.py ;
~/anaconda3/bin/python t_close_all_trades.py -r ;
/opt/lampp/bin/mysql -uroot -padmin888.@_@  ryan_stock_db  <  ~/oandapybot-ubuntu/empty_order_tracking_froex.sql
echo "table order_tracking_forex was truncated"
