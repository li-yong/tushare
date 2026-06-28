import logging
import traceback
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import datetime
import json
import pprint
import sys
from optparse import OptionParser



sys.path.append('/home/ryan/tushare_ryan/Oanda_v2/logic')
import t_ph_lib


parser = OptionParser()
parser.add_option("-r", "--revert", action="store_true",
                  dest="revert", default=False,
                  help="revert B/S action on comparation account")

(options, args) = parser.parse_args()
revert=options.revert

print("revert: "+str(revert))

if revert:
    #practice account sunraise2008,  740@qq, fav
    active_account = '101-011-8038242-001'
    token = '1aff7320463828a486046f155ecfaad6-5f8ad04cdaaf330fd17ab07d4dbcea12'
else:
    #practice account sunraise2007
    active_account = '101-011-7847380-001'
    token = '43b32f61afca7adc3a85ebd5d8fcfb8a-015be2ef2131fa8db6d68a675d2c1a64'



if __name__ == "__main__":
    # t_ph_lib.v20C().simple_limit()


    instrument = 'XAU_USD'
    price = '1410'
    unit = '10'
    take_profit_price = '1450'
    stop_loss_price = '1350'
    trailing_stop_loss_distance = '20'


    # t_ph_lib.v20C().simple_stop(instrument,price,unit,take_profit_price,stop_loss_price,trailing_stop_loss_distance)
    # t_ph_lib.v20C().simple_cancel_orders()
    # t_ph_lib.v20C().simple_cancel_trades()
    # t_ph_lib.v20C().simple_get_candles()
    t_ph_lib.v20C(active_account,token).simple_close_all_trades(close_reason="Closed All trades by user request")


