import traceback
import logging
import sys


from datetime import datetime, timedelta
from optparse import OptionParser

sys.path.append('/home/ryan/tushare_ryan/Oanda_v2/logic')
import t_ph_lib

parser = OptionParser()

yesterday= datetime.now() - timedelta(days=2) #so will not report end time in future any o'clocks at a day.


parser.add_option("-y", "--year", dest="year",default=yesterday.year, type="int",
                  help="start year, yyyy")

parser.add_option("-m", "--month", dest="month",default=yesterday.month, type="int",
                  help="start month, mm")

parser.add_option("-d", "--day", dest="day",default=yesterday.day, type="int",
                  help="start day,dd")

parser.add_option("-n", "--next_n_days", dest="next_n_days",default=1, type="int",
                  help="next N days after ")

(options, args) = parser.parse_args()


year=options.year
month=options.month
day=options.day
next_n_days=options.next_n_days

active_account = '101-011-8038242-001'
token = '1aff7320463828a486046f155ecfaad6-5f8ad04cdaaf330fd17ab07d4dbcea12'

t_ph_lib.v20C(active_account,token).get_history_forex_data(year, month, day, next_n_days)

exit(1)
