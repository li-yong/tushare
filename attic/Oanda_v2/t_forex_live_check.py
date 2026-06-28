# coding: utf-8

import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas as pd
import os

from datetime import datetime, timedelta
from sqlalchemy import create_engine
import re
from scipy import stats
from multiprocessing import Pool
import multiprocessing
import sys
from optparse import OptionParser

import shutil
from tushare.util import dateu
import tushare.util.dateu as ts_ud

import oandapy
#from settings import *
#from util.mail import Email
#from util.ui import CursedUI
#from exchange.oanda import Oanda
#from exchange.oanda import OandaExceptionCode
#from logic.strategy import Strategy
import traceback
import logging
import sys
import pandas as pd
import numpy as np
import sys
import datetime
import mysql.connector
import calendar

sys.path.append('/home/ryan/tushare_ryan')
import finlib

sys.path.append('/home/ryan/tushare_ryan/Oanda_v2/logic')
import t_ph_lib


#from settings import *

pd.set_option("display.max_rows", 99999)
pd.set_option("display.max_columns", 100)
pd.set_option('expand_frame_repr', False)


#This script run daily after the marketing closed,
#     Please run after the csv file updated today's data.
#It show the stocks which meet patter (buy or sell) point.
#The result can be a reference for next day's trading.

#step1: update DATA/DAY
#Step2: vim t_daily_pattern_Hit_Price_Volume.py : debug=false, max_exam_day = 22
#Step3: python t_daily_pattern_Hit_Price_Volume.py. Then today's talib and pv hit after filtering DB are saved to DATA/result/today/pv.csv
#
parser = OptionParser()

parser.add_option("-m", "--max_exam_day", dest="max_exam_day",default=22, type="int",
                  help="max_exam_day, 300000 for perf gathering, default 22 for daily ptn checking")

parser.add_option("-d", "--debug", action="store_true",
                  dest="debug", default=False,
                  help="enable debug, use in development purpose")

parser.add_option("-s", "--single_process", action="store_true", dest="single_process",default=False,
                  help="using single process, otherwise using default multiple process")

parser.add_option("-e", "--exam_date", dest="exam_date",
                  help="exam_date, YYYY-MM--DD, no default value, missing will calc the nearest trading day, most time is today")


parser.add_option("-f", "--forex", action="store_true",
                  dest="forex", default=False,
                  help="handle Forex, source is /home/ryan/DATA/DAY_Forex")

parser.add_option("-g", "--merge_today_only", action="store_true",
                  dest="merge_only", default=False,
                  help="skip calc DAY source, just merge the result from /home/ryan/DATA/tmp/pv_today")
parser.add_option("-0", "--bool_check_all", action="store_true", dest="bool_check_all", default=False, help="run all check")

if True:
    parser.add_option("-1", "--bool_calc_std_mean", action="store_true", dest="bool_calc_std_mean", default=False, help="run loop #1")
    parser.add_option("-2", "--bool_perc_std_mean", action="store_true",dest="bool_perc_std_mean", default=False,help="run loop #2")
    parser.add_option("-3", "--bool_talib_pattern", action="store_true",dest="bool_talib_pattern", default=False,help="run loop #3")
    parser.add_option("-4", "--bool_pv_hit", action="store_true",dest="bool_pv_hit", default=False,help="run loop #4")


    parser.add_option("-5", "--bool_p_mfi_div", action="store_true",dest="bool_p_mfi_div", default=False,help="run loop #5")
    parser.add_option("-6", "--bool_p_rsi_div", action="store_true", dest="bool_p_rsi_div", default=False, help="run loop #6")
    parser.add_option("-7", "--bool_p_natr_div", action="store_true", dest="bool_p_natr_div", default=False, help="run loop #7")
    parser.add_option("-8", "--bool_p_tema_div", action="store_true",dest="bool_p_tema_div", default=False,help="run loop #8")
    parser.add_option("-9", "--bool_p_trima_div", action="store_true", dest="bool_p_trima_div", default=False, help="run loop #9")


    #https://mrjbq7.github.io/ta-lib/func_groups/momentum_indicators.html
    parser.add_option("--10",  action="store_true", dest="bool_p_adx_div", default=False, help="run loop #10")
    parser.add_option("--11",  action="store_true", dest="bool_p_adxr_div", default=False, help="run loop #11")
    parser.add_option("--12",  action="store_true", dest="bool_p_apo_div", default=False, help="run loop #12")
    parser.add_option("--13",  action="store_true", dest="bool_p_aroon_div", default=False, help="run loop #13")
    parser.add_option("--14",  action="store_true", dest="bool_p_aroonosc_div", default=False, help="run loop #14")
    parser.add_option("--15",  action="store_true", dest="bool_p_bop_div", default=False, help="run loop #15")
    parser.add_option("--16",  action="store_true", dest="bool_p_cci_div", default=False, help="run loop #16")
    parser.add_option("--17",  action="store_true", dest="bool_p_cmo_div", default=False, help="run loop #17")
    parser.add_option("--18",  action="store_true", dest="bool_p_dx_div", default=False, help="run loop #18")
    parser.add_option("--19",  action="store_true", dest="bool_p_minusdi_div", default=False, help="run loop #19")
    parser.add_option("--20",  action="store_true", dest="bool_p_minusdm_div", default=False, help="run loop #20")
    parser.add_option("--21",  action="store_true", dest="bool_p_mom_div", default=False, help="run loop #21")
    parser.add_option("--22",  action="store_true", dest="bool_p_plusdi_div", default=False, help="run loop #22")
    parser.add_option("--23",  action="store_true", dest="bool_p_plusdm_div", default=False, help="run loop #23")
    parser.add_option("--24",  action="store_true", dest="bool_p_ppo_div", default=False, help="run loop #24")
    parser.add_option("--25",  action="store_true", dest="bool_p_roc_div", default=False, help="run loop #25")
    parser.add_option("--26",  action="store_true", dest="bool_p_rocp_div", default=False, help="run loop #26")
    parser.add_option("--27",  action="store_true", dest="bool_p_rocr_div", default=False, help="run loop #27")
    parser.add_option("--28",  action="store_true", dest="bool_p_rocr100_div", default=False, help="run loop #28")
    parser.add_option("--29",  action="store_true", dest="bool_p_trix_div", default=False, help="run loop #29")
    parser.add_option("--30",  action="store_true", dest="bool_p_ultosc_div", default=False, help="run loop #30")
    parser.add_option("--31",  action="store_true", dest="bool_p_willr_div", default=False, help="run loop #31")
    parser.add_option("--32",  action="store_true", dest="bool_p_macd_div", default=False, help="run loop #32")
    parser.add_option("--33",  action="store_true", dest="bool_p_macdext_div", default=False, help="run loop #33")
    parser.add_option("--34",  action="store_true", dest="bool_p_macdfix_div", default=False, help="run loop #34")

    #https://mrjbq7.github.io/ta-lib/func_groups/volume_indicators.html
    parser.add_option("--35",  action="store_true", dest="bool_p_ad_div", default=False, help="run loop #35")
    parser.add_option("--36",  action="store_true", dest="bool_p_adosc_div", default=False, help="run loop #36")
    parser.add_option("--37",  action="store_true", dest="bool_p_obv_div", default=False, help="run loop #37")

    #https://mrjbq7.github.io/ta-lib/func_groups/price_transform.html
    parser.add_option("--38",  action="store_true", dest="bool_p_avgprice_div", default=False, help="run loop #38")
    parser.add_option("--39",  action="store_true", dest="bool_p_medprice_div", default=False, help="run loop #39")
    parser.add_option("--40",  action="store_true", dest="bool_p_typprice_div", default=False, help="run loop #40")
    parser.add_option("--41",  action="store_true", dest="bool_p_wclprice_div", default=False, help="run loop #41")


    #https://mrjbq7.github.io/ta-lib/func_groups/cycle_indicators.html
    parser.add_option("--42",  action="store_true", dest="bool_p_htdcperiod_div", default=False, help="run loop #42")
    parser.add_option("--43",  action="store_true", dest="bool_p_htdcphase_div", default=False, help="run loop #43")
    parser.add_option("--44",  action="store_true", dest="bool_p_htphasor_div", default=False, help="run loop #44")
    parser.add_option("--45",  action="store_true", dest="bool_p_htsine_div", default=False, help="run loop #45")
    parser.add_option("--46",  action="store_true", dest="bool_p_httrendmode_div", default=False, help="run loop #46")


    #https://mrjbq7.github.io/ta-lib/func_groups/statistic_functions.html
    parser.add_option("--47",  action="store_true", dest="bool_p_beta_div", default=False, help="run loop #47")
    parser.add_option("--48",  action="store_true", dest="bool_p_correl_div", default=False, help="run loop #48")
    parser.add_option("--49",  action="store_true", dest="bool_p_linearreg_div", default=False, help="run loop #49")
    parser.add_option("--50",  action="store_true", dest="bool_p_linearregangle_div", default=False, help="run loop #50")
    parser.add_option("--51",  action="store_true", dest="bool_p_linearregintercept_div", default=False, help="run loop #51")
    parser.add_option("--52",  action="store_true", dest="bool_p_linearregslope_div", default=False, help="run loop #52")
    parser.add_option("--53",  action="store_true", dest="bool_p_stddev_div", default=False, help="run loop #53")
    parser.add_option("--54",  action="store_true", dest="bool_p_tsf_div", default=False, help="run loop #54")
    parser.add_option("--55",  action="store_true", dest="bool_p_var_div", default=False, help="run loop #55")


    parser.add_option("--56",  action="store_true", dest="bool_p_wma_div", default=False, help="run loop #56")
    parser.add_option("--57",  action="store_true", dest="bool_p_t3_div", default=False, help="run loop #57")
    parser.add_option("--58",  action="store_true", dest="bool_p_sma_div", default=False, help="run loop #58")
    parser.add_option("--59",  action="store_true", dest="bool_p_sarext_div", default=False, help="run loop #59")
    parser.add_option("--60",  action="store_true", dest="bool_p_sar_div", default=False, help="run loop #60")
    parser.add_option("--61",  action="store_true", dest="bool_p_midprice_div", default=False, help="run loop #61")
    parser.add_option("--62",  action="store_true", dest="bool_p_midpoint_div", default=False, help="run loop #62")
    #parser.add_option("--63",  action="store_true", dest="bool_p_mavp_div", default=False, help="run loop #63")
    parser.add_option("--64",  action="store_true", dest="bool_p_mama_div", default=False, help="run loop #64")
    parser.add_option("--65",  action="store_true", dest="bool_p_ma_div", default=False, help="run loop #65")
    parser.add_option("--66",  action="store_true", dest="bool_p_kama_div", default=False, help="run loop #66")
    parser.add_option("--67",  action="store_true", dest="bool_p_httrendline_div", default=False, help="run loop #67")
    parser.add_option("--68",  action="store_true", dest="bool_p_ema_div", default=False, help="run loop #68")
    parser.add_option("--69",  action="store_true", dest="bool_p_dema_div", default=False, help="run loop #69")
    parser.add_option("--70",  action="store_true", dest="bool_p_bbands_div", default=False, help="run loop #70")





(options, args) = parser.parse_args()


for key in options.__dict__:
    if options.bool_check_all:
        if re.match('bool', key):
            options.__dict__[key] = True
            #eval("options."+key+" = True")


debug=options.debug
max_exam_day=options.max_exam_day
single_process=options.single_process
exam_date=options.exam_date
forex=options.forex
merge_only=options.merge_only

bool_check_all=options.bool_check_all

#isApple = True if fruit == 'Apple' else False
if True:
    bool_calc_std_mean=options.bool_calc_std_mean;
    bool_perc_std_mean=options.bool_perc_std_mean
    bool_talib_pattern=options.bool_talib_pattern
    bool_pv_hit=options.bool_pv_hit
    bool_p_mfi_div=options.bool_p_mfi_div
    bool_p_rsi_div=options.bool_p_rsi_div
    bool_p_natr_div=options.bool_p_natr_div
    bool_p_tema_div=options.bool_p_tema_div
    bool_p_trima_div=options.bool_p_trima_div

    bool_p_adx_div=options.bool_p_adx_div
    bool_p_adxr_div=options.bool_p_adxr_div
    bool_p_apo_div=options.bool_p_apo_div
    bool_p_aroon_div=options.bool_p_aroon_div
    bool_p_aroonosc_div=options.bool_p_aroonosc_div
    bool_p_bop_div=options.bool_p_bop_div
    bool_p_cci_div=options.bool_p_cci_div
    bool_p_cmo_div=options.bool_p_cmo_div
    bool_p_dx_div=options.bool_p_dx_div
    bool_p_minusdi_div=options.bool_p_minusdi_div
    bool_p_minusdm_div=options.bool_p_minusdm_div
    bool_p_mom_div=options.bool_p_mom_div
    bool_p_plusdi_div=options.bool_p_plusdi_div
    bool_p_plusdm_div=options.bool_p_plusdm_div
    bool_p_ppo_div=options.bool_p_ppo_div
    bool_p_roc_div=options.bool_p_roc_div
    bool_p_rocp_div=options.bool_p_rocp_div
    bool_p_rocr_div=options.bool_p_rocr_div
    bool_p_rocr100_div=options.bool_p_rocr100_div
    bool_p_trix_div=options.bool_p_trix_div
    bool_p_ultosc_div=options.bool_p_ultosc_div
    bool_p_willr_div=options.bool_p_willr_div
    bool_p_macd_div=options.bool_p_macd_div
    bool_p_macdext_div=options.bool_p_macdext_div
    bool_p_macdfix_div=options.bool_p_macdfix_div


    bool_p_ad_div=options.bool_p_ad_div
    bool_p_adosc_div=options.bool_p_adosc_div
    bool_p_obv_div=options.bool_p_obv_div

    bool_p_avgprice_div=options.bool_p_avgprice_div
    bool_p_medprice_div=options.bool_p_medprice_div
    bool_p_typprice_div=options.bool_p_typprice_div
    bool_p_wclprice_div=options.bool_p_wclprice_div

    bool_p_htdcperiod_div=options.bool_p_htdcperiod_div
    bool_p_htdcphase_div=options.bool_p_htdcphase_div
    bool_p_htphasor_div=options.bool_p_htphasor_div
    bool_p_htsine_div=options.bool_p_htsine_div
    bool_p_httrendmode_div=options.bool_p_httrendmode_div


    bool_p_beta_div=options.bool_p_beta_div
    bool_p_correl_div=options.bool_p_correl_div
    bool_p_linearreg_div=options.bool_p_linearreg_div
    bool_p_linearregangle_div=options.bool_p_linearregangle_div
    bool_p_linearregintercept_div=options.bool_p_linearregintercept_div
    bool_p_linearregslope_div=options.bool_p_linearregslope_div
    bool_p_stddev_div=options.bool_p_stddev_div
    bool_p_tsf_div=options.bool_p_tsf_div
    bool_p_var_div=options.bool_p_var_div

    bool_p_wma_div=options.bool_p_wma_div
    bool_p_t3_div=options.bool_p_t3_div
    bool_p_sma_div=options.bool_p_sma_div
    bool_p_sarext_div=options.bool_p_sarext_div
    bool_p_sar_div=options.bool_p_sar_div
    bool_p_midprice_div=options.bool_p_midprice_div
    bool_p_midpoint_div=options.bool_p_midpoint_div
    #bool_p_mavp_div=options.bool_p_mavp_div
    bool_p_mama_div=options.bool_p_mama_div
    bool_p_ma_div=options.bool_p_ma_div
    bool_p_kama_div=options.bool_p_kama_div
    bool_p_httrendline_div=options.bool_p_httrendline_div
    bool_p_ema_div=options.bool_p_ema_div
    bool_p_dema_div=options.bool_p_dema_div
    bool_p_bbands_div=options.bool_p_bbands_div


#exam_date= ts_ud.last_tddate() #not accurate.


print("debug: "+str(debug))
print("max_exam_day: "+str(max_exam_day))
print("single_process: "+str(single_process))
print("exam_date: "+str(exam_date))
print("forex: "+str(forex))
print("merge_only: "+str(merge_only))

print("bool_check_all: "+str(bool_check_all))
time.sleep(2)

if True:
    print("bool_calc_std_mean, loop #1: "+str(bool_calc_std_mean))
    print("bool_perc_std_mean, loop #2: "+str(bool_perc_std_mean))
    print("bool_talib_pattern, loop #3: "+str(bool_talib_pattern))
    print("bool_pv_hit, loop #4: "+str(bool_pv_hit))
    print("bool_p_mfi_div, loop #5: "+str(bool_p_mfi_div))
    print("bool_p_rsi_div, loop #6: "+str(bool_p_rsi_div))
    print("bool_p_natr_div, loop #7: "+str(bool_p_natr_div))
    print("bool_p_tema_div, loop #8: "+str(bool_p_tema_div))
    print("bool_p_trima_div, loop #9: "+str(bool_p_trima_div))


    print("bool_p_adx_div, loop #10: "+str(bool_p_adx_div))
    print("bool_p_adxr_div, loop #11: "+str(bool_p_adxr_div))
    print("bool_p_apo_div, loop #12: "+str(bool_p_apo_div))
    print("bool_p_aroon_div, loop #13: "+str(bool_p_aroon_div))
    print("bool_p_aroonosc_div, loop #14: "+str(bool_p_aroonosc_div))
    print("bool_p_bop_div, loop #15: "+str(bool_p_bop_div))
    print("bool_p_cci_div, loop #16: "+str(bool_p_cci_div))
    print("bool_p_cmo_div, loop #17: "+str(bool_p_cmo_div))
    print("bool_p_dx_div, loop #18: "+str(bool_p_dx_div))
    print("bool_p_minusdi_div, loop #19: "+str(bool_p_minusdi_div))
    print("bool_p_minusdm_div, loop #20: "+str(bool_p_minusdm_div))
    print("bool_p_mom_div, loop #21: "+str(bool_p_mom_div))
    print("bool_p_plusdi_div, loop #22: "+str(bool_p_plusdi_div))
    print("bool_p_plusdm_div, loop #23: "+str(bool_p_plusdm_div))
    print("bool_p_ppo_div, loop #24:"+str(bool_p_ppo_div))
    print("bool_p_roc_div, loop #25: "+str(bool_p_roc_div))
    print("bool_p_rocp_div, loop #26: "+str(bool_p_rocp_div))
    print("bool_p_rocr_div, loop #27: "+str(bool_p_rocr_div))
    print("bool_p_rocr100_div, loop #28: "+str(bool_p_rocr100_div))
    print("bool_p_trix_div, loop #29: "+str(bool_p_trix_div))
    print("bool_p_ultosc_div, loop #30: "+str(bool_p_ultosc_div))
    print("bool_p_willr_div, loop #31: "+str(bool_p_willr_div))
    print("bool_p_macd_div, loop #32: "+str(bool_p_macd_div))
    print("bool_p_macdext_div, loop #33: "+str(bool_p_macdext_div))
    print("bool_p_macdfix_div, loop #34: "+str(bool_p_macdfix_div))


    print("bool_p_ad_div, loop #35: "+str(bool_p_ad_div))
    print("bool_p_adosc_div, loop #36: "+str(bool_p_adosc_div))
    print("bool_p_obv_div, loop #37: "+str(bool_p_obv_div))

    print("bool_p_avgprice_div, loop #38: "+str(bool_p_avgprice_div))
    print("bool_p_medprice_div, loop #39: "+str(bool_p_medprice_div))
    print("bool_p_typprice_div, loop #40: "+str(bool_p_typprice_div))
    print("bool_p_wclprice_div, loop #41: "+str(bool_p_wclprice_div))

    print("bool_p_htdcperiod_div, loop #42: "+str(bool_p_htdcperiod_div))
    print("bool_p_htdcphase_div, loop #43: "+str(bool_p_htdcphase_div))
    print("bool_p_htphasor_div, loop #44: "+str(bool_p_htphasor_div))
    print("bool_p_htsine_div, loop #45: "+str(bool_p_htsine_div))
    print("bool_p_httrendmode_div, loop #46: "+str(bool_p_httrendmode_div))

    print("bool_p_beta_div, loop #47: "+str(bool_p_beta_div))
    print("bool_p_correl_div, loop #48: "+str(bool_p_correl_div))
    print("bool_p_linearreg_div, loop #49: "+str(bool_p_linearreg_div))
    print("bool_p_linearregangle_div, loop #50: "+str(bool_p_linearregangle_div))
    print("bool_p_linearregintercept_div, loop #51: "+str(bool_p_linearregintercept_div))
    print("bool_p_linearregslope_div, loop #52: "+str(bool_p_linearregslope_div))
    print("bool_p_stddev_div, loop #53: "+str(bool_p_stddev_div))
    print("bool_p_tsf_div, loop #54: "+str(bool_p_tsf_div))
    print("bool_p_var_div, loop #55: "+str(bool_p_var_div))


    print("bool_p_wma_div, loop #56: "+str(bool_p_wma_div))
    print("bool_p_t3_div, loop #57: "+str(bool_p_t3_div))
    print("bool_p_sma_div, loop #58: "+str(bool_p_sma_div))
    print("bool_p_sarext_div, loop #59: "+str(bool_p_sarext_div))
    print("bool_p_sar_div, loop #60: "+str(bool_p_sar_div))
    print("bool_p_midprice_div, loop #61: "+str(bool_p_midprice_div))
    print("bool_p_midpoint_div, loop #62: "+str(bool_p_midpoint_div))
    #print("bool_p_mavp_div, loop #63: "+str(bool_p_mavp_div))
    print("bool_p_mama_div, loop #64: "+str(bool_p_mama_div))
    print("bool_p_ma_div, loop #65: "+str(bool_p_ma_div))
    print("bool_p_kama_div, loop #66: "+str(bool_p_kama_div))
    print("bool_p_httrendline_div, loop #67: "+str(bool_p_httrendline_div))
    print("bool_p_ema_div, loop #68: "+str(bool_p_ema_div))
    print("bool_p_dema_div, loop #69: "+str(bool_p_dema_div))
    print("bool_p_bbands_div, loop #70: "+str(bool_p_bbands_div))



if exam_date is None:
    exam_date = finlib.Finlib().get_last_trading_day()
    print("exam_date reset to: "+exam_date)

#exit(1)

progress_run = 0
toal_run = 0


global lock
lock = multiprocessing.Lock()


def get_open_order_count(code, BS, engine):
    # Don't place order if have same direction opened
    sql = 'SELECT * FROM `order_tracking_forex` WHERE `code` = \"' + code + '\" AND `BS` =\"' + BS + '\" AND status IN( \"NEW\", \"PLACED\")'
    open_record = pd.read_sql_query(sql, engine)
    return (open_record.__len__())


def resample_to_candle(df):
    #input:
    #       code                 date         o         h         l         c   vol
    #0   EUR_USD  2018-02-19 16:44:30  1.239860  1.239905  1.239830  1.239855  12.0
    #1   EUR_USD  2018-02-19 16:45:30  1.239970  1.23997  1.239955  1.239955   6.0


    #output:
    #                       vol         h         c         l         o
    #date
    #2018-02-19 16:45:00  163.0  1.240500  1.240475  1.239870  1.239905
    #2018-02-19 16:50:00  144.0  1.240580  1.240455  1.240200  1.240480


    df2 = df.set_index('date')
    df2.index = pd.to_datetime(df2.index)
    df2 = df2.drop(['code'], axis=1)

    ohlc_dict = {
        'o': 'first',
        'h': 'max',
        'l': 'min',
        'c': 'last',
        'vol': 'sum'
    }

    df2=df2.resample('5T', how=ohlc_dict, closed='left', label='left')
    return df2



def calc_init(array):
    global progress_run
    global toal_f_cnt
    inputF = array['inputF']
    outputF = array['outputF']
    outputF_today = array['outputF_today']
    exam_date = array['exam_date'] #not important

    shortFile = inputF.split("/")[-1]  #EUR_USD.txt
    code = re.match('(.*)\.txt', shortFile).group(1) #EUR_USD

    #lock.acquire()
    #progress_run += 1
    #print("Work on file " + inputF + ", last date is " + exam_date + ", run " + str(progress_run) + "/" + str(toal_run))
    #lock.release()

    opt={}
    opt['debug']=debug
    opt['forex']=forex

    if True:
        opt['bool_calc_std_mean']=bool_calc_std_mean
        opt['bool_perc_std_mean']=bool_perc_std_mean
        opt['bool_talib_pattern']=bool_talib_pattern
        opt['bool_pv_hit']=bool_pv_hit
        opt['bool_p_mfi_div']=bool_p_mfi_div
        opt['bool_p_rsi_div']=bool_p_rsi_div
        opt['bool_p_natr_div']=bool_p_natr_div
        opt['bool_p_tema_div']=bool_p_tema_div
        opt['bool_p_trima_div']=bool_p_trima_div



        opt['bool_p_adx_div']=bool_p_adx_div
        opt['bool_p_adxr_div']=bool_p_adxr_div
        opt['bool_p_apo_div']=bool_p_apo_div
        opt['bool_p_aroon_div']=bool_p_aroon_div
        opt['bool_p_aroonosc_div']=bool_p_aroonosc_div
        opt['bool_p_bop_div']=bool_p_bop_div
        opt['bool_p_cci_div']=bool_p_cci_div
        opt['bool_p_cmo_div']=bool_p_cmo_div
        opt['bool_p_dx_div']=bool_p_dx_div
        opt['bool_p_minusdi_div']=bool_p_minusdi_div
        opt['bool_p_minusdm_div']=bool_p_minusdm_div
        opt['bool_p_mom_div']=bool_p_mom_div
        opt['bool_p_plusdi_div']=bool_p_plusdi_div
        opt['bool_p_plusdm_div']=bool_p_plusdm_div
        opt['bool_p_ppo_div']=bool_p_ppo_div
        opt['bool_p_roc_div']=bool_p_roc_div
        opt['bool_p_rocp_div']=bool_p_rocp_div
        opt['bool_p_rocr_div']=bool_p_rocr_div
        opt['bool_p_rocr100_div']=bool_p_rocr100_div
        opt['bool_p_trix_div']=bool_p_trix_div
        opt['bool_p_ultosc_div']=bool_p_ultosc_div
        opt['bool_p_willr_div']=bool_p_willr_div
        opt['bool_p_macd_div']=bool_p_macd_div
        opt['bool_p_macdext_div']=bool_p_macdext_div
        opt['bool_p_macdfix_div']=bool_p_macdfix_div

        opt['bool_p_ad_div']=bool_p_ad_div
        opt['bool_p_adosc_div']=bool_p_adosc_div
        opt['bool_p_obv_div']=bool_p_obv_div

        opt['bool_p_avgprice_div']=bool_p_avgprice_div
        opt['bool_p_medprice_div']=bool_p_medprice_div
        opt['bool_p_typprice_div']=bool_p_typprice_div
        opt['bool_p_wclprice_div']=bool_p_wclprice_div



        opt['bool_p_htdcperiod_div']=bool_p_htdcperiod_div
        opt['bool_p_htdcphase_div']=bool_p_htdcphase_div
        opt['bool_p_htphasor_div']=bool_p_htphasor_div
        opt['bool_p_htsine_div']=bool_p_htsine_div
        opt['bool_p_httrendmode_div']=bool_p_httrendmode_div


        opt['bool_p_beta_div']=bool_p_beta_div
        opt['bool_p_correl_div']=bool_p_correl_div
        opt['bool_p_linearreg_div']=bool_p_linearreg_div
        opt['bool_p_linearregangle_div']=bool_p_linearregangle_div
        opt['bool_p_linearregintercept_div']=bool_p_linearregintercept_div
        opt['bool_p_linearregslope_div']=bool_p_linearregslope_div
        opt['bool_p_stddev_div']=bool_p_stddev_div
        opt['bool_p_tsf_div']=bool_p_tsf_div
        opt['bool_p_var_div']=bool_p_var_div


        opt['bool_p_wma_div']=bool_p_wma_div
        opt['bool_p_t3_div']=bool_p_t3_div
        opt['bool_p_sma_div']=bool_p_sma_div
        opt['bool_p_sarext_div']=bool_p_sarext_div
        opt['bool_p_sar_div']=bool_p_sar_div
        opt['bool_p_midprice_div']=bool_p_midprice_div
        opt['bool_p_midpoint_div']=bool_p_midpoint_div
        #opt['bool_p_mavp_div']=bool_p_mavp_div
        opt['bool_p_mama_div']=bool_p_mama_div
        opt['bool_p_ma_div']=bool_p_ma_div
        opt['bool_p_kama_div']=bool_p_kama_div
        opt['bool_p_httrendline_div']=bool_p_httrendline_div
        opt['bool_p_ema_div']=bool_p_ema_div
        opt['bool_p_dema_div']=bool_p_dema_div
        opt['bool_p_bbands_div']=bool_p_bbands_div

    mysql_host = '127.0.0.1'
    #mysql_host = 'td'

    #used by pandas read from db
    #engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
    engine = create_engine('mysql+mysqldb://root:admin888.@_@@'+mysql_host+'/ryan_stock_db?charset=utf8')

    #used by python mysql.connector write to db
    cnx = mysql.connector.connect(host=mysql_host, user='root', password='admin888.@_@',database="ryan_stock_db")
    cursor = cnx.cursor()


    try:
        #####################
        #
        # Get Candles
        #####################
        NOC = 30  # number of candle
        SOC = 'M1'  # size of candle.

        print(str(datetime.datetime.now()) + " retriving candles: " + code + ", Size of candle: " + str(
            SOC) + " Min, number of last candles: " + str(NOC))
        df = t_ph_lib.v20C().simple_get_candles(instrument=code, granularity=SOC, count=NOC)
        print(str(datetime.datetime.now()) + " NOTE: retrieved candles "+code+" "+str(SOC) +" "+str(NOC))

        exam_date = df.iloc[df.__len__() - 1]['date']

        #####################
        #
        # Analyse Candles
        #####################
        print(str(datetime.datetime.now()) + " evaluating candles.")
        (df1, df_result) = finlib.Finlib().calc(opt, df,outputF,outputF_today,exam_date,live_trading=True)
        print(str(datetime.datetime.now()) + " NOTE: evaluated candles, hitted records "+str(df_result.__len__()))


        #####################
        #
        # Query DB to filter analysing result
        #####################
        for index, row in df_result.iterrows():
            #print(str(datetime.datetime.now()) + " "+row['code'],row['date'],row['op_rsn'],row['close_p'],row['op_strength'])


            #critical Sql
            #SELECT `3med`, `3mea`, `240med`, `240mea`, `3uc` ,`3dc`, `10uc` ,`10dc`, `60uc` ,`60dc`,`120uc` ,`120dc`, `240uc` ,`240dc` FROM `pattern_perf_forex` WHERE (3uc/30dc > 2 AND 3dc > 20)  ORDER BY `3uc`/`3dc` DESC

            #sql = "SELECT * FROM `pattern_perf_forex` WHERE `pattern` = \"EURUSD_B_talib_CDLHARAMI\" "
            sql = 'SELECT * FROM `pattern_perf_forex` WHERE `pattern` = \"'+row['op_rsn']+'\"'

            print(str(datetime.datetime.now()) + " NOTE: Hit a Ptn,  query pattern perf "+ row['op_rsn']+" in DB")

            df_db_record = pd.read_sql_query(sql, engine, index_col='ID')

            if df_db_record.__len__() <= 0: #no perf result in DB.  The number should be 1. (only one record in db)
                print(str(datetime.datetime.now()) + " NOTE: No such ptn perf record in DB, ignore "+ row['op_rsn'])
                continue
            else:
                print(str(datetime.datetime.now()) + " NOTE: Found ptn perf record in DB " + row['op_rsn'])


            pp = df_db_record.iloc[0] #pattern performance

            operation={}
            operation['BS']="NA"

            #Buy condiction check
            #if (2uc > 2dc * 3) and (2uc > 200) and (5uc > 5dc * 3) and (5uc > 200) :
            if (pp['2uc'] > pp['2dc'] * 3) and (pp['2dc'] > 30):
                #if (pp['2mea'] > 0.01) and pp['5mea'] > pp['2mea']:
                if (pp['2mea'] > 0.0001):
                    operation['BS']="B"
                    operation['open_time']=row['date']
                    operation['code'] = row['code']
                    operation['open_reason']=str(datetime.datetime.now())+" "+row['op_rsn']
                    operation['open_price']=float(row['close_p'])
                    operation['hold_time']="120" #2minutes
                    operation['stop_lost']=float(row['close_p'] - pp['2med'])
                    operation['take_profit']=float(row['close_p'] + pp['2med'])

                    operation['his_max_win']=float(pp['2max'])
                    operation['his_max_lost']=float(pp['2min'])
                    operation['his_avg']=float(pp['2mea'])
                    operation['his_mid']=float(pp['2med'])

                    if operation['stop_lost'] < 0 or operation['take_profit'] < 0 or operation['open_price'] < 0:
                        continue

                    if get_open_order_count(operation['code'],operation['BS'],engine ) > 0:
                        print(str(datetime.datetime.now()) + " NOTE: Already have the openning " + operation['BS'] + " direction on " + operation[
                            'code'] + ", will not open new dup order.")
                        continue

            # sell condition check
            if (pp['2dc'] > pp['2uc'] * 3) and (pp['2dc'] > 30):
            #if True: #ryan debug
                if (pp['2mea'] < -0.0001):
                #if True:
                    operation['BS']="S"
                    operation['open_time']=row['date']
                    operation['code']=row['code']
                    operation['open_reason']=str(datetime.datetime.now())+" "+row['op_rsn']
                    operation['open_price']=float(row['close_p'])
                    operation['hold_time']="120" #2minutes
                    operation['stop_lost']=float(row['close_p'] + pp['2med'])
                    operation['take_profit']=float(row['close_p'] - pp['2med'])

                    operation['his_max_win'] = float(pp['2max'])
                    operation['his_max_lost'] = float(pp['2min'])
                    operation['his_avg'] = float(pp['2mea'])
                    operation['his_mid'] = float(pp['2med'])

                    if operation['stop_lost'] < 0 or operation['take_profit'] < 0 or operation['open_price']<0:
                        continue

                    if get_open_order_count(operation['code'],operation['BS'],engine ) > 0:
                        print(str(datetime.datetime.now()) + " NOTE: Already have the openning " + operation['BS'] + " direction on " + operation[
                            'code'] + ", will not open new dup order.")
                        continue




            # The creating order record is decided when running into this step.
            #operation['open_epoch_plan'] = calendar.timegm(time.gmtime())
            #operation['close_epoch_plan'] = calendar.timegm(time.gmtime())  + 120

            #operation['open_epoch_plan'] =  (datetime.datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%S.%f000Z')   - datetime.datetime(1970,1,1)).total_seconds()
            operation['open_epoch_plan'] =  int(datetime.datetime.now().strftime('%s'))
            #operation['close_epoch_plan'] = operation['open_epoch_plan']  + 120  #2 mins
            operation['close_epoch_plan'] = operation['open_epoch_plan']  + 1200 #20 mins

            #update the result to DB
            if operation['BS'] != 'NA':
                add_order_tracking_forex = ("INSERT INTO order_tracking_forex "

                                      "(oanda_number, code, status,status_revert, BS, open_time, open_reason, open_price, open_epoch_plan, close_epoch_plan, last_review_time, order_change_hist, his_max_win, his_max_lost, his_avg, his_mid, R, expected_increase, expected_hold_time, expected_decrease_max, exit_critrial, stop_lost, take_profit_stop) "

                                      "VALUES (%(oanda_number)s, %(code)s, %(status)s, %(status_revert)s, %(BS)s, %(open_time)s, %(open_reason)s, %(open_price)s,%(open_epoch_plan)s,%(close_epoch_plan)s, %(last_review_time)s, %(order_change_hist)s, %(his_max_win)s, %(his_max_lost)s, %(his_avg)s, %(his_mid)s, %(R)s, %(expected_increase)s, %(expected_hold_time)s, %(expected_decrease_max)s, %(exit_critrial)s, %(stop_lost)s, %(take_profit_stop)s)")

                data_order_tracking_forex = {
                    'oanda_number':0,
                    'code': operation['code'],
                    'status': "NEW",
                    'status_revert': "NEW",
                    'BS': operation['BS'],

                    'open_time': operation['open_time'],
                    'open_reason': operation['open_reason'],
                    'open_price': operation['open_price'],
                    'open_epoch_plan': operation['open_epoch_plan'],
                    'close_epoch_plan': operation['close_epoch_plan'],
                    'last_review_time': datetime.datetime.now(),
                    'order_change_hist': "",
                    'his_max_win': operation['his_max_win'],
                    'his_max_lost': operation['his_max_lost'],
                    'his_avg': operation['his_avg'],
                    'his_mid': operation['his_mid'],
                    'R': operation['his_avg'],
                    'expected_increase': operation['his_mid'],
                    'expected_hold_time': operation['hold_time'],
                    'expected_decrease_max': operation['his_max_lost'],
                    'exit_critrial': "",
                    'stop_lost': operation['stop_lost'],
                    'take_profit_stop': operation['take_profit'],
                }


                cursor.execute(add_order_tracking_forex, data_order_tracking_forex)
                cnx.commit()
                print(str(datetime.datetime.now()) + " inserted new operation to db" \
                      + "," +operation['BS'] +"," + str(operation['code']) \
                      + "," +operation['open_reason'] \
                      +","+ operation['open_time']+","+ str(operation['open_price']))

       # print(1)
    except:
        traceback.print_exc(file=sys.stdout)
        print("catched exception. outputF is "+outputF)
    finally:
        engine.dispose()
        cursor.close()
        cnx.close()



### Main Start ###

root_dir ='/home/ryan/DATA/DAY_Forex'

if debug:
    root_dir = '/home/ryan/DATA/DAY_Forex_debug'


print("root_dir "+root_dir)

array = []

#if debug:
#    pool=Pool( processes=1, initializer=init, initargs=(l,)) #leave one core free
#else:
#    pool = Pool(processes=process_cnt, initializer=init, initargs=(l,))  # leave one core free

if False:
    if os.path.isdir("/home/ryan/DATA/tmp/pv_today.del"):
        shutil.rmtree("/home/ryan/DATA/tmp/pv_today.del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv.del"):
        shutil.rmtree("/home/ryan/DATA/tmp/pv.del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv_today"):
        os.rename("/home/ryan/DATA/tmp/pv_today", "/home/ryan/DATA/tmp/pv_today.del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv"):
        os.rename("/home/ryan/DATA/tmp/pv", "/home/ryan/DATA/tmp/pv.del")

    os.mkdir("/home/ryan/DATA/tmp/pv_today")
    os.mkdir("/home/ryan/DATA/tmp/pv")

    # multicore implmentation:
    for root, dirs, files in os.walk(root_dir):
        pass

cpu_count = multiprocessing.cpu_count()

(inst_dict,code_to_instrument_dict) = t_ph_lib.v20C().get_instruments()

instruments = inst_dict.keys()

files = []
for instrument in instruments:
    if inst_dict[instrument]['type'] in ('CFD', 'METAL'):
        print("ignore the instrument type other than CURRENCY: " + inst_dict[instrument]['type'])
        continue

    files.append(instrument+".txt")

while True:
    if single_process:
        process_cnt = 1
        toal_run = int(files.__len__())

        for file in files:
            array = ({
            'inputF': root_dir + "/" + file,
            'outputF': "/home/ryan/DATA/tmp/pv/" + file,
            'outputF_today': "/home/ryan/DATA/tmp/pv_today/" + file,
            'exam_date': exam_date,})  # array of dict
            calc_init(array)

        print(str(datetime.datetime.now()) + " single process loop completed.\n")
    else:
        process_cnt = cpu_count
        toal_run = int(files.__len__() / (cpu_count)) + 1

        # each code generate a csv
        for file in files:
            array.append({'inputF': root_dir + "/" + file,
                          'outputF': "/home/ryan/DATA/tmp/pv/" + file,
                          'outputF_today': "/home/ryan/DATA/tmp/pv_today/" + file,
                          'exam_date': exam_date, })  # array of dict


        #pool = Pool(processes=process_cnt, initializer=init, initargs=(l,))
        pool = Pool(processes=process_cnt)
        pool.map(calc_init, array)

        pool.close()
        
        pool.join()

print("Script completed")
