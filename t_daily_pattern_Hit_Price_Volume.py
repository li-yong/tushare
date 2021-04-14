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
import finlib
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
import traceback

pd.set_option("display.max_rows", 99999)
pd.set_option("display.max_columns", 100)
pd.set_option('expand_frame_repr', False)

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#This script run daily after the marketing closed,
#     It need be ran after the csv file updated today's data.
#It show the stocks which meet patter (buy or sell) point.
#The result can be a reference for next day's trading.

#step1: update DATA/DAY
#Step2: vim t_daily_pattern_Hit_Price_Volume.py : debug=false, max_exam_day = 22
#Step3: python t_daily_pattern_Hit_Price_Volume.py. Then today's talib and pv hit after filtering DB are saved to DATA/result/today/pv.csv
#
parser = OptionParser()

parser.add_option("-m", "--max_exam_day", dest="max_exam_day", default=221, type="int", help="max_exam_day, 300000 for perf gathering, default 221 for daily ptn checking")

parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="enable debug, use in development purpose")

parser.add_option("-s", "--single_process", action="store_true", dest="single_process", default=False, help="using single process, otherwise using default multiple process")

parser.add_option("-e", "--exam_date", dest="exam_date", help="exam_date, YYYY-MM--DD, no default value, missing will calc the nearest trading day, most time is today")

parser.add_option("-l", "--local_source", action="store_true", dest="local_source", default=False, help="use local source, use when need modify source files. " "with -f --> /home/ryan/DATA/DAY_Forex_local, without -f /home/ryan/DATA/DAY_local")
parser.add_option("-f", "--forex", action="store_true", dest="forex", default=False, help="handle Forex, source is /home/ryan/DATA/DAY_Forex")

parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(A G)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

parser.add_option("-g", "--merge_today_only", action="store_true", dest="merge_only", default=False, help="skip calc DAY source, just merge the result from /home/ryan/DATA/tmp/pv_today")
parser.add_option("-0", "--bool_check_all", action="store_true", dest="bool_check_all", default=False, help="run all check")

parser.add_option("-1", "--bool_calc_std_mean", action="store_true", dest="bool_calc_std_mean", default=False, help="run loop #1")
parser.add_option("-2", "--bool_perc_std_mean", action="store_true", dest="bool_perc_std_mean", default=False, help="run loop #2")
parser.add_option("-3", "--bool_talib_pattern", action="store_true", dest="bool_talib_pattern", default=False, help="run loop #3")
parser.add_option("-4", "--bool_pv_hit", action="store_true", dest="bool_pv_hit", default=False, help="run loop #4")

parser.add_option("-5", "--bool_p_mfi_div", action="store_true", dest="bool_p_mfi_div", default=False, help="run loop #5")
parser.add_option("-6", "--bool_p_rsi_div", action="store_true", dest="bool_p_rsi_div", default=False, help="run loop #6")
parser.add_option("-7", "--bool_p_natr_div", action="store_true", dest="bool_p_natr_div", default=False, help="run loop #7")
parser.add_option("-8", "--bool_p_tema_div", action="store_true", dest="bool_p_tema_div", default=False, help="run loop #8")
parser.add_option("-9", "--bool_p_trima_div", action="store_true", dest="bool_p_trima_div", default=False, help="run loop #9")

#https://mrjbq7.github.io/ta-lib/func_groups/momentum_indicators.html
parser.add_option("--10", action="store_true", dest="bool_p_adx_div", default=False, help="run loop #10")
parser.add_option("--11", action="store_true", dest="bool_p_adxr_div", default=False, help="run loop #11")
parser.add_option("--12", action="store_true", dest="bool_p_apo_div", default=False, help="run loop #12")
parser.add_option("--13", action="store_true", dest="bool_p_aroon_div", default=False, help="run loop #13")
parser.add_option("--14", action="store_true", dest="bool_p_aroonosc_div", default=False, help="run loop #14")
parser.add_option("--15", action="store_true", dest="bool_p_bop_div", default=False, help="run loop #15")
parser.add_option("--16", action="store_true", dest="bool_p_cci_div", default=False, help="run loop #16")
parser.add_option("--17", action="store_true", dest="bool_p_cmo_div", default=False, help="run loop #17")
parser.add_option("--18", action="store_true", dest="bool_p_dx_div", default=False, help="run loop #18")
parser.add_option("--19", action="store_true", dest="bool_p_minusdi_div", default=False, help="run loop #19")
parser.add_option("--20", action="store_true", dest="bool_p_minusdm_div", default=False, help="run loop #20")
parser.add_option("--21", action="store_true", dest="bool_p_mom_div", default=False, help="run loop #21")
parser.add_option("--22", action="store_true", dest="bool_p_plusdi_div", default=False, help="run loop #22")
parser.add_option("--23", action="store_true", dest="bool_p_plusdm_div", default=False, help="run loop #23")
parser.add_option("--24", action="store_true", dest="bool_p_ppo_div", default=False, help="run loop #24")
parser.add_option("--25", action="store_true", dest="bool_p_roc_div", default=False, help="run loop #25")
parser.add_option("--26", action="store_true", dest="bool_p_rocp_div", default=False, help="run loop #26")
parser.add_option("--27", action="store_true", dest="bool_p_rocr_div", default=False, help="run loop #27")
parser.add_option("--28", action="store_true", dest="bool_p_rocr100_div", default=False, help="run loop #28")
parser.add_option("--29", action="store_true", dest="bool_p_trix_div", default=False, help="run loop #29")
parser.add_option("--30", action="store_true", dest="bool_p_ultosc_div", default=False, help="run loop #30")
parser.add_option("--31", action="store_true", dest="bool_p_willr_div", default=False, help="run loop #31")
parser.add_option("--32", action="store_true", dest="bool_p_macd_div", default=False, help="run loop #32")
parser.add_option("--33", action="store_true", dest="bool_p_macdext_div", default=False, help="run loop #33")
parser.add_option("--34", action="store_true", dest="bool_p_macdfix_div", default=False, help="run loop #34")

#https://mrjbq7.github.io/ta-lib/func_groups/volume_indicators.html
parser.add_option("--35", action="store_true", dest="bool_p_ad_div", default=False, help="run loop #35")
parser.add_option("--36", action="store_true", dest="bool_p_adosc_div", default=False, help="run loop #36")
parser.add_option("--37", action="store_true", dest="bool_p_obv_div", default=False, help="run loop #37")

#https://mrjbq7.github.io/ta-lib/func_groups/price_transform.html
parser.add_option("--38", action="store_true", dest="bool_p_avgprice_div", default=False, help="run loop #38")
parser.add_option("--39", action="store_true", dest="bool_p_medprice_div", default=False, help="run loop #39")
parser.add_option("--40", action="store_true", dest="bool_p_typprice_div", default=False, help="run loop #40")
parser.add_option("--41", action="store_true", dest="bool_p_wclprice_div", default=False, help="run loop #41")

#https://mrjbq7.github.io/ta-lib/func_groups/cycle_indicators.html
parser.add_option("--42", action="store_true", dest="bool_p_htdcperiod_div", default=False, help="run loop #42")
parser.add_option("--43", action="store_true", dest="bool_p_htdcphase_div", default=False, help="run loop #43")
parser.add_option("--44", action="store_true", dest="bool_p_htphasor_div", default=False, help="run loop #44")
parser.add_option("--45", action="store_true", dest="bool_p_htsine_div", default=False, help="run loop #45")
parser.add_option("--46", action="store_true", dest="bool_p_httrendmode_div", default=False, help="run loop #46")

#https://mrjbq7.github.io/ta-lib/func_groups/statistic_functions.html
parser.add_option("--47", action="store_true", dest="bool_p_beta_div", default=False, help="run loop #47")
parser.add_option("--48", action="store_true", dest="bool_p_correl_div", default=False, help="run loop #48")
parser.add_option("--49", action="store_true", dest="bool_p_linearreg_div", default=False, help="run loop #49")
parser.add_option("--50", action="store_true", dest="bool_p_linearregangle_div", default=False, help="run loop #50")
parser.add_option("--51", action="store_true", dest="bool_p_linearregintercept_div", default=False, help="run loop #51")
parser.add_option("--52", action="store_true", dest="bool_p_linearregslope_div", default=False, help="run loop #52")
parser.add_option("--53", action="store_true", dest="bool_p_stddev_div", default=False, help="run loop #53")
parser.add_option("--54", action="store_true", dest="bool_p_tsf_div", default=False, help="run loop #54")
parser.add_option("--55", action="store_true", dest="bool_p_var_div", default=False, help="run loop #55")

parser.add_option("--56", action="store_true", dest="bool_p_wma_div", default=False, help="run loop #56")
parser.add_option("--57", action="store_true", dest="bool_p_t3_div", default=False, help="run loop #57")
parser.add_option("--58", action="store_true", dest="bool_p_sma_div", default=False, help="run loop #58")
parser.add_option("--59", action="store_true", dest="bool_p_sarext_div", default=False, help="run loop #59")
parser.add_option("--60", action="store_true", dest="bool_p_sar_div", default=False, help="run loop #60")
parser.add_option("--61", action="store_true", dest="bool_p_midprice_div", default=False, help="run loop #61")
parser.add_option("--62", action="store_true", dest="bool_p_midpoint_div", default=False, help="run loop #62")
#parser.add_option("--63",  action="store_true", dest="bool_p_mavp_div", default=False, help="run loop #63")
parser.add_option("--64", action="store_true", dest="bool_p_mama_div", default=False, help="run loop #64")
parser.add_option("--65", action="store_true", dest="bool_p_ma_div", default=False, help="run loop #65")
parser.add_option("--66", action="store_true", dest="bool_p_kama_div", default=False, help="run loop #66")
parser.add_option("--67", action="store_true", dest="bool_p_httrendline_div", default=False, help="run loop #67")
parser.add_option("--68", action="store_true", dest="bool_p_ema_div", default=False, help="run loop #68")
parser.add_option("--69", action="store_true", dest="bool_p_dema_div", default=False, help="run loop #69")
parser.add_option("--70", action="store_true", dest="bool_p_bbands_div", default=False, help="run loop #70")

(options, args) = parser.parse_args()

for key in options.__dict__:
    if options.bool_check_all:
        if re.match('bool', key):
            options.__dict__[key] = True
            #eval("options."+key+" = True")

debug = options.debug
max_exam_day = options.max_exam_day
single_process = options.single_process
exam_date = options.exam_date
forex = options.forex
stock_global = options.stock_global
selected = options.selected

if stock_global is None:
    logging.info(__file__+" "+"-x --stock_global is None, check help for available options, program exit")
    exit(0)

merge_only = options.merge_only
local_source = options.local_source
bool_check_all = options.bool_check_all

#isApple = True if fruit == 'Apple' else False

bool_calc_std_mean = options.bool_calc_std_mean
bool_perc_std_mean = options.bool_perc_std_mean
bool_talib_pattern = options.bool_talib_pattern
bool_pv_hit = options.bool_pv_hit
bool_p_mfi_div = options.bool_p_mfi_div
bool_p_rsi_div = options.bool_p_rsi_div
bool_p_natr_div = options.bool_p_natr_div
bool_p_tema_div = options.bool_p_tema_div
bool_p_trima_div = options.bool_p_trima_div

bool_p_adx_div = options.bool_p_adx_div
bool_p_adxr_div = options.bool_p_adxr_div
bool_p_apo_div = options.bool_p_apo_div
bool_p_aroon_div = options.bool_p_aroon_div
bool_p_aroonosc_div = options.bool_p_aroonosc_div
bool_p_bop_div = options.bool_p_bop_div
bool_p_cci_div = options.bool_p_cci_div
bool_p_cmo_div = options.bool_p_cmo_div
bool_p_dx_div = options.bool_p_dx_div
bool_p_minusdi_div = options.bool_p_minusdi_div
bool_p_minusdm_div = options.bool_p_minusdm_div
bool_p_mom_div = options.bool_p_mom_div
bool_p_plusdi_div = options.bool_p_plusdi_div
bool_p_plusdm_div = options.bool_p_plusdm_div
bool_p_ppo_div = options.bool_p_ppo_div
bool_p_roc_div = options.bool_p_roc_div
bool_p_rocp_div = options.bool_p_rocp_div
bool_p_rocr_div = options.bool_p_rocr_div
bool_p_rocr100_div = options.bool_p_rocr100_div
bool_p_trix_div = options.bool_p_trix_div
bool_p_ultosc_div = options.bool_p_ultosc_div
bool_p_willr_div = options.bool_p_willr_div
bool_p_macd_div = options.bool_p_macd_div
bool_p_macdext_div = options.bool_p_macdext_div
bool_p_macdfix_div = options.bool_p_macdfix_div

bool_p_ad_div = options.bool_p_ad_div
bool_p_adosc_div = options.bool_p_adosc_div
bool_p_obv_div = options.bool_p_obv_div

bool_p_avgprice_div = options.bool_p_avgprice_div
bool_p_medprice_div = options.bool_p_medprice_div
bool_p_typprice_div = options.bool_p_typprice_div
bool_p_wclprice_div = options.bool_p_wclprice_div

bool_p_htdcperiod_div = options.bool_p_htdcperiod_div
bool_p_htdcphase_div = options.bool_p_htdcphase_div
bool_p_htphasor_div = options.bool_p_htphasor_div
bool_p_htsine_div = options.bool_p_htsine_div
bool_p_httrendmode_div = options.bool_p_httrendmode_div

bool_p_beta_div = options.bool_p_beta_div
bool_p_correl_div = options.bool_p_correl_div
bool_p_linearreg_div = options.bool_p_linearreg_div
bool_p_linearregangle_div = options.bool_p_linearregangle_div
bool_p_linearregintercept_div = options.bool_p_linearregintercept_div
bool_p_linearregslope_div = options.bool_p_linearregslope_div
bool_p_stddev_div = options.bool_p_stddev_div
bool_p_tsf_div = options.bool_p_tsf_div
bool_p_var_div = options.bool_p_var_div

bool_p_wma_div = options.bool_p_wma_div
bool_p_t3_div = options.bool_p_t3_div
bool_p_sma_div = options.bool_p_sma_div
bool_p_sarext_div = options.bool_p_sarext_div
bool_p_sar_div = options.bool_p_sar_div
bool_p_midprice_div = options.bool_p_midprice_div
bool_p_midpoint_div = options.bool_p_midpoint_div
#bool_p_mavp_div=options.bool_p_mavp_div
bool_p_mama_div = options.bool_p_mama_div
bool_p_ma_div = options.bool_p_ma_div
bool_p_kama_div = options.bool_p_kama_div
bool_p_httrendline_div = options.bool_p_httrendline_div
bool_p_ema_div = options.bool_p_ema_div
bool_p_dema_div = options.bool_p_dema_div
bool_p_bbands_div = options.bool_p_bbands_div

#exam_date= ts_ud.last_tddate() #not accurate.

logging.info(__file__+" "+"debug: " + str(debug))
logging.info(__file__+" "+"max_exam_day: " + str(max_exam_day))
logging.info(__file__+" "+"single_process: " + str(single_process))
logging.info(__file__+" "+"exam_date: " + str(exam_date))
logging.info(__file__+" "+"forex: " + str(forex))
logging.info(__file__+" "+"stock_global: " + str(stock_global))
logging.info(__file__+" "+"merge_only: " + str(merge_only))
logging.info(__file__+" "+"local_source: " + str(local_source))

logging.info(__file__+" "+"bool_check_all: " + str(bool_check_all))
time.sleep(2)

if False:
    logging.info(__file__+" "+"bool_calc_std_mean, loop #1: " + str(bool_calc_std_mean))
    logging.info(__file__+" "+"bool_perc_std_mean, loop #2: " + str(bool_perc_std_mean))
    logging.info(__file__+" "+"bool_talib_pattern, loop #3: " + str(bool_talib_pattern))
    logging.info(__file__+" "+"bool_pv_hit, loop #4: " + str(bool_pv_hit))
    logging.info(__file__+" "+"bool_p_mfi_div, loop #5: " + str(bool_p_mfi_div))
    logging.info(__file__+" "+"bool_p_rsi_div, loop #6: " + str(bool_p_rsi_div))
    logging.info(__file__+" "+"bool_p_natr_div, loop #7: " + str(bool_p_natr_div))
    logging.info(__file__+" "+"bool_p_tema_div, loop #8: " + str(bool_p_tema_div))
    logging.info(__file__+" "+"bool_p_trima_div, loop #9: " + str(bool_p_trima_div))

    logging.info(__file__+" "+"bool_p_adx_div, loop #10: " + str(bool_p_adx_div))
    logging.info(__file__+" "+"bool_p_adxr_div, loop #11: " + str(bool_p_adxr_div))
    logging.info(__file__+" "+"bool_p_apo_div, loop #12: " + str(bool_p_apo_div))
    logging.info(__file__+" "+"bool_p_aroon_div, loop #13: " + str(bool_p_aroon_div))
    logging.info(__file__+" "+"bool_p_aroonosc_div, loop #14: " + str(bool_p_aroonosc_div))
    logging.info(__file__+" "+"bool_p_bop_div, loop #15: " + str(bool_p_bop_div))
    logging.info(__file__+" "+"bool_p_cci_div, loop #16: " + str(bool_p_cci_div))
    logging.info(__file__+" "+"bool_p_cmo_div, loop #17: " + str(bool_p_cmo_div))
    logging.info(__file__+" "+"bool_p_dx_div, loop #18: " + str(bool_p_dx_div))
    logging.info(__file__+" "+"bool_p_minusdi_div, loop #19: " + str(bool_p_minusdi_div))
    logging.info(__file__+" "+"bool_p_minusdm_div, loop #20: " + str(bool_p_minusdm_div))
    logging.info(__file__+" "+"bool_p_mom_div, loop #21: " + str(bool_p_mom_div))
    logging.info(__file__+" "+"bool_p_plusdi_div, loop #22: " + str(bool_p_plusdi_div))
    logging.info(__file__+" "+"bool_p_plusdm_div, loop #23: " + str(bool_p_plusdm_div))
    logging.info(__file__+" "+"bool_p_ppo_div, loop #24:" + str(bool_p_ppo_div))
    logging.info(__file__+" "+"bool_p_roc_div, loop #25: " + str(bool_p_roc_div))
    logging.info(__file__+" "+"bool_p_rocp_div, loop #26: " + str(bool_p_rocp_div))
    logging.info(__file__+" "+"bool_p_rocr_div, loop #27: " + str(bool_p_rocr_div))
    logging.info(__file__+" "+"bool_p_rocr100_div, loop #28: " + str(bool_p_rocr100_div))
    logging.info(__file__+" "+"bool_p_trix_div, loop #29: " + str(bool_p_trix_div))
    logging.info(__file__+" "+"bool_p_ultosc_div, loop #30: " + str(bool_p_ultosc_div))
    logging.info(__file__+" "+"bool_p_willr_div, loop #31: " + str(bool_p_willr_div))
    logging.info(__file__+" "+"bool_p_macd_div, loop #32: " + str(bool_p_macd_div))
    logging.info(__file__+" "+"bool_p_macdext_div, loop #33: " + str(bool_p_macdext_div))
    logging.info(__file__+" "+"bool_p_macdfix_div, loop #34: " + str(bool_p_macdfix_div))

    logging.info(__file__+" "+"bool_p_ad_div, loop #35: " + str(bool_p_ad_div))
    logging.info(__file__+" "+"bool_p_adosc_div, loop #36: " + str(bool_p_adosc_div))
    logging.info(__file__+" "+"bool_p_obv_div, loop #37: " + str(bool_p_obv_div))

    logging.info(__file__+" "+"bool_p_avgprice_div, loop #38: " + str(bool_p_avgprice_div))
    logging.info(__file__+" "+"bool_p_medprice_div, loop #39: " + str(bool_p_medprice_div))
    logging.info(__file__+" "+"bool_p_typprice_div, loop #40: " + str(bool_p_typprice_div))
    logging.info(__file__+" "+"bool_p_wclprice_div, loop #41: " + str(bool_p_wclprice_div))

    logging.info(__file__+" "+"bool_p_htdcperiod_div, loop #42: " + str(bool_p_htdcperiod_div))
    logging.info(__file__+" "+"bool_p_htdcphase_div, loop #43: " + str(bool_p_htdcphase_div))
    logging.info(__file__+" "+"bool_p_htphasor_div, loop #44: " + str(bool_p_htphasor_div))
    logging.info(__file__+" "+"bool_p_htsine_div, loop #45: " + str(bool_p_htsine_div))
    logging.info(__file__+" "+"bool_p_httrendmode_div, loop #46: " + str(bool_p_httrendmode_div))

    logging.info(__file__+" "+"bool_p_beta_div, loop #47: " + str(bool_p_beta_div))
    logging.info(__file__+" "+"bool_p_correl_div, loop #48: " + str(bool_p_correl_div))
    logging.info(__file__+" "+"bool_p_linearreg_div, loop #49: " + str(bool_p_linearreg_div))
    logging.info(__file__+" "+"bool_p_linearregangle_div, loop #50: " + str(bool_p_linearregangle_div))
    logging.info(__file__+" "+"bool_p_linearregintercept_div, loop #51: " + str(bool_p_linearregintercept_div))
    logging.info(__file__+" "+"bool_p_linearregslope_div, loop #52: " + str(bool_p_linearregslope_div))
    logging.info(__file__+" "+"bool_p_stddev_div, loop #53: " + str(bool_p_stddev_div))
    logging.info(__file__+" "+"bool_p_tsf_div, loop #54: " + str(bool_p_tsf_div))
    logging.info(__file__+" "+"bool_p_var_div, loop #55: " + str(bool_p_var_div))

    logging.info(__file__+" "+"bool_p_wma_div, loop #56: " + str(bool_p_wma_div))
    logging.info(__file__+" "+"bool_p_t3_div, loop #57: " + str(bool_p_t3_div))
    logging.info(__file__+" "+"bool_p_sma_div, loop #58: " + str(bool_p_sma_div))
    logging.info(__file__+" "+"bool_p_sarext_div, loop #59: " + str(bool_p_sarext_div))
    logging.info(__file__+" "+"bool_p_sar_div, loop #60: " + str(bool_p_sar_div))
    logging.info(__file__+" "+"bool_p_midprice_div, loop #61: " + str(bool_p_midprice_div))
    logging.info(__file__+" "+"bool_p_midpoint_div, loop #62: " + str(bool_p_midpoint_div))
    #logging.info(__file__+" "+"bool_p_mavp_div, loop #63: "+str(bool_p_mavp_div))
    logging.info(__file__+" "+"bool_p_mama_div, loop #64: " + str(bool_p_mama_div))
    logging.info(__file__+" "+"bool_p_ma_div, loop #65: " + str(bool_p_ma_div))
    logging.info(__file__+" "+"bool_p_kama_div, loop #66: " + str(bool_p_kama_div))
    logging.info(__file__+" "+"bool_p_httrendline_div, loop #67: " + str(bool_p_httrendline_div))
    logging.info(__file__+" "+"bool_p_ema_div, loop #68: " + str(bool_p_ema_div))
    logging.info(__file__+" "+"bool_p_dema_div, loop #69: " + str(bool_p_dema_div))
    logging.info(__file__+" "+"bool_p_bbands_div, loop #70: " + str(bool_p_bbands_div))

if exam_date is None:
    exam_date = finlib.Finlib().get_last_trading_day()
    exam_date_d = datetime.strptime(exam_date, '%Y%m%d')

    if exam_date_d.weekday() == 0:  #MONDAY
        last_friday = datetime.strptime(exam_date, '%Y%m%d') - timedelta(3)
        exam_date_d = last_friday

    if stock_global in ['US', 'CN', 'MG']:  #us hk data is one day dalay of GMT +8
        exam_date = exam_date_d - timedelta(2)
        exam_date = exam_date.strftime('%Y%m%d')
    elif (datetime.today().isoweekday() not in (6, 7)):
        exam_date = exam_date_d - timedelta(1)  #suppose run the AG on next day morning.
        exam_date = exam_date.strftime('%Y%m%d')

    exam_date = datetime.strptime(exam_date, '%Y%m%d').strftime('%Y-%m-%d')
    logging.info(__file__+" "+"exam_date: " + exam_date)

#exit(1)

progress_run = 0
toal_run = 0

l = multiprocessing.Lock()

# take last 3000 rows for analysis
#if debug:
#    max_exam_day = 300
#else:
#    #max_exam_day = 22 # this is for daily ptn hitting checking
#    max_exam_day = 220000 # this is for history perf gathering.

#This script run daily after the marketing closed,
# Identify the price and volume


def init(l):
    global lock
    lock = l


def calc_init(array):
    global progress_run
    global toal_f_cnt
    inputF = array['inputF']
    outputF = array['outputF']
    outputF_today = array['outputF_today']
    exam_date = array['exam_date']

    if not finlib.Finlib().is_non_zero_file(inputF):
        logging.info(__file__+" "+"not continue, empty file " + inputF)
        return

    # inputF = '/home/ryan/DATA/DAY/' + file
    # outputF = '/home/ryan/DATA/result/' + file
    # outputF_today = '/home/ryan/DATA/result/today/pv_' + file

    # debug
    # if debug:
    # inputF = "/home/ryan/DATA/DAY_dev/SZ300475.csv"
    #     inputF = "/home/ryan/DATA/DAY_dev/SH000001.csv"
    # inputF = "/home/ryan/DATA/DAY_dev/SZ300475_debug.csv"
    # inputF = "/home/ryan/DATA/DAY_dev/SZ000780.csv"
    if single_process:
        progress_run += 1
        #logging.info(__file__+" "+"Work on file " + inputF + ", last date is " + exam_date + ", run " + str(progress_run) + "/" + str(toal_run))
        logging.info(__file__+" "+"Work on file " + inputF + ", run " + str(progress_run) + "/" + str(toal_run))
    else:
        lock.acquire()
        progress_run += 1
        #logging.info(__file__+" "+"Work on file " + inputF + ", last date is " + exam_date + ", run " + str(progress_run) + "/" + str(toal_run))
        logging.info(__file__+" "+"Work on file " + inputF + ", run " + str(progress_run) + "/" + str(toal_run))
        lock.release()
    '''
    if not re.match(".*DAY_Global.*/AG/.*", inputF):
        
        #df = pd.read_csv(inputF, converters={'code': str})
        #df.rename(columns={"datetime": "date"}, inplace=True)
        #df.rename(columns={"high": "h"}, inplace=True)
        #df.rename(columns={"open": "o"}, inplace=True)
        #df.rename(columns={"low": "l"}, inplace=True)
        #df.rename(columns={"close": "c"}, inplace=True)
        #df.rename(columns={"volume": "vol"}, inplace=True)
    else:
        df = pd.read_csv(inputF, skiprows=1, converters={'code': str}, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
    '''
    df = finlib.Finlib().regular_read_csv_to_stdard_df(inputF)
    df_original = df

    if df_original.__len__() <= 1:
        logging.info(__file__+" "+"empty or only one line file, remove file " + inputF)
        #os.remove(inputF) # Don't remove, the stock just released to market and only have one line, and ReadOnly file sytem.
        return

    if df_original.__len__() <= (max_exam_day + 253):  #253 trading days a year.
        logging.info(__file__+" "+"not continue. day available less than specified max_exam_day+253 records" + inputF)
        return

    exam_date_in_df = df_original.iloc[-1].date
    exam_date = str(exam_date_in_df)
    logging.info(__file__+" "+"exam_date set to latest date in csv " + exam_date)

    # take last 3000 rows for analysis
    if max_exam_day == 0:
        logging.info(__file__+" "+"explicitly check all the records " + str(df.__len__()) + " in file " + inputF)
    else:
        logging.info(__file__+" "+"check latest " + str(max_exam_day) + " records in file " + inputF)
        df = df_original.iloc[df_original.__len__() - max_exam_day - 253:]
        df_not_processed = df_original.iloc[:df_original.__len__() - max_exam_day - 253 - 1]

    df_52_week = df_original.iloc[df_original.__len__() - max_exam_day - 253:]

    #df_52_week = df_original
    #df_52_week = df_52_week.reset_index().drop('index', axis=1)

    #if df_52_week.__len__() >= 250: #near 251 working days in a year
    #    df_52_week = df_original.iloc[df_original.__len__() - 250:]

    df.reset_index(inplace=True)
    # print df.head(1)
    opt = {}
    opt['debug'] = debug
    opt['forex'] = forex
    opt['stock_global'] = stock_global

    opt['bool_calc_std_mean'] = bool_calc_std_mean
    opt['bool_perc_std_mean'] = bool_perc_std_mean
    opt['bool_talib_pattern'] = bool_talib_pattern
    opt['bool_pv_hit'] = bool_pv_hit
    opt['bool_p_mfi_div'] = bool_p_mfi_div
    opt['bool_p_rsi_div'] = bool_p_rsi_div
    opt['bool_p_natr_div'] = bool_p_natr_div
    opt['bool_p_tema_div'] = bool_p_tema_div
    opt['bool_p_trima_div'] = bool_p_trima_div

    opt['bool_p_adx_div'] = bool_p_adx_div
    opt['bool_p_adxr_div'] = bool_p_adxr_div
    opt['bool_p_apo_div'] = bool_p_apo_div
    opt['bool_p_aroon_div'] = bool_p_aroon_div
    opt['bool_p_aroonosc_div'] = bool_p_aroonosc_div
    opt['bool_p_bop_div'] = bool_p_bop_div
    opt['bool_p_cci_div'] = bool_p_cci_div
    opt['bool_p_cmo_div'] = bool_p_cmo_div
    opt['bool_p_dx_div'] = bool_p_dx_div
    opt['bool_p_minusdi_div'] = bool_p_minusdi_div
    opt['bool_p_minusdm_div'] = bool_p_minusdm_div
    opt['bool_p_mom_div'] = bool_p_mom_div
    opt['bool_p_plusdi_div'] = bool_p_plusdi_div
    opt['bool_p_plusdm_div'] = bool_p_plusdm_div
    opt['bool_p_ppo_div'] = bool_p_ppo_div
    opt['bool_p_roc_div'] = bool_p_roc_div
    opt['bool_p_rocp_div'] = bool_p_rocp_div
    opt['bool_p_rocr_div'] = bool_p_rocr_div
    opt['bool_p_rocr100_div'] = bool_p_rocr100_div
    opt['bool_p_trix_div'] = bool_p_trix_div
    opt['bool_p_ultosc_div'] = bool_p_ultosc_div
    opt['bool_p_willr_div'] = bool_p_willr_div
    opt['bool_p_macd_div'] = bool_p_macd_div
    opt['bool_p_macdext_div'] = bool_p_macdext_div
    opt['bool_p_macdfix_div'] = bool_p_macdfix_div

    opt['bool_p_ad_div'] = bool_p_ad_div
    opt['bool_p_adosc_div'] = bool_p_adosc_div
    opt['bool_p_obv_div'] = bool_p_obv_div

    opt['bool_p_avgprice_div'] = bool_p_avgprice_div
    opt['bool_p_medprice_div'] = bool_p_medprice_div
    opt['bool_p_typprice_div'] = bool_p_typprice_div
    opt['bool_p_wclprice_div'] = bool_p_wclprice_div

    opt['bool_p_htdcperiod_div'] = bool_p_htdcperiod_div
    opt['bool_p_htdcphase_div'] = bool_p_htdcphase_div
    opt['bool_p_htphasor_div'] = bool_p_htphasor_div
    opt['bool_p_htsine_div'] = bool_p_htsine_div
    opt['bool_p_httrendmode_div'] = bool_p_httrendmode_div

    opt['bool_p_beta_div'] = bool_p_beta_div
    opt['bool_p_correl_div'] = bool_p_correl_div
    opt['bool_p_linearreg_div'] = bool_p_linearreg_div
    opt['bool_p_linearregangle_div'] = bool_p_linearregangle_div
    opt['bool_p_linearregintercept_div'] = bool_p_linearregintercept_div
    opt['bool_p_linearregslope_div'] = bool_p_linearregslope_div
    opt['bool_p_stddev_div'] = bool_p_stddev_div
    opt['bool_p_tsf_div'] = bool_p_tsf_div
    opt['bool_p_var_div'] = bool_p_var_div

    opt['bool_p_wma_div'] = bool_p_wma_div
    opt['bool_p_t3_div'] = bool_p_t3_div
    opt['bool_p_sma_div'] = bool_p_sma_div
    opt['bool_p_sarext_div'] = bool_p_sarext_div
    opt['bool_p_sar_div'] = bool_p_sar_div
    opt['bool_p_midprice_div'] = bool_p_midprice_div
    opt['bool_p_midpoint_div'] = bool_p_midpoint_div
    #opt['bool_p_mavp_div']=bool_p_mavp_div
    opt['bool_p_mama_div'] = bool_p_mama_div
    opt['bool_p_ma_div'] = bool_p_ma_div
    opt['bool_p_kama_div'] = bool_p_kama_div
    opt['bool_p_httrendline_div'] = bool_p_httrendline_div
    opt['bool_p_ema_div'] = bool_p_ema_div
    opt['bool_p_dema_div'] = bool_p_dema_div
    opt['bool_p_bbands_div'] = bool_p_bbands_div

    #(df, df_result) = finlib.Finlib().calc(opt, df, outputF, outputF_today, exam_date)
    #exit(0)
    #df, df_result saves to file right after they calculated, so no need to warriy about the multi-proc, especially in function calls
    try:
        exc_info = sys.exc_info()
        (df, df_result) = finlib.Finlib().calc(max_exam_day, opt, df, df_52_week, outputF, outputF_today, exam_date)

        if local_source:  #this is for pattern performance statisc using?
            if df_not_processed.__len__() >= 0:
                df_not_processed.to_csv(inputF)
            else:
                logging.info(__file__+" "+"local resource, but df_not_processed len is " + str(df_not_processed.__len__()))
        else:
            logging.info(__file__+" "+"keep remote source file no change.")  # when call -l, make sure -m equal 0. AS the no processing df will not be saved to remote.

    except:
        traceback.print_exception(*exc_info)
        logging.info(__file__+" "+"catched exception. outputF is " + outputF)

    finally:
        del exc_info


### Main Start ###
logging.info(__file__+" "+"\n")
logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

if forex:
    root_dir = '/home/ryan/DATA/DAY_Forex'
    if local_source:
        root_dir = '/home/ryan/DATA/DAY_Forex_local'  #have to copy the folder manually,coz this script will be called looply.

elif stock_global is not None:

    if stock_global in ["HK"]:
        root_dir = '/home/ryan/DATA/DAY_Global/HK'
    elif stock_global in ["US", "US_INDEX"]:
        root_dir = '/home/ryan/DATA/DAY_Global/stooq/' + stock_global
    else:
        root_dir = '/home/ryan/DATA/DAY_Global/' + stock_global

    if local_source:
        root_dir = '/home/ryan/DATA/DAY_Global_local/' + stock_global  #have to copy the folder manually,coz this script will be called looply.
        #if stock_global =="HK":
        #    root_dir = '/home/ryan/DATA/DAY_Global_local/HK'

        #else:
#    root_dir = '/home/ryan/DATA/DAY'
#    if local_source:
#        root_dir = '/home/ryan/DATA/DAY_local' #have to copy the folder manually,coz this script will be called looply.

if debug:
    if forex:
        root_dir = '/home/ryan/DATA/DAY_Forex_dev'
    else:
        root_dir = '/home/ryan/DATA/DAY_Global_dev/' + stock_global  #or -d -x dev

logging.info(__file__+" "+"root_dir " + root_dir)

array = []

#if debug:
#    pool=Pool( processes=1, initializer=init, initargs=(l,)) #leave one core free
#else:
#    pool = Pool(processes=process_cnt, initializer=init, initargs=(l,))  # leave one core free

if not merge_only:
    if os.path.isdir("/home/ryan/DATA/tmp/pv_today/" + stock_global + ".del"):
        shutil.rmtree("/home/ryan/DATA/tmp/pv_today/" + stock_global + ".del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv/" + stock_global + ".del"):
        shutil.rmtree("/home/ryan/DATA/tmp/pv/" + stock_global + ".del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv_today/" + stock_global):
        os.rename("/home/ryan/DATA/tmp/pv_today/" + stock_global, "/home/ryan/DATA/tmp/pv_today/" + stock_global + ".del")

    if os.path.isdir("/home/ryan/DATA/tmp/pv/" + stock_global):
        os.rename("/home/ryan/DATA/tmp/pv/" + stock_global, "/home/ryan/DATA/tmp/pv/" + stock_global + ".del")

    if not os.path.isdir("/home/ryan/DATA/tmp/pv_today/"):
        os.mkdir("/home/ryan/DATA/tmp/pv_today/")

    if not os.path.isdir("/home/ryan/DATA/tmp/pv/"):
        os.mkdir("/home/ryan/DATA/tmp/pv/")

    os.mkdir("/home/ryan/DATA/tmp/pv_today/" + stock_global)
    os.mkdir("/home/ryan/DATA/tmp/pv/" + stock_global)

    # multicore implmentation:
    for root, dirs, files in os.walk(root_dir):
        pass

    if selected:
        rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
        stock_list = rst['stock_list']
        stock_list['code'] = stock_list['code'].apply(lambda _d: _d + ".csv")
        files = stock_list['code'].to_list()

    cpu_count = multiprocessing.cpu_count()

    if single_process:
        process_cnt = 1
        toal_run = int(files.__len__())

        for file in files:
            array = ({
                'inputF': root_dir + "/" + file,
                'outputF': "/home/ryan/DATA/tmp/pv/" + stock_global + "/" + file,
                'outputF_today': "/home/ryan/DATA/tmp/pv_today/" + stock_global + "/" + file,
                'exam_date': exam_date,
            })  # array of dict

            if file != 'SZ000880.csv' and file != 'SZ000822.csv':
                #continue #debug
                pass

            calc_init(array)

        logging.info(__file__+" "+"single process loop completed.")
    else:
        process_cnt = cpu_count - 1
        if process_cnt == 0:
            process_cnt = 1

        toal_run = int(files.__len__() / (process_cnt)) + 1

        # each code generate a csv
        for file in files:
            array.append({
                'inputF': root_dir + "/" + file,
                'outputF': "/home/ryan/DATA/tmp/pv/" + stock_global + "/" + file,
                'outputF_today': "/home/ryan/DATA/tmp/pv_today/" + stock_global + "/" + file,
                'exam_date': exam_date,
            })  # array of dict
        pool = Pool(processes=process_cnt, initializer=init, initargs=(l, ))
        pool.map(calc_init, array)
        pool.close()
        pool.terminate()
        pool.join()

#### combine each csv to one csv
for root, dirs, files in os.walk('/home/ryan/DATA/tmp/pv_today/' + stock_global):
    pass

df = pd.DataFrame(columns=[])

if files.__len__() == 0:
    logging.info(__file__+" "+"No any stock hit the criteria today, script completed")
    exit(0)

logging.info(__file__+" "+"merging today's PV hit result files from files in " + root)
for file in files:
    df_tmp = pd.read_csv(root + "/" + file, converters={'code': str})
    if df_tmp.__len__() == 0:
        logging.info(__file__+" "+"skip empty file " + file)
        continue
    df = df.append(df_tmp)

df.drop_duplicates(inplace=True)
df = df.reset_index()

if stock_global in ['KG', 'HK', 'CH', 'MG', 'US']:  #HK, US
    df_code_name_map = finlib.Finlib().get_instrument()
    df_code_name_map = df_code_name_map[['code', 'name']]  # only select code and name column
elif stock_global in ['AG']:  #A stock
    #df_code_name_map = finlib.Finlib().get_security()
    df_code_name_map = finlib.Finlib().get_A_stock_instrment()
    df_code_name_map = finlib.Finlib().add_market_to_code(df_code_name_map)  # 000603  ==> SZ000603
    df_code_name_map = df_code_name_map[['code', 'name']]  #only select code and name column

if selected:
    result_csv = "/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_" + stock_global.lower() + ".csv"
else:
    result_csv = "/home/ryan/DATA/result/today/talib_and_pv_no_db_filter_" + stock_global.lower() + ".csv"

#df is today's ptn(talib+pv) hit
#filter not trading stock out.
df = df.loc[df['close_p'] != 0.0]

if stock_global != 'dev':  #dev == debug, may have mixed type of code, result zero inner merge result.
    df = pd.merge(df_code_name_map, df, on='code', how='inner')

df.to_csv(result_csv)
logging.info(__file__+" "+"Today Talib and PV B no filter length " + str(df.__len__()) + ", result saved to " + result_csv)



#Comment out below if don't want sql data
#logging.info(__file__+" "+"Script completed")
#os._exit(0)


### Filter with DB records
# engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
#
# query_buy_sql = "SELECT * FROM `pattern_perf` \
#     WHERE `2mea` > 0.02 AND `5mea`> `2mea` \
#     AND `2uc`> `2dc`*3 \
#     AND `2uc`> 10 "
#
# query_buy_sql += "ORDER BY `2uc`-`2dc`"
#
# query_sell_sql = "SELECT * FROM `pattern_perf` \
#     WHERE `2mea` < -0.02 AND `2mea`> `5mea` \
#     AND `2dc`> `2uc`*3 \
#     AND `2dc`> 10 "
#
# query_sell_sql += "ORDER BY `2dc`-`2uc`"
#
# logging.info(__file__+" "+"query ideal BUY code/pattern perf in DB")
# idea_buy_stock_in_db = pd.read_sql_query(query_buy_sql, engine, index_col='ID')
# idea_buy_stock_in_db.rename(columns={'pattern': 'op_rsn', 'stockID': 'code'}, inplace=True)
# logging.info(__file__+" "+"query ideal BUY code/pattern perf in DB, got record # " + str(idea_buy_stock_in_db.__len__()))
#
# logging.info(__file__+" "+"query ideal SELL code/pattern perf in DB")
# idea_sell_stock_in_db = pd.read_sql_query(query_sell_sql, engine, index_col='ID')
# idea_sell_stock_in_db.rename(columns={'pattern': 'op_rsn', 'stockID': 'code'}, inplace=True)
# logging.info(__file__+" "+"query ideal SELL code/pattern perf in DB, got record # " + str(idea_sell_stock_in_db.__len__()))
#
# df_buy = pd.merge(df, idea_buy_stock_in_db, left_on=['op_rsn', 'code'], right_on=['op_rsn', 'code'], how='inner')
# df_sell = pd.merge(df, idea_sell_stock_in_db, left_on=['op_rsn', 'code'], right_on=['op_rsn', 'code'], how='inner')
#
# df_buy.drop_duplicates(inplace=True)
# df_sell.drop_duplicates(inplace=True)
#
# logging.info(__file__+" "+"inner merge with today hit, record # " + str(df_buy.__len__()))
#
# cols = df_buy.columns.tolist()
# cols = [
#     'code',
#     'date',
#     'op',
#     'op_rsn',
#     'op_strength',
#     'close_p',
#     '2mea',
#     '5mea',
#     '10mea',
#     '20mea',
#     '60mea',
#     '120mea',
#     '2uc',
#     '2dc',
#     '5uc',
#     '5dc',
#     '10uc',
#     '10dc',
#     '20uc',
#     '20dc',
#     #'up_cnt_60d','dn_cnt_60d','up_cnt_120d','dn_cnt_120d',
# ]
# df_buy = df_buy[cols]
# df_buy.sort_values('2mea', ascending=False)
#
# df_sell = df_sell[cols]
# df_sell.sort_values('2mea', ascending=True)
#
# engine.dispose()
#
# #save filtered ptn(talib+pv) result to csv
# result_csv_buy = "/home/ryan/DATA/result/today/talib_and_pv_db_buy_filtered_" + stock_global + ".csv"
# result_csv_sell = "/home/ryan/DATA/result/today/talib_and_pv_db_sell_filtered_" + stock_global + ".csv"
#
# if stock_global != 'dev':
#     df_buy = pd.merge(df_code_name_map, df_buy, on='code', how='inner')
#     df_sell = pd.merge(df_code_name_map, df_sell, on='code', how='inner')
#
# df_buy.to_csv(result_csv_buy, index=False)
# df_sell.to_csv(result_csv_sell, index=False)
#
# logging.info(__file__+" "+"Today Talib and PV B filtered result saved to " + result_csv_buy)
# logging.info(__file__+" "+"Today Talib and PV S filtered result saved to " + result_csv_sell)

logging.info(__file__+" "+"Script completed")
os._exit(0)
