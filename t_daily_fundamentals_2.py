# coding: utf-8

import sys, traceback, threading
import tushare as ts
import talib
import pickle
import os.path
import pandas as pd
import time
import numpy as np
#import matplotlib.pyplot as plt
import pandas
import math
import re
from scipy import stats
import finlib
import datetime
from optparse import OptionParser
import sys
import os
import logging
import signal

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

#from datetime import datetime, timedelta

# This script Run every week to get the fundamental info with tushare pro.
USD_DIV_CNY = 7

global debug_global
global force_run_global
global myToken
global fund_base
global fund_base_source
global fund_base_merged
global fund_base_report
global fund_base_tmp

global csv_income
global csv_balancesheet
global csv_cashflow
global csv_forecast
global csv_dividend
global csv_express
global csv_fina_indicator
global csv_fina_audit
global csv_fina_mainbz
global csv_fina_mainbz_sum
global csv_disclosure_date
global csv_basic

global fund_base_latest

global csv_income_latest
global csv_balancesheet_latest
global csv_cashflow_latest
global csv_forecast_latest
global csv_express_latest
global csv_dividend_latest
global csv_fina_indicator_latest
global csv_fina_audit_latest
global csv_fina_mainbz_latest
global csv_fina_mainbz_sum_latest
global csv_disclosure_date_latest
global csv_disclosure_date_latest_notify
global csv_fina_mainbz_latest_percent

global col_list_income

global col_list_balancesheet

global col_list_cashflow
global col_list_forecast
global col_list_dividend
global col_list_express

global col_list_fina_indicator
global col_list_fina_audit
global col_list_fina_mainbz
global col_list_disclosure_date

global query_fields_income
global query_fields_balancesheet
global query_fields_cashflow
global query_fields_fina_indicator
global query_fields_forecast
global query_fields_dividend
global query_fields_express
global query_fields_fina_audit
global query_fields_fina_mainbz
global query_fields_disclosure_date

#pd.set_option('display.height', 1000)
#pd.set_option('display.max_rows', 500)
#pd.set_option('display.max_columns', 500)
#pd.set_option('display.width', 1000)

global df_all_ts_pro
global df_all_jaqs
global big_memory_global


def set_global(debug=False, big_memory=False, force_run=False):
    global debug_global
    global force_run_global
    global myToken
    global fund_base
    global fund_base_source
    global fund_base_merged
    global fund_base_report
    global fund_base_tmp

    global csv_income
    global csv_balancesheet
    global csv_cashflow
    global csv_forecast
    global csv_dividend
    global csv_express
    global csv_fina_indicator
    global csv_fina_audit
    global csv_fina_mainbz
    global csv_fina_mainbz_sum
    global csv_disclosure_date
    global csv_basic

    global fund_base_latest

    global csv_income_latest
    global csv_balancesheet_latest
    global csv_cashflow_latest
    global csv_forecast_latest
    global csv_express_latest
    global csv_dividend_latest
    global csv_fina_indicator_latest
    global csv_fina_audit_latest
    global csv_fina_mainbz_latest
    global csv_fina_mainbz_sum_latest
    global csv_fina_mainbz_latest_percent
    global csv_disclosure_date_latest
    global csv_disclosure_date_latest_notify

    global col_list_income

    global col_list_balancesheet

    global col_list_cashflow
    global col_list_forecast
    global col_list_dividend
    global col_list_express

    global col_list_fina_indicator
    global col_list_fina_audit
    global col_list_fina_mainbz
    global col_list_disclosure_date

    global query_fields_income
    global query_fields_balancesheet
    global query_fields_cashflow
    global query_fields_fina_indicator
    global query_fields_forecast
    global query_fields_dividend
    global query_fields_express
    global query_fields_fina_audit
    global query_fields_fina_mainbz
    global query_fields_disclosure_date

    global df_all_ts_pro
    global df_all_jaqs
    global big_memory_global

    ### Global Variables ####

    myToken = '4cc9a1cd78bf41e759dddf92c919cdede5664fa3f1204de572d8221b'
    fund_base = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2"

    fund_base_source = fund_base + "/source"
    fund_base_merged = fund_base + "/merged"
    fund_base_report = fund_base + "/report"
    fund_base_tmp = fund_base + "/tmp"
    fund_base_tmp = fund_base + "/tmp"

    debug_global = False
    force_run_global = False
    big_memory_global = False

    if force_run:
        force_run_global = True

    if debug:
        debug_global = True
        fund_base_source = fund_base + "/source.dev"
        fund_base_merged = fund_base + "/merged.dev"  # modify global fund_base_merged to dev
        fund_base_report = fund_base + "/report.dev"

    if big_memory:
        big_memory_global = True
        df_all_jaqs = finlib.Finlib().load_all_jaqs()
        df_all_ts_pro = finlib.Finlib().load_all_ts_pro(debug=debug, overwrite=True)

    csv_income = fund_base_source + "/income.csv"
    csv_balancesheet = fund_base_source + "/balancesheet.csv"
    csv_cashflow = fund_base_source + "/cashflow.csv"
    csv_forecast = fund_base_source + "/forecast.csv"
    csv_dividend = fund_base_source + "/dividend.csv"
    csv_express = fund_base_source + "/express.csv"
    csv_fina_indicator = fund_base_source + "/fina_indicator.csv"
    csv_fina_audit = fund_base_source + "/fina_audit.csv"
    csv_fina_mainbz = fund_base_source + "/fina_mainbz.csv"
    csv_fina_mainbz_sum = fund_base_source + "/fina_mainbz_sum.csv"
    csv_disclosure_date = fund_base_source + "/disclosure_date.csv"
    csv_basic = fund_base_source + "/basic.csv"

    fund_base_latest = fund_base_source + "/latest"

    csv_income_latest = fund_base_latest + "/income.csv"
    csv_balancesheet_latest = fund_base_latest + "/balancesheet.csv"
    csv_cashflow_latest = fund_base_latest + "/cashflow.csv"
    csv_forecast_latest = fund_base_latest + "/forecast.csv"
    csv_express_latest = fund_base_latest + "/express.csv"
    csv_dividend_latest = fund_base_latest + "/dividend.csv"
    csv_fina_indicator_latest = fund_base_latest + "/fina_indicator.csv"
    csv_fina_audit_latest = fund_base_latest + "/fina_audit.csv"
    csv_fina_mainbz_latest = fund_base_latest + "/fina_mainbz.csv"
    csv_fina_mainbz_sum_latest = fund_base_latest + "/fina_mainbz_sum.csv"
    csv_fina_mainbz_latest_percent = fund_base_latest + "/fina_mainbz_percent.csv"
    csv_disclosure_date_latest = fund_base_latest + "/disclosure_date.csv"
    csv_disclosure_date_latest_notify = fund_base_latest + "/disclosure_date_notify.csv"

    col_list_income = ['ts_code', 'name', 'end_date', 'basic_eps', 'total_revenue', 'revenue', 'oth_b_income', 'n_income_attr_p', 'distable_profit']
    col_list_balancesheet = ['ts_code', 'name', 'end_date', 'total_assets', 'total_liab', 'money_cap', 'undistr_porfit', 'invest_real_estate', 'fa_avail_for_sale', 'lt_borr', 'st_borr', 'cb_borr']
    col_list_cashflow = ['ts_code', 'name', 'end_date', 'net_profit']
    col_list_forecast = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_dividend = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_express = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_disclosure_date = ['ts_code', 'name', 'ann_date', 'end_date', 'pre_date', 'actual_date', 'modify_date']

    # rd_exp is not in the actual output
    col_list_fina_indicator = [
        'ts_code',
        'name',
        'end_date',
        'eps',
        'roe',
        'debt_to_assets',
        'rd_exp',
        'total_revenue_ps',
        'netprofit_margin',
    ]
    col_list_fina_audit = [
        'ts_code',
        'name',
        'end_date',
    ]
    col_list_fina_mainbz = [
        'ts_code',
        'name',
        'end_date',
    ]

    ####start
    query_fields_income = 'ts_code,update_flag,end_date,basic_eps,total_revenue,revenue,oth_b_income,n_income_attr_p,distable_profit,admin_exp,ann_date,ass_invest_income,assets_impair_loss,biz_tax_surchg,comm_exp,comm_income,comp_type,compens_payout,compens_payout_refu,compr_inc_attr_m_s,compr_inc_attr_p,diluted_eps,div_payt,ebit,ebitda,f_ann_date,fin_exp,forex_gain,fv_value_chg_gain,income_tax,insur_reser_refu,insurance_exp,int_exp,int_income,invest_income,minority_gain,n_asset_mg_income,n_commis_income,n_income,n_oth_b_income,n_oth_income,n_sec_tb_income,n_sec_uw_income,nca_disploss,non_oper_exp,non_oper_income,oper_cost,oper_exp,operate_profit,oth_compr_income,other_bus_cost,out_prem,prem_earned,prem_income,prem_refund,reins_cost_refund,reins_exp,reins_income,report_type,reser_insur_liab,sell_exp,t_compr_income,total_cogs,total_profit,undist_profit,une_prem_reser'

    query_fields_balancesheet = 'ts_code,update_flag,end_date,total_assets,total_liab,money_cap,undistr_porfit,invest_real_estate,fa_avail_for_sale,lt_borr,st_borr,cb_borr,acc_exp,acc_receivable,accounts_receiv,acct_payable,acting_trading_sec,acting_uw_sec,adv_receipts,agency_bus_liab,amor_exp,ann_date,bond_payable,cap_rese,cash_reser_cb,cip,client_depos,client_prov,comm_payable,comp_type,const_materials,decr_in_disbur,defer_inc_non_cur_liab,defer_tax_assets,defer_tax_liab,deferred_inc,depos,depos_ib_deposits,depos_in_oth_bfi,depos_oth_bfi,depos_received,deriv_assets,deriv_liab,div_payable,div_receiv,estimated_liab,f_ann_date,fix_assets,fixed_assets_disp,forex_differ,goodwill,hfs_assets,hfs_sales,htm_invest,indem_payable,indep_acct_assets,indept_acc_liab,int_payable,int_receiv,intan_assets,inventories,invest_as_receiv,invest_loss_unconf,lending_funds,loan_oth_bank,loanto_oth_bank_fi,lt_amor_exp,lt_eqt_invest,lt_payable,lt_payroll_payable,lt_rec,minority_int,nca_within_1y,non_cur_liab_due_1y,notes_payable,notes_receiv,oil_and_gas_assets,ordin_risk_reser,oth_assets,oth_comp_income,oth_cur_assets,oth_cur_liab,oth_eqt_tools,oth_eqt_tools_p_shr,oth_liab,oth_nca,oth_ncl,oth_payable,oth_receiv,payable_to_reinsurer,payables,payroll_payable,ph_invest,ph_pledge_loans,pledge_borr,policy_div_payable,prec_metals,prem_receiv_adva,premium_receiv,prepayment,produc_bio_assets,pur_resale_fa,r_and_d,refund_cap_depos,refund_depos,reinsur_receiv,reinsur_res_receiv,report_type,reser_lins_liab,reser_lthins_liab,reser_outstd_claims,reser_une_prem,rr_reins_lins_liab,rr_reins_lthins_liab,rr_reins_outstd_cla,rr_reins_une_prem,rsrv_insur_cont,sett_rsrv,sold_for_repur_fa,special_rese,specific_payables,st_bonds_payable,st_fin_payable,surplus_rese,taxes_payable,time_deposits,total_cur_assets,total_cur_liab,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,total_nca,total_ncl,total_share,trad_asset,trading_fl,transac_seat_fee,treasury_share'

    query_fields_cashflow = 'ts_code,update_flag,end_date,net_profit,amort_intang_assets,ann_date,beg_bal_cash,beg_bal_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_disp_withdrwl_invest,c_fr_oth_operate_a,c_fr_sale_sg,c_inf_fr_operate_a,c_paid_for_taxes,c_paid_goods_s,c_paid_invest,c_paid_to_for_empl,c_pay_acq_const_fiolta,c_pay_claims_orig_inco,c_pay_dist_dpcp_int_exp,c_prepay_amt_borr,c_recp_borrow,c_recp_cap_contrib,c_recp_return_invest,comp_type,conv_copbonds_due_within_1y,conv_debt_into_cap,decr_def_inc_tax_assets,decr_deferred_exp,decr_inventories,decr_oper_payable,depr_fa_coga_dpba,eff_fx_flu_cash,end_bal_cash,end_bal_cash_equ,f_ann_date,fa_fnc_leases,finan_exp,free_cashflow,ifc_cash_incr,im_n_incr_cash_equ,im_net_cashflow_oper_act,incl_cash_rec_saims,incl_dvd_profit_paid_sc_ms,incr_acc_exp,incr_def_inc_tax_liab,incr_oper_payable,invest_loss,loss_disp_fiolta,loss_fv_chg,loss_scr_fa,lt_amort_deferred_exp,n_cap_incr_repur,n_cash_flows_fnc_act,n_cashflow_act,n_cashflow_inv_act,n_depos_incr_fi,n_disp_subs_oth_biz,n_inc_borr_oth_fi,n_incr_cash_cash_equ,n_incr_clt_loan_adv,n_incr_dep_cbob,n_incr_disp_faas,n_incr_disp_tfa,n_incr_insured_dep,n_incr_loans_cb,n_incr_loans_oth_bank,n_incr_pledge_loan,n_recp_disp_fiolta,n_recp_disp_sobu,n_reinsur_prem,oth_cash_pay_oper_act,oth_cash_recp_ral_fnc_act,oth_cashpay_ral_fnc_act,oth_pay_ral_inv_act,oth_recp_ral_inv_act,others,pay_comm_insur_plcy,pay_handling_chrg,prem_fr_orig_contr,proc_issue_bonds,prov_depr_assets,recp_tax_rends,report_type,st_cash_out_act,stot_cash_in_fnc_act,stot_cashout_fnc_act,stot_inflows_inv_act,stot_out_inv_act,uncon_invest_loss'

    query_fields_fina_indicator = 'ts_code,update_flag,end_date,eps,roe,debt_to_assets,total_revenue_ps,netprofit_margin,adminexp_of_gr,ann_date,ar_turn,assets_to_eqt,assets_turn,assets_yoy,basic_eps_yoy,bps,bps_yoy,ca_to_assets,ca_turn,capital_rese_ps,cash_ratio,cfps,cfps_yoy,cogs_of_sales,current_exint,current_ratio,currentdebt_to_debt,debt_to_eqt,diluted2_eps,dp_assets_to_eqt,dt_eps,dt_eps_yoy,dt_netprofit_yoy,ebit,ebit_of_gr,ebit_ps,ebitda,ebt_yoy,eqt_to_debt,eqt_to_interestdebt,eqt_to_talcapital,eqt_yoy,equity_yoy,expense_of_sales,extra_item,fa_turn,fcfe,fcfe_ps,fcff,fcff_ps,finaexp_of_gr,fixed_assets,gc_of_gr,gross_margin,grossprofit_margin,impai_ttm,int_to_talcap,interestdebt,invest_capital,longdeb_to_debt,nca_to_assets,netdebt,netprofit_yoy,networking_capital,noncurrent_exint,npta,ocf_to_debt,ocf_to_shortdebt,ocf_yoy,ocfps,op_income,op_of_gr,op_yoy,or_yoy,profit_dedt,profit_to_gr,profit_to_op,q_dt_roe,q_gc_to_gr,q_npta,q_ocf_to_sales,q_op_qoq,q_roe,q_saleexp_to_gr,q_sales_yoy,quick_ratio,retained_earnings,retainedps,revenue_ps,roa,roa2_yearly,roa_dp,roa_yearly,roe_dt,roe_waa,roe_yearly,roe_yoy,roic,saleexp_to_gr,surplus_rese_ps,tangasset_to_intdebt,tangible_asset,tangibleasset_to_debt,tangibleasset_to_netdebt,tbassets_to_totalassets,tr_yoy,turn_days,undist_profit_ps,working_capital'

    query_fields_forecast = 'ts_code,end_date,ann_date,change_reason,first_ann_date,last_parent_net,net_profit_max,net_profit_min,p_change_max,p_change_min,summary,type'

    query_fields_dividend = 'ts_code,end_date,ann_date,cash_div,cash_div_tax,div_listdate,div_proc,ex_date,imp_ann_date,pay_date,record_date,stk_bo_rate,stk_co_rate,stk_div'

    query_fields_express = 'ts_code,end_date,ann_date,bps,diluted_eps,diluted_roe,eps_last_year,growth_assets,growth_bps,is_audit,n_income,np_last_year,op_last_year,open_bps,open_net_assets,operate_profit,or_last_year,perf_summary,remark,revenue,total_assets,total_hldr_eqy_exc_min_int,total_profit,tp_last_year,yoy_dedu_np,yoy_eps,yoy_equity,yoy_net_profit,yoy_op,yoy_roe,yoy_sales,yoy_tp'

    query_fields_fina_audit = 'ts_code,end_date,ann_date,audit_agency,audit_result,audit_sign'

    query_fields_fina_mainbz = 'ts_code,update_flag,end_date,bz_cost,bz_item,bz_profit,bz_sales,curr_type'

    query_fields_disclosure_date = 'ts_code,ann_date,end_date,pre_date,actual_date,modify_date'
    ###end


def get_ts_field(ts_code, ann_date, field, big_memory):

    if big_memory:
        df = df_all_ts_pro
        df = df[df['ts_code'] == ts_code]
        if (df.__len__() == 0):
            logging.info("no ts_code in df_all_ts_pro " + ts_code)
            return

        df = df[df['end_date'] == ann_date]
        if (df.__len__() == 0):
            logging.info("no end_date in df_all_ts_pro " + ts_code + " " + ann_date)
            return

        data_in_field = df[field].values[0]
        df = None
        return (data_in_field)
    else:
        f = fund_base_merged + "/" + "merged_all_" + ann_date + ".csv"

        if not os.path.exists(f):
            logging.info("file not exists, " + f)
            return

        df = pd.read_csv(f, converters={'end_date': str})

        if not field in df.columns:
            logging.info("filed not in the file, " + field + " " + f)
            return

        df = df[df['ts_code'] == ts_code]

        if (df.__len__() == 0):
            logging.info("no ts_code in file " + ts_code + " " + f)
            return

        data_in_field = df[field].values[0]  #always return the first one. suppose the 1st is the most updated one if multiple lines for the code+ann_date

        return (data_in_field)


'''
def zzz_get_jaqs_field(ts_code, date=None, big_memory=False): #date: YYYYMMDD, code:600519, read from ~/DATA/DAY_JAQS/SH600519.csv
  #date : None, then return the latest record.

    code_in_number_only = re.match("(\d{6})\.(.*)", ts_code).group(1)
    #market = re.match("(\d{6})\.(.*)", ts_code).group(2)

    #self.append_market_to_code_single_dot(code = code_in_number_only) #'600519.SH'
    codeInFmtMktCode = finlib.Finlib().add_market_to_code_single(code=code_in_number_only) #'SH600519'
    #self.add_market_to_code(df=pd.DataFrame({'code':code_in_number_only}, index=[0]), dot_f=True, tspro_format=True) #0  600519.SH

    if big_memory:
        df = df_all_jaqs[df_all_jaqs['code']==code_in_number_only]
    else:
        f = "/home/ryan/DATA/DAY_JAQS/"+codeInFmtMktCode+'.csv'
        if not os.path.exists(f):
            logging.info('file not exist '+f)
            return
        df = pd.read_csv(f, converters={'code':str, 'trade_date':str})

    if date == None:
        df = df.tail(1)
    else:
        date_Y_M_D = finlib.Finlib().get_last_trading_day(date)
        date = datetime.strptime(date_Y_M_D, '%Y-%m-%d').strftime('%Y%m%d')
        df = df[df['trade_date'] == date]

        if df.__len__() == 0:
            logging.info('code '+ts_code+' has no record at date '+ date+". Use latest known date.")
            df = df.tail(1)
        elif df.__len__() > 0:
            df = df.head(1)  # if multiple records, only use the 1st one.

    dict_rtn = {'pe':0,'pe_ttm':0,'pb':0,'ps':0}

    if df['pe'].__len__()>0:
      dict_rtn['pe']=df['pe'].values[0]


    if df['pe_ttm'].__len__() > 0:
        dict_rtn['pe_ttm']=df['pe_ttm'].values[0]


    if df['pb'].__len__() > 0:
        dict_rtn['pb']=df['pb'].values[0]


    if df['ps'].__len__() > 0:
        dict_rtn['ps']=df['ps'].values[0]


    dict_rtn['all']=df.reset_index().drop('index', axis=1)
    df = None
    return(dict_rtn)
'''


def get_jaqs_field(ts_code, date=None, big_memory=False):  #date: YYYYMMDD, code:600519, read from ~/DATA/DAY_JAQS/SH600519.csv
    #date : None, then return the latest record.

    if big_memory:
        df = df_all_jaqs[df_all_jaqs['ts_code'] == ts_code]
    else:
        f = fund_base_source + "/individual_per_stock/" + ts_code + "_basic.csv"
        if not os.path.exists(f):
            logging.info('file not exist ' + f)
            return
        df = pd.read_csv(f, converters={'ts_code': str, 'trade_date': str})

    if date == None:
        df = df.tail(1)
    else:
        date_Y_M_D = finlib.Finlib().get_last_trading_day(date)
        date = datetime.datetime.strptime(date_Y_M_D, '%Y%m%d').strftime('%Y%m%d')
        df = df[df['trade_date'] == date]

        if df.__len__() == 0:
            logging.info('ts_code ' + ts_code + ' has no record at date ' + date + ". Use latest known date.")
            df = df.tail(1)
        elif df.__len__() > 0:
            df = df.head(1)  # if multiple records, only use the 1st one.

    dict_rtn = {'pe': 0, 'pe_ttm': 0, 'pb': 0, 'ps': 0}

    if df['pe'].__len__() > 0:
        dict_rtn['pe'] = df['pe'].values[0]

    if df['pe_ttm'].__len__() > 0:
        dict_rtn['pe_ttm'] = df['pe_ttm'].values[0]

    if df['pb'].__len__() > 0:
        dict_rtn['pb'] = df['pb'].values[0]

    if df['ps'].__len__() > 0:
        dict_rtn['ps'] = df['ps'].values[0]

    dict_rtn['all'] = df.reset_index().drop('index', axis=1)
    df = None
    return (dict_rtn)


def get_a_specify_stock(ts_code, end_date):  #(ts_code='600519.SH', end_date='20180630')
    odir = fund_base_tmp + "/" + ts_code + "_" + end_date

    if not os.path.exists(fund_base_tmp):
        os.makedirs(fund_base_tmp)

    csv_income_tmp = odir + "/income.csv"
    csv_balancesheet_tmp = odir + "/balancesheet.csv"
    csv_cashflow_tmp = odir + "/cashflow.csv"
    csv_forecast_tmp = odir + "/forecast.csv"
    csv_express_tmp = odir + "/express.csv"
    csv_dividend_tmp = odir + "/dividend.csv"
    csv_fina_indicator_tmp = odir + "/fina_indicator.csv"
    csv_fina_audit_tmp = odir + "/fina_audit.csv"
    #csv_fina_mainbz_tmp = odir + "/fina_mainbz.csv"
    csv_fina_mainbz_sum_tmp = odir + "/fina_mainbz_sum.csv"
    csv_disclosure_date_tmp = odir + "/disclosure_date.csv"

    _extract_latest(csv_input=csv_income, csv_output=csv_income_tmp, feature='income', col_name_list=col_list_income, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_balancesheet, csv_output=csv_balancesheet_tmp, feature='balancesheet', col_name_list=col_list_balancesheet, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_cashflow, csv_output=csv_cashflow_tmp, feature='cashflow', col_name_list=col_list_cashflow, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_forecast, csv_output=csv_forecast_tmp, feature='forecast', col_name_list=col_list_forecast, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_dividend, csv_output=csv_dividend_tmp, feature='dividend', col_name_list=col_list_dividend, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_express, csv_output=csv_express_tmp, feature='express', col_name_list=col_list_express, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_indicator, csv_output=csv_fina_indicator_tmp, feature='fina_indicator', col_name_list=col_list_fina_indicator, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_audit, csv_output=csv_fina_audit_tmp, feature='fina_audit', col_name_list=col_list_fina_audit, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_fina_mainbz_sum, csv_output=csv_fina_mainbz_sum_tmp, feature='fina_mainbz', col_name_list=col_list_fina_mainbz, ts_code=ts_code, end_date=end_date)
    _extract_latest(csv_input=csv_disclosure_date, csv_output=csv_disclosure_date_tmp, feature='disclosure_date', col_name_list=col_list_fina_mainbz, ts_code=ts_code, end_date=end_date)


def remove_dup_record(df_input, csv_name):

    df = pd.DataFrame(columns=list(df_input.columns))

    lst = list(df_input['ts_code'].unique())
    lst.sort()

    for code in lst:
        df_tmp = df_input[df_input['ts_code'] == code]
        df_append = pd.DataFrame()

        len = df_tmp.__len__()

        if len <= 1:
            df = df.append(df_tmp)
            continue

        #now df_tmp have multiple records (dup), check if any record has update_flag set.
        if "update_flag" in df_tmp.columns:
            df_tmp_updated = df_tmp[df_tmp['update_flag'] == '1']  #<<< it is string 1.
            if df_tmp_updated.__len__() > 1:
                sys.stdout.write(csv_name + " has multi update_flag records, len " + str(df_tmp_updated.__len__()) + ".")
                sys.stdout.flush()
                logging.info("\t" + df_tmp_updated['ts_code'] + " " + df_tmp_updated['end_date'])
                df_append = df_tmp_updated.iloc[0]  #choose the 1st updated records if have multiple updated records
                df = df.append(df_append)
                continue
            elif df_tmp_updated.__len__() == 1:
                sys.stdout.write(csv_name + " has one update_flag record\n")
                df_append = df_tmp_updated.iloc[0]
                df = df.append(df_append)
                continue
            else:
                sys.stdout.write(csv_name + " has multi records, len " + str(len) + " and zero update_flag records.")
                sys.stdout.flush()
                df_append = df_tmp.iloc[0]
                logging.info("\t" + df_append['ts_code'] + " " + df_append['end_date'])
                df = df.append(df_append)
                continue
        else:
            #now df_tmp have multiple dup records, and no update_flag in columns.
            sys.stdout.write(csv_name + " has multi records, len " + str(len) + " and no update_flag in column.")
            sys.stdout.flush()
            df_append = df_tmp.iloc[0]
            logging.info("\t" + df_append['ts_code'] + " " + df_append['end_date'])
            df = df.append(df_append)
            continue

    len = df.__len__()
    logging.info("len of " + csv_name + " after remove dup records " + str(len))
    return (df)


def load_fund_result(mini_score=80):
    stable_rpt_date = finlib.Finlib().get_year_month_quarter()['stable_report_perid']

    f_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_" + stable_rpt_date + ".csv"

    logging.info("loading df_fund_2, " + f_fund_2)
    if (os.path.isfile(f_fund_2)) and os.stat(f_fund_2).st_size >= 10:  # > 10 bytes
        df_fund_2 = pd.read_csv(f_fund_2)
        df_fund_2 = df_fund_2[df_fund_2['sos'] > mini_score]

        #df_fund_2 = finlib.Finlib().ts_code_to_code(df_fund_2)
        df_fund_2 = df_fund_2[['ts_code', 'name']]
        df_fund_2 = df_fund_2.rename(columns={"ts_code": "code"}, inplace=False)

        df_fund_2 = df_fund_2.drop_duplicates()
        df_fund_2 = df_fund_2.reset_index().drop('index', axis=1)
    else:
        logging.info("no such file " + f_fund_2)
        logging.info("stop and exit")
        exit(0)
    return (df_fund_2)


def fetch(fast_fetch=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    time_series = finlib.Finlib().get_year_month_quarter()
    fetch_period_list = []

    if not os.path.exists(fund_base):
        os.makedirs(fund_base)

    if not os.path.exists(fund_base_source + "/individual"):
        os.makedirs(fund_base_source + "/individual")

    if fast_fetch:
        fetch_period_list = time_series['fetch_most_recent_report_perid']

        # high_score_stock_only
        if (not force_run_global):  #ryan debug
            stock_list = load_fund_result(mini_score=70)
            #print(stock_list.__len__())
        else:
            stock_list = finlib.Finlib().get_A_stock_instrment()  #603999
            stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)

    else:
        stock_list = finlib.Finlib().get_A_stock_instrment()  #603999
        stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  #603999.SH
        fetch_period_list = time_series['full_period_list']
        #fetch_period_list = time_series['full_period_list_yearly']

    if debug_global:  #ryan debug start of fetching
        stock_list = stock_list[stock_list['code'] == '600519.SH']
        #stock_list=stock_list[stock_list['code']=='300081.SZ']
        #stock_list=stock_list[stock_list['code']=='000333.SZ']  #2019-04-20
        #stock_list=stock_list[stock_list['code']=='603888.SH']  #2019-04-20
        #stock_list=stock_list[stock_list['code']=='300602.SZ'] #2019-02-27
        #stock_list=stock_list[stock_list['code']=='002442.SZ'] #2019-02-15  PASS
        #stock_list=stock_list[stock_list['code']=='603638.SH'] #2019-02-20 FALSE
        #stock_list=stock_list[stock_list['code']=='601518.SH'] #2019-03-20
        #stock_list=stock_list[stock_list['code']=='000651.SZ'] #2019-04-29
        #stock_list=stock_list[stock_list['code']=='300319.SZ'] #income 20181231 is empty

    #select = datetime.datetime.today().day%2  # avoid too many requests a day

    #if (select == 0)  or fast_fetch or force_run_global: #'save_only' runs on the HK VPS.

    # not fetching/calculating fundermental data at month 6,9, 11, 12
    if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
        logging.info("not processing fundermental data at this month. ")
        return ()
    else:
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'income', query_fields_income, fetch_period_list)  #利润表
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'balancesheet', query_fields_balancesheet, fetch_period_list)  #资产负债表
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'cashflow', query_fields_cashflow, fetch_period_list)  #现金流量表
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'fina_mainbz', query_fields_fina_mainbz, fetch_period_list)  #主营业务构成

        _ts_pro_fetch(pro, stock_list, fast_fetch, 'dividend', query_fields_dividend, fetch_period_list)  #分红送股
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'fina_indicator', query_fields_fina_indicator, fetch_period_list)  #财务指标数据
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'fina_audit', query_fields_fina_audit, fetch_period_list)  #财务审计意见

        _ts_pro_fetch(pro, stock_list, fast_fetch, 'forecast', query_fields_forecast, fetch_period_list)  #业绩预告
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'express', query_fields_express, fetch_period_list)  #业绩快报
        _ts_pro_fetch(pro, stock_list, fast_fetch, 'disclosure_date', query_fields_disclosure_date, fetch_period_list)  #财报披露计划日期


def handler(signum, frame):
    logging.info("timeout when fetching!")
    raise Exception("end of time")


#save only == True
def _ts_pro_fetch(pro_con, stock_list, fast_fetch, query, query_fields, fetch_period_list):
    #save_only == generate 6 source/*.csv, e.g income.csv, balance_sheet.csv
    basic_df = get_pro_basic()
    fetch_most_recent_report_perid = finlib.Finlib().get_year_month_quarter()['fetch_most_recent_report_perid'][0]

    if not os.path.exists(fund_base_source):
        os.makedirs(fund_base_source)

    total = str(stock_list.__len__())

    stock_cnt = 0
    for ts_code in stock_list['code']:
        stock_cnt += 1

        fetch_period_list = list(set(fetch_period_list))  # remove duplicate in list
        #fetch_period_list.sort(reverse=False) #20161231 -> 20171231 -> 20181231.
        fetch_period_list.sort(reverse=True)  #20181231 -> 20171231 -> 20161231
        all_per_cnt = fetch_period_list.__len__()

        already_fetch_p = []

        p_cnt = 0

        for period in fetch_period_list:
            p_cnt += 1

            #logging.info("p_cnt "+str(p_cnt)+" stock_cnd "+str(stock_cnt) + " total " + total+" query "+query )
            #continue

            if period in already_fetch_p:
                logging.info("skip period " + period + ", it has been fetched before")
                continue

            dir = fund_base_source + "/individual/" + period
            if not os.path.isdir(dir):
                os.mkdir(dir)

            ind_csv = dir + "/" + ts_code + "_" + query + ".csv"

            #print(ind_csv)

            #if (finlib.Finlib().is_cached(ind_csv, day=3)) and (not force_run_global) :
            if (finlib.Finlib().is_cached(ind_csv, day=6)):
                logging.info("file updated in 3 day, no--concept_topt fetch again " + ind_csv)
                continue
            elif not os.path.exists(ind_csv):
                open(ind_csv, 'a').close()  #create empty
            else:  #exist but ctime is two days before
                now = datetime.datetime.now()
                modTime = time.mktime(now.timetuple())
                os.utime(ind_csv, (modTime, modTime))

            if (not force_run_global) and (period < fetch_most_recent_report_perid):
                logging.info("not fetch stable period on " + ind_csv)
                continue


            if os.path.exists(ind_csv) \
                    and os.stat(ind_csv).st_size > 0 \
                    and period < fetch_most_recent_report_perid:
                already_fetch_p.append(period)
                logging.info("not fetch as file already exists " + ind_csv + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
                continue

            weekday = datetime.datetime.today().weekday()
            #on Friday, the most recent Q fund data will be updated.
            if (not force_run_global) \
                    and os.path.exists(ind_csv) \
                    and os.stat(ind_csv).st_size > 0\
                    and weekday != 5:
                #and os.stat(ind_csv).st_size > 0 \
                #and period < fetch_most_recent_report_perid:
                already_fetch_p.append(period)
                logging.info("file have data already, not fetch out of FRIDAY. " + ind_csv + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
                continue

            if not finlib.Finlib().is_on_market(ts_code, period, basic_df):
                #logging.info()
                logging.info("not fetch as stock is not on market. " + ts_code + " " + period + ". p_cnt " + str(p_cnt) + " stock_cnd " + str(stock_cnt) + " total " + total + " query " + query)
                continue

            signal.signal(signal.SIGALRM, handler)

            try:
                logging.info("fetching period " + str(p_cnt) + " of " + str(all_per_cnt) + " , stock " + str(stock_cnt) + " of " + total + ", Getting " + query + " " + ts_code + " " + period)
                time.sleep(0.7)

                signal.alarm(5)

                if query in ['income', 'balancesheet', 'cashflow', 'fina_indicator', 'fina_audit', 'disclosure_date', 'express', 'fina_mainbz']:
                    if fast_fetch:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                    else:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, end_date=period)
                elif query in ['forecast']:
                    if fast_fetch:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, period=period)
                    else:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                elif query in ['dividend']:
                    if fast_fetch:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields)
                    else:
                        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, end_date=period)

                logging.info(". received len " + str(df_tmp.__len__()))
                logging.info(df_tmp.head(2))
                logging.info(df_tmp.tail(2))

                signal.alarm(0)
                # df_tmp = df_tmp.astype(str)
                # logging.info(df_tmp)

                field = ''
                if 'end_date' in df_tmp.columns:
                    field = 'end_date'
                elif 'period' in df_tmp.columns:
                    field = 'period'
                elif 'ann_date' in df_tmp.columns:
                    field = 'ann_date'

                if (not force_run_global) and fast_fetch:
                    df_tmp = df_tmp[df_tmp[field] == fetch_most_recent_report_perid]

                name = stock_list[stock_list['code'] == ts_code]['name'].values[0]
                df_tmp = pd.DataFrame([name] * df_tmp.__len__(), columns=['name']).join(df_tmp)
                df_tmp = df_tmp.drop_duplicates().reset_index().drop('index', axis=1)

                #df_tmp contains multiple end_date
                end_date_lst = list(df_tmp[field].unique())

                #create an empty csv file if return df is empty.
                #if (not os.path.exists(ind_csv)) and (period not in end_date_lst or df_tmp.__len__()== 0):
                #if (not os.path.exists(ind_csv)):
                #    open(ind_csv, 'a').close()
                #    logging.info("created empty file "+ind_csv)

                for ed in end_date_lst:

                    if ed in already_fetch_p:
                        #print("already fetched " + ed)
                        continue

                    df_tmp_sub = df_tmp[df_tmp[field] == ed]
                    df_tmp_sub = df_tmp_sub.reset_index().drop('index', axis=1)

                    dir_sub = fund_base_source + "/individual/" + ed
                    if not os.path.isdir(dir_sub):
                        os.mkdir(dir_sub)

                    ind_csv_sub = dir_sub + "/" + ts_code + "_" + query + ".csv"

                    if (not os.path.exists(ind_csv_sub)) or (force_run_global) or (os.stat(ind_csv).st_size == 0):
                        df_tmp_sub.to_csv(ind_csv_sub, encoding='UTF-8', index=False)
                        logging.info(__file__ + ": " + "saved " + ind_csv_sub + " . len " + str(df_tmp_sub.__len__()))
                    #else:
                    #logging.info("file exists, "+ind_csv_sub)

                    if not ed in already_fetch_p:
                        already_fetch_p.append(ed)
                        #logging.info("append "+ed +" to already_fetch_p")

            # df_tmp.to_csv(ind_csv, encoding='UTF-8', index=False)

            except:
                logging.info("exception, sleeping 30sec then renew the ts_pro connection")

            finally:

                if sys.exc_info() == (None, None, None):
                    pass  # no exception
                else:
                    # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8')) #python2
                    logging.info(str(traceback.print_exception(*sys.exc_info())).encode('utf8'))  #python3
                    logging.info(sys.exc_value.message)  # print the human readable unincode
                    logging.info("query: " + query + " ts_code: " + ts_code + " period: " + period)
                    sys.exc_clear()


'''
        try:

            #ind_csv = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/invidual/601888.SH_income.csv'
            #ind_csv =


            if force_run_global or fast_fetch or (not finlib.Finlib().is_cached(ind_csv, day=1)):
                #3352 of 3621, Getting Income 000626.SZ. len 89041
                sys.stdout.write(str(j)+" of "+total+", Getting "+query+" "+ts_code+". ")
                sys.stdout.flush()

                df_sub = pd.DataFrame()

                if query in['income','balancesheet','cashflow','fina_indicator','fina_audit','disclosure_date','forecast','express','fina_mainbz']: #forcast requires period input
                    df_sub = _ts_pro_fetch_end_date(pro_con, ts_code, fast_fetch, query, query_fields, fetch_period_list)
                elif query in ['dividend']:
                    df_sub = pro_con.query(query, ts_code=ts_code, fields=query_fields)  #<<<<<< Query tushare api
                    time.sleep(1) #抱歉，您每分最多访问该接口80次:-(，详情访问

                name = stock_list[stock_list['code'] == ts_code]['name'].values[0]
                df_sub = pd.DataFrame([name] * df_sub.__len__(), columns=['name']).join(df_sub)

                df_sub = df_sub.drop_duplicates().reset_index().drop('index', axis=1)
                df_sub.to_csv(ind_csv, encoding='UTF-8', index=False)
                logging.info(__file__ + ": " + "saved sub csv "+ind_csv)
            else:
                logging.info("file has been update in 1 days, not fetch again. " +ind_csv)

            j += 1

        except:
            logging.info("exception, sleeping 30sec then renew the ts_pro connection")

        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                logging.info(sys.exc_value.message) #print the human readable unincode
                logging.info("query: "+query+" ts_code: " + ts_code)
                sys.exc_clear()
'''
'''
def _ts_pro_fetch_end_date(pro_con, ts_code,  query, query_fields, fetch_period_list ):
    fetch_period_list = list(set(fetch_period_list))  # remove duplicate in list
    fetch_period_list.sort()

    for period in fetch_period_list:
        dir = fund_base_source + "/individual/"+period
        if not os.path.isdir(dir):
            os.mkdir(dir)

        ind_csv = dir + "/" + ts_code + "_" + query + ".csv"


        if os.path.exists(ind_csv) and "20151231" > period:
            sys.stdout.write("not fetch as file alreay exists "+ind_csv)
            continue


        if finlib.Finlib().is_cached(ind_csv, 3):
            logging.info("file already updated in 3 days, not fetch again "+ind_csv)
            continue



        #if os.path.exists(ind_csv):
            # df_sub = pd.read_csv(ind_csv)
        #    df_sub = pd.read_csv(ind_csv, converters={i: str for i in range(100)})
        #    df_sub = df_sub.drop(columns="name")
        #else:
        #    df_sub = pd.DataFrame(columns=[])



        logging.info(ts_code + " " + query + " " + period)
        df_tmp = pro_con.query(query, ts_code=ts_code, fields=query_fields, end_date=period)
        time.sleep(1)
        df_tmp = df_tmp.astype(str)
        df_tmp.to_csv(ind_csv, encoding='UTF-8', index=False)
        logging.info("saved "+ind_csv)


        #df_sub = pd.concat([df_tmp, df_sub], sort=False)

    #df_sub = df_sub.drop_duplicates()
    #df_sub = df_sub.sort_values('ann_date', ascending=False, inplace=False).reset_index().drop('index', axis=1)
    #return(df_sub)
'''


#jasq stop work, get PE, PB from tushare. 20190302
def fetch_basic_quarterly():
    ts.set_token(myToken)
    pro = ts.pro_api()

    a = finlib.Finlib().get_year_month_quarter()
    b = a['full_period_list']

    fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm,pb, ps, ps_ttm'
    fields += ',total_share,float_share,total_mv,circ_mv '

    dir_q = fund_base_source + "/basic_quarterly"

    if not os.path.isdir(dir_q):
        os.mkdir(dir_q)

    ### get quarterly

    for i in b:
        output_csv = dir_q + "/basic_" + i + ".csv"  #the date in filename is i but not the actual date of the data.
        if os.path.exists(output_csv) and os.stat(output_csv).st_size >= 10 and (not force_run_global):
            logging.info("file exist and have content, not fetch again " + output_csv)
            continue

        d = finlib.Finlib().get_last_trading_day(date=i)

        reg = re.match("(\d{4})(\d{2})(\d{2})", d)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)

        trade_date = yyyy + mm + dd

        df = pro.daily_basic(ts_code='', trade_date=trade_date, fields=fields)
        time.sleep(1)
        df.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info("saved basic of all stocks to " + output_csv + " len " + str(df.__len__()))


def fetch_basic_daily(fast_fetch=False):
    ts.set_token(myToken)
    pro = ts.pro_api()

    ##### get daily basic
    calendar_f = "/home/ryan/DATA/pickle/trading_day_2020.csv"
    if not os.path.isfile(calendar_f):
        logging.error("no such file " + calendar_f)
        exit()

    trade_days = pandas.read_csv(calendar_f)
    todayS = datetime.datetime.today().strftime('%Y%m%d')

    trading_days = trade_days[(trade_days.cal_date <= int(todayS)) & (trade_days.is_open == 1)]
    trading_days = trading_days.sort_values('cal_date', ascending=False, inplace=False)

    if fast_fetch:  #run on daily, fetch the most recent 5 day only.
        trading_days = trading_days[:5]

    # the file should keep same between t_daily_update_csv_from_tushare_.py and t_daily_fundamentals_2.py
    fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm,pb, ps, ps_ttm'
    fields += ',total_share,float_share,total_mv,circ_mv '

    dir_d = fund_base_source + "/basic_daily"

    if not os.path.isdir(dir_d):
        os.mkdir(dir_d)

    for i in trading_days['cal_date']:
        reg = re.match(r"(\d{4})(\d{2})(\d{2})", str(i))
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)

        trade_date = yyyy + mm + dd

        #trade_date="20191224" #ryan debug

        output_csv = dir_d + "/basic_" + trade_date + ".csv"

        if os.path.exists(output_csv) and os.stat(output_csv).st_size >= 10 and (not force_run_global):
            logging.info("file exist and have content, not fetch again " + output_csv)
            continue

        logging.info("fetch daily_basic on date " + str(trade_date))
        df = pro.daily_basic(ts_code='', trade_date=trade_date, fields=fields)
        time.sleep(1)

        df.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info("saved basic of all stocks to " + output_csv + " len " + str(df.__len__()))

    pass


#input: source/*.csv
#output: source/individual_per_stock/stockid_feature.csv , which include all history. (not the 50 records entry)


def merge_individual():
    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list = stock_list['code']

    list = ['income', 'balancesheet', 'cashflow', 'fina_mainbz', 'dividend', 'fina_indicator', 'fina_audit', 'forecast', 'express', 'disclosure_date']

    if debug_global:
        stock_list = ["600519.SH"]
        stock_list = ["000333.SZ"]

    for ts_code in stock_list:
        for feature in list:
            _merge_individual_bash(ts_code, feature)


#input: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/a_stock_code_feature.csv
def _merge_individual_bash(ts_code, feature):
    logging.info("processing " + ts_code + " " + feature)
    fetch_most_recent_report_perid = finlib.Finlib().get_year_month_quarter()['fetch_most_recent_report_perid'][0]

    input_file = fund_base_source + "/" + feature + ".csv"

    if finlib.Finlib().is_cached(input_file, 6) and (not force_run_global):
        logging.info("not processing as file have been updated in 3 days. " + input_file)
        return

    output_dir = fund_base_source + "/individual_per_stock"
    output_csv = output_dir + "/" + ts_code + "_" + feature + ".csv"

    if finlib.Finlib().is_cached(output_csv, day=6) and (not force_run_global):
        logging.info("file updated in 3 days, not processing. " + output_csv)
        return ()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #f_header = "~/tmp/header.txt"
    #f_content = "~/tmp/content.txt"

    #os.system("rm -f " + f_header + "; ")
    #os.system("rm -f " + f_content + "; ")

    os.system("head -1 " + input_file + " > " + output_csv)

    cmd = "grep " + ts_code + " " + input_file + " >> " + output_csv
    logging.info(cmd)
    os.system(cmd)


#input: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
#output: ~/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/a_stock_code_basic.csv
def merge_individual_bash_basic(fast_fetch=False):
    if fast_fetch:

        last_trade_date = finlib.Finlib().get_last_trading_day()
        reg = re.match("(\d{4})(\d{2})(\d{2})", last_trade_date)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)
        last_trade_date = yyyy + mm + dd

        input_csv = fund_base_source + "/basic_daily/basic_" + last_trade_date + ".csv"

        logging.info("DAILY UPDATE, update " + input_csv + " to source/individual_per_stock/*code*_basic.csv")

        if not os.path.exists(input_csv):
            logging.error("no such file " + input_csv + " , cannot continue")
            exit(0)

        logging.info("read csv " + input_csv)
        df = pd.read_csv(
            input_csv,
            converters={i: str
                        for i in range(20)},
            names=['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_share', 'float_share', 'total_mv', 'circ_mv'])

        totals = df.__len__()
        for cnt in range(totals - 1):
            entry = df.iloc[cnt + 1]
            ts_code = entry['ts_code']
            this_date = entry['trade_date']
            logging.info(str(cnt + 1) + " of " + str(totals - 1) + " " + ts_code)

            output_csv = fund_base_source + "/individual_per_stock/" + ts_code + "_basic.csv"

            if os.path.exists(output_csv):
                df_exist = pd.read_csv(output_csv, converters={i: str for i in range(20)})  #@todo: need to specify the column name. Dangerous
                df_test = df_exist[df_exist.trade_date == this_date]
                if df_test.__len__() == 1:
                    logging.info("file already updated. " + output_csv + " to date " + this_date)
                    continue
                elif df_test.__len__() > 1:
                    logging.info("ERROR, duplicate records in . " + output_csv + " for day " + this_date)
                    continue
                else:
                    df_exist = df_exist.append(entry).reset_index().drop('index', axis=1)
                    df_exist.to_csv(output_csv, encoding='UTF-8', index=False)
                    logging.info("updated day " + this_date + " to " + output_csv + " len " + str(df_exist.__len__()))
            else:
                logging.info("new stock " + ts_code + " no such file, " + output_csv)
                entry.to_csv(output_csv, encoding='UTF-8', index=False)
                logging.info("file saved, len " + str(entry.__len__()))

    if not fast_fetch:
        logging.info("FULL UPDATE, overwrite exists. processing basic, split source/basic.csv to source/individual_per_stock/ts_code_basic.csv")

        check_csv = fund_base_source + "/individual_per_stock/600519.SH_basic.csv"

        if (not force_run_global) and finlib.Finlib().is_cached(check_csv, day=6):
            logging.info("*_basic.csv are updated in 5 days, not process. result checked by " + check_csv)

        tmp_dir = "~/tmp/pro_basic"
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        cmd = "cd " + fund_base_source + ";"

        cmd += "for i in `awk -F',' '{print $1}' basic.csv  | uniq|grep -v ts_code` ; do "
        cmd += "echo ${i}_basic.csv;  head -1 basic.csv > ~/tmp/pro_basic/${i}_basic.csv;"
        cmd += "grep -E \"^$i\" basic.csv >> ~/tmp/pro_basic/${i}_basic.csv; "
        cmd += "mv ~/tmp/pro_basic/${i}_basic.csv " + fund_base_source + "/individual_per_stock/ ;"
        cmd += "done"

        logging.info(cmd)
        os.system(cmd)


'''
def zzz_merge_local():
    _merge_local_bash('income', csv_income, col_list_income) #利润表
    _merge_local_bash('balancesheet',  csv_balancesheet,col_list_balancesheet)#资产负债表
    _merge_local_bash('cashflow',  csv_cashflow,col_list_cashflow)#现金流量表
    _merge_local_bash('fina_indicator',  csv_fina_indicator, col_list_fina_indicator)#财务指标数据
    _merge_local_bash('forecast',  csv_forecast,col_list_forecast)#业绩预告
    _merge_local_bash('dividend',  csv_dividend,col_list_dividend)#分红送股
    _merge_local_bash('express',  csv_express,col_list_express)#业绩快报
    _merge_local_bash('fina_audit',  csv_fina_audit,col_list_fina_audit)#财务审计意见
    _merge_local_bash('fina_mainbz',  csv_fina_mainbz,col_list_fina_mainbz)#主营业务构成
    _merge_local_bash('disclosure_date',  csv_disclosure_date, col_list_disclosure_date)#财报披露计划



    return

    stock_list = finlib.Finlib().get_A_stock_instrment() #603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True) #603999.SH

    #ryan debug start
    if debug_global:
        stock_list=stock_list[stock_list['code']=='600519.SH']
    #ryan debug end

    _merge_local( stock_list,  'income',  csv_income,col_list_income) #利润表
    _merge_local( stock_list,   'balancesheet',  csv_balancesheet,col_list_balancesheet)#资产负债表
    _merge_local( stock_list,   'cashflow',  csv_cashflow,col_list_cashflow)#现金流量表
    _merge_local( stock_list,   'fina_indicator',  csv_fina_indicator, col_list_fina_indicator)#财务指标数据

    _merge_local( stock_list,   'forecast',  csv_forecast,col_list_forecast)#业绩预告
    _merge_local( stock_list,   'dividend',  csv_dividend,col_list_dividend)#分红送股
    _merge_local( stock_list,   'express',  csv_express,col_list_express)#业绩快报

    _merge_local( stock_list,   'fina_audit',  csv_fina_audit,col_list_fina_audit)#财务审计意见
    _merge_local( stock_list,   'fina_mainbz',  csv_fina_mainbz,col_list_fina_mainbz)#主营业务构成
    _merge_local( stock_list,   'disclosure_date',  csv_disclosure_date, col_list_disclosure_date)#财报披露计划

'''
'''

def zzz_merge_local_bash(feature,  output_csv, col_name_list):
    if os.path.isfile(output_csv): #to make things simple, remove the file everytime.
        os.remove(output_csv)

    f_header = "~/tmp/header.txt"
    f_content = "~/tmp/content.txt"

    cmd_header = "for i in `ls "+ fund_base_source+"/individual_per_stock/*_"+feature+".csv`; do sed 1q $i > "+ f_header +"; break; done;"
    #cmd_header = "for i in `ls "+ fund_base_source+"/individual/*_"+feature+".csv`; do sed 1q $i > "+ f_header +"; break; done;"

    cmd_content = "rm -f "+f_content+"; "
    cmd_content += "for i in `ls "+ fund_base_source+"/individual_per_stock/*_"+feature+".csv`; do sed 1d $i >> "+ f_content +"; done;"
    #cmd_content += "for i in `ls "+ fund_base_source+"/individual/*_"+feature+".csv`; do sed 1d $i >> "+ f_content +"; done;"

    cmd_exist_content_remove_header_overwrite = "sed -i 1d "+output_csv

    #cmd_uniq = "cat "+f_content +" | sort | uniq > "+f_content
    cmd_uniq = "sort -u -o "+f_content+ " "+f_content

    if ((os.path.isfile(output_csv)) and os.stat(output_csv).st_size >= 10):
        #There is a problem, the new generate conent colums are more than exists file.
        #So never let the script runs to here.
        #os.system(cmd_exist_content_remove_header_overwrite) #remove exist output file header
        #os.system(cmd_header) #generate header to f_header
        #os.system(cmd_content) #generate new content to f_content
        #os.system("cat "+output_csv+" >> "+ f_content) #append  output_csv to f_content, f_content is all the content.
        #os.system(cmd_uniq) #sort,uniq the content to f_content
        #os.system("cat "+f_content + ">> "+f_header) #append f_content to f_header
        #os.system("mv "+f_header+ " "+output_csv)
        #os.system("rm -f "+f_content)
        pass
    else:
        #logging.info(cmd_header)
        os.system(cmd_header)

        #logging.info(cmd_content)
        os.system(cmd_content)

        #logging.info(cmd_uniq)
        os.system(cmd_uniq)

        cmd="cat " + f_content + " >> " + f_header
        #logging.info(cmd)
        os.system(cmd)

        cmd="mv " + f_header + " " + output_csv
        #logging.info(cmd)
        os.system(cmd)

        cmd="rm -f " + f_content
        #logging.info(cmd)
        os.system(cmd)

    df = pd.read_csv(output_csv, converters={i: str for i in range(100)})
    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info("warning, no column named " + i + " in cols")

    df = df[cols]
    df = df.fillna(0)

    if not df.empty:
        df = df.reset_index().drop('index', axis=1)

        df.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + output_csv+" . len "+str(df.__len__()))
        df = None    #free memory
        df_exist_all = None
    else:
        logging.info("df is empty")


    pass




'''


#########################
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual/*.csv
#output : /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv [ fina_mainbz_sum.csv,balancesheet.csv,dividend.csv,fina_audit.csv,fina_mainbz.csv,forecast.csv]
#
#########################
def merge_local_bash():
    features = ['income', 'balancesheet', 'cashflow', 'fina_mainbz', 'dividend', 'fina_indicator', 'fina_audit', 'forecast', 'express', 'disclosure_date']

    input_dir = fund_base_source + "/individual"

    if not os.path.exists(fund_base_tmp):
        os.makedirs(fund_base_tmp)

    for f in features:
        tmp_f = fund_base_tmp + "/" + f + ".txt"
        os.system("rm -f " + tmp_f)

        output_csv = fund_base_source + "/" + f + ".csv"

        if (not force_run_global) and finlib.Finlib().is_cached(output_csv, day=6):
            logging.info("file updated in 3 days, not process. " + output_csv)
            continue

        cmd = " find -L " + input_dir + " -name *_" + f + ".csv  -exec cat {} >> " + tmp_f + " \;"
        logging.info(cmd)
        start_time = time.time()
        os.system(cmd)
        logging.info("--- %s seconds ---" % (time.time() - start_time))

        #sort and uniq, inplace

        os.system("head -1 " + tmp_f + " > " + output_csv)  #make header

        os.system("sort -u -o " + tmp_f + " " + tmp_f)
        os.system("grep -vE \"ts_code.*name|name.*ts_code\" " + tmp_f + " >> " + output_csv)  #append body
        os.system("rm -f " + tmp_f)

        #os.system("mv "+tmp_f +  " "+output_csv)
        logging.info("merged all " + f + " to " + output_csv)


###############################
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/*.csv
#output:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic.csv
###############################
def merge_local_bash_basic(output_csv, fast=False):
    logging.info("DAILY merge local basic")
    logging.info(output_csv)
    if (not fast) and (not force_run_global) and finlib.Finlib().is_cached(output_csv, 6) and (os.stat(output_csv).st_size >= 10):
        logging.info("file is updated in 5 days, not merge again. " + output_csv)
        return ()

    f_header = "~/tmp/header.txt"
    f_content = "~/tmp/content.txt"

    if fast:  #run daily, merge daily to the basic.csv
        last_trade_date = finlib.Finlib().get_last_trading_day()
        reg = re.match("(\d{4})(\d{2})(\d{2})", last_trade_date)
        yyyy = reg.group(1)
        mm = reg.group(2)
        dd = reg.group(3)
        last_trade_date = yyyy + mm + dd

        input_csv = fund_base_source + "/basic_daily/basic_" + last_trade_date + ".csv"

        if not os.path.exists(input_csv):
            logging.error("no such file " + input_csv + " , cannot continue")
            exit(0)

        if not os.path.exists(output_csv):
            logging.error("no such file " + output_csv + " , cannot continue")
            exit(0)

        logging.info("read csv " + output_csv)
        df = pd.read_csv(
            output_csv,
            skiprows=9390000,
            converters={i: str
                        for i in range(20)},
            names=['ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_share', 'float_share', 'total_mv', 'circ_mv'])
        if not df.empty:
            df = df[df.trade_date == last_trade_date]
            if df.__len__() > 1000:  #should have more than 3000+ records
                logging.info(str(df.__len__()) + " records were found, date " + last_trade_date + " should have already updated to " + output_csv)
                return ()

        cmd_content = "sed 1d " + input_csv + " > " + f_content
        logging.info(cmd_content)
        os.system(cmd_content)

        cmd_content = "cat " + f_content + " >> " + output_csv
        logging.info(cmd_content)
        os.system(cmd_content)

        cmd_content = "rm -f " + f_content
        logging.info(cmd_content)
        os.system(cmd_content)

        logging.info("merged latest trading date" + last_trade_date + " to " + output_csv)
        return ()

    if (not fast):
        logging.info("FULLY merge local basic")
        cmd_header = "for i in `ls " + fund_base_source + "/basic_daily/basic_*.csv`; do sed 1q $i > " + f_header + "; break; done;"

        cmd_content = "rm -f " + f_content + "; "
        cmd_content += "for i in `ls " + fund_base_source + "/basic_daily/basic_*.csv`; do sed 1d $i >> " + f_content + "; done;"

        #cmd_exist_content_remove_header_overwrite = "sed -i 1d "+output_csv

        cmd_uniq = "sort -u -o " + f_content + " " + f_content

        logging.info(cmd_header)
        os.system(cmd_header)

        logging.info(cmd_content)
        os.system(cmd_content)

        logging.info(cmd_uniq)
        os.system(cmd_uniq)

        cmd = "cat " + f_content + " >> " + f_header
        logging.info(cmd)
        os.system(cmd)

        cmd = "mv " + f_header + " " + output_csv
        logging.info(cmd)
        os.system(cmd)

        cmd = "rm -f " + f_content
        logging.info(cmd)
        os.system(cmd)
        return
    ''' ### pandas operation being killed by linux. csv file 1G+
    df = pd.read_csv(output_csv, converters={i: str for i in range(20)})
    df = df.fillna(0)

    if not df.empty:
        df = df.reset_index().drop('index', axis=1)

        df.to_csv(output_csv, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "saved, " + output_csv+" . len "+str(df.__len__()))
    else:
        logging.info("df is empty")
    '''


###############################
#input: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/*.csv
#output:  /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly.csv
###############################
def merge_local_bash_basic_quarterly():
    output_csv = fund_base_source + "/basic_quarterly.csv"

    logging.info("DAILY merge local basic quartly")
    logging.info(output_csv)
    if (not force_run_global) and finlib.Finlib().is_cached(output_csv, 6) and (os.stat(output_csv).st_size >= 10):
        logging.info("file is updated in 5 days, not merge again. " + output_csv)
        return ()

    f_header = "~/tmp/header.txt"
    f_content = "~/tmp/content.txt"

    logging.info("merge local basic quarterly starts")
    cmd_header = "for i in `ls " + fund_base_source + "/basic_quarterly/basic_*.csv`; do sed 1q $i > " + f_header + "; break; done;"

    cmd_content = "rm -f " + f_content + "; "
    cmd_content += "for i in `ls " + fund_base_source + "/basic_quarterly/basic_*.csv`; do sed 1d $i >> " + f_content + "; done;"

    #cmd_exist_content_remove_header_overwrite = "sed -i 1d "+output_csv

    cmd_uniq = "sort -u -o " + f_content + " " + f_content

    logging.info(cmd_header)
    os.system(cmd_header)

    logging.info(cmd_content)
    os.system(cmd_content)

    logging.info(cmd_uniq)
    os.system(cmd_uniq)

    cmd = "cat " + f_content + " >> " + f_header
    logging.info(cmd)
    os.system(cmd)

    cmd = "mv " + f_header + " " + output_csv
    logging.info(cmd)
    os.system(cmd)

    cmd = "rm -f " + f_content
    logging.info(cmd)
    os.system(cmd)
    logging.info("merge local basic quarterly completed , saved to " + output_csv)
    return


def sum_fina_mainbz():

    if (not force_run_global) and finlib.Finlib().is_cached(csv_fina_mainbz_sum, day=6):
        logging.info("skip file, it been updated in 3 day. " + csv_fina_mainbz_sum)
        return

    df = pd.read_csv(csv_fina_mainbz, converters={'end_date': str})

    df = df.fillna(0)

    df_result = pd.DataFrame(columns=list(df.columns))
    df_result = df_result.drop('bz_item', axis=1)

    lst = list(df['ts_code'].unique())
    lst.sort()

    i = 0

    for code in lst:
        df_tmp = df[df['ts_code'] == code]

        ed = list(df_tmp['end_date'].unique())

        for e in ed:
            sys.stdout.write(code + " " + e + ". ")
            sys.stdout.flush()

            df_code_date = df_tmp[df_tmp['end_date'] == e]

            df_code_date_cny = df_code_date[df_code_date['curr_type'] == 'CNY']
            df_code_date_usd = df_code_date[df_code_date['curr_type'] == 'USD']

            ts_code = code
            end_date = e
            name = df_code_date.iloc[0]['name']
            #USD_DIV_CNY=6

            bz_sales = df_code_date_cny.bz_sales.sum() + df_code_date_usd.bz_sales.sum() * USD_DIV_CNY
            bz_profit = df_code_date_cny.bz_profit.sum() + df_code_date_usd.bz_profit.sum() * USD_DIV_CNY
            bz_cost = df_code_date_cny.bz_cost.sum() + df_code_date_usd.bz_cost.sum() * USD_DIV_CNY

            df_result.loc[i] = pd.Series({'ts_code': ts_code, 'name': name, 'end_date': end_date, 'bz_sales': bz_sales, 'bz_profit': bz_profit, 'bz_cost': bz_cost})

            i += 1

    df_result.to_csv(csv_fina_mainbz_sum, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "sum of fina_mainbz saved to " + csv_fina_mainbz_sum + " , len " + str(df_result.__len__()))
    return (df)


def percent_fina_mainbz():

    if (not force_run_global) and finlib.Finlib().is_cached(csv_fina_mainbz_latest_percent, day=6):
        logging.info("skip file, it been updated in 6 day. " + csv_fina_mainbz_latest_percent)
        return

    df = pd.read_csv(csv_fina_mainbz_latest, converters={'end_date': str})

    df = df.fillna(0)

    df_result = pd.DataFrame()

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_sales'])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_profit'])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['perc_cost'])
    df = new_value_df.join(df)

    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['bz_cnt'])
    df = new_value_df.join(df)

    lst = list(df['ts_code'].unique())
    lst.sort()

    i = 0

    for code in lst:
        sys.stdout.write(code + " ")
        sys.stdout.flush()

        df_tmp = df[df['ts_code'] == code].reset_index().drop('index', axis=1)

        df_code_date_cny = df_tmp[df_tmp['curr_type'] == 'CNY']
        df_code_date_usd = df_tmp[df_tmp['curr_type'] == 'USD']

        cost_sum = df_code_date_cny['bz_cost'].sum() + df_code_date_usd['bz_cost'].sum() * USD_DIV_CNY
        profit_sum = df_code_date_cny['bz_profit'].sum() + df_code_date_usd['bz_profit'].sum() * USD_DIV_CNY
        sales_sum = df_code_date_cny['bz_sales'].sum() + df_code_date_usd['bz_sales'].sum() * USD_DIV_CNY

        for j in range(df_tmp.__len__()):
            df_tmp.iloc[j, df_tmp.columns.get_loc('bz_cnt')] = df_tmp.__len__()

            if cost_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_cost')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_cost')] / cost_sum, 0)

            if profit_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_profit')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_profit')] / profit_sum, 0)

            if sales_sum != 0 and "CNY" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_sales')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_sales')] / sales_sum, 0)

            if cost_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_cost')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_cost')] * USD_DIV_CNY / cost_sum, 0)

            if profit_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_profit')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_profit')] * USD_DIV_CNY / profit_sum, 0)

            if sales_sum != 0 and "USD" == df_tmp.iloc[j, df_tmp.columns.get_loc('curr_type')]:
                df_tmp.iloc[j, df_tmp.columns.get_loc('perc_sales')] = round(100 * df_tmp.iloc[j, df_tmp.columns.get_loc('bz_sales')] * USD_DIV_CNY / sales_sum, 0)

        df_result = df_result.append(df_tmp)

    df_result = df_result.reset_index().drop('index', axis=1)
    df_result = df_result[['ts_code', 'name', 'end_date', 'bz_cnt', 'perc_cost', 'perc_profit', 'perc_sales', 'bz_cost', 'bz_item', 'bz_profit', 'bz_sales', 'curr_type']]

    df_result.to_csv(csv_fina_mainbz_latest_percent, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "percent of fina_mainbz saved to " + csv_fina_mainbz_latest_percent + "  , len " + str(df_result.__len__()))
    return (df)


'''
def zzz_merge_local(stock_list, feature,  output_csv, col_name_list):
    if not os.path.exists(fund_base_source):
        os.makedirs(fund_base_source)

    df_exist_all = pd.DataFrame(columns=['ts_code','end_date']) #including all the ts_code

    if (not force_run_global) and finlib.Finlib().is_cached(output_csv, day=3):
        logging.info("skip file, it been updated in 3 day. "+output_csv)
        return
    ## start of load from exist csv
    # fast_fetch will updating source/*.csv based on this.
    # none fast_fetch (no source/*.csv exist) will combine from source/individual/*.csv

    if ((os.path.isfile(output_csv)) and os.stat(output_csv).st_size >= 10):  # > 10 bytes
        #df_exist_all = pd.read_csv(output_csv, converters={'end_date': str})
        df_exist_all = pd.read_csv(output_csv, converters={i: str for i in range(100)})
        df_exist_all = df_exist_all.drop_duplicates()
        logging.info("loaded " + output_csv + ", len " + str(df_exist_all.__len__()))
    else:
        logging.info("File not exist, no local history reocrds need to merge, generate new from individual/*.csv. "+output_csv)


    ## end of load from exist csv

    j = 1
    total_len = stock_list.__len__()

    for ts_code in stock_list['code']:
        #try:
            exc_info = sys.exc_info()

            df_sub = pd.DataFrame()

            ind_csv = fund_base_source+"/individual_per_stock/"+ts_code+"_"+feature+".csv"

            if (not os.path.exists(ind_csv)): #@todo: split merge_local and save_only to two functions
                j = j+1
                continue

            sys.stdout.write("_merge_local " +feature+" "+  str(j)+" of "+str(total_len)+". ")
            sys.stdout.flush()

            #logging.info("file will be reading "+ind_csv)

            #df_sub = pd.read_csv(ind_csv, converters={'end_date': str})
            df_sub = pd.read_csv(ind_csv, converters={i: str for i in range(100)}) #read all columns as string

            j += 1


            df_exist = df_exist_all[df_exist_all['ts_code']==ts_code]
            df_exist_exclude = df_exist_all[df_exist_all['ts_code']!=ts_code]

            ### start of merge with df_exist
            #records in exist csv only: will be keep.
            #records in both exist csv and new fetch df: will use records in new fetched df
            #records in new fetched df only: will be keep.

            df_new = df_sub
            df_exist_tmp = df_exist[['ts_code', 'end_date']]
            df_new_tmp = df_sub[['ts_code', 'end_date']]

            df_diff = pd.merge(df_new_tmp, df_exist_tmp, how='outer', left_on=['ts_code', 'end_date'],
                               right_on=['ts_code', 'end_date'], indicator='Exist')
            df_diff_only = df_diff.loc[df_diff['Exist'] == 'right_only']

            df_exist_only = df_exist[df_exist['end_date'].isin(list(df_diff_only['end_date'].values))]
            sys.stdout.write("Records only exsits in local csv, len "+str(df_exist_only.__len__())+". ")
            sys.stdout.flush()


            df_result = pd.concat([df_exist_only, df_new], sort=False).sort_values(by='end_date', ascending=False).reset_index(
                drop=True)
            df_result = df_result[df_sub.columns]
            sys.stdout.write("Records will dump to local csv, len " + str(df_result.__len__())+". ")
            sys.stdout.flush()


            df_sub = df_result
            ### end of merge with df_exist

            #df = df.append(df_sub)
            df_exist_all =  pd.concat([df_exist_exclude, df_sub], sort=False)

            df_sub = pd.DataFrame() #empty the df_sub
            logging.info("csv len "+str(df_exist_all.__len__()))


        #except:
        #    logging.info(traceback.print_exception(*exc_info))
        #    logging.info("exception, sleeping 30sec then renew the ts_pro connection")

        #finally:
        #    if exc_info == (None, None, None):
        #        pass  # no exception
        #    else:
        #        logging.info(traceback.print_exception(*exc_info))
        #        del exc_info


    #adjust column sequence here
    df = df_exist_all

    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info("warning, no column named " + i + " in cols")

    df = df[cols]
    df = df.fillna(0)
    df = df.reset_index().drop('index', axis=1)


    df.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved, " + output_csv)
    df = None    #free memory
    df_exist_all = None

'''


#''' COMMENT THIS FUC TO SEE IF ANY OTHER USES IT
#########################
#merge all 9 tables to one table, "merged_all_"+end_date+".csv"
#input:/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/*.csv
#output: /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/merged/*.csv
######################
def merge_quarterly(fast=False):
    # requires lots memory. >10 GB estimated.

    #df_income = pd.read_csv(csv_income, converters={'end_date': str})
    df_income = pd.read_csv(csv_income, converters={i: str for i in range(1000)})

    lst = []

    if not fast:
        lst = list(df_income['end_date'].unique())
        lst.sort()
    else:
        #lst = finlib.Finlib().get_report_publish_status()['period_to_be_checked_lst']  # period to be checked at this time point (based on month)
        lst = finlib.Finlib().get_year_month_quarter()['fetch_most_recent_report_perid'][0]  # period to be checked at this time point (based on month)

    #df_balancesheet = pd.read_csv(csv_balancesheet, converters={'end_date': str})
    df_balancesheet = pd.read_csv(csv_balancesheet, converters={i: str for i in range(1000)})  #convert all columns as string
    #df_cashflow = pd.read_csv(csv_cashflow, converters={'end_date': str})
    df_cashflow = pd.read_csv(csv_cashflow, converters={i: str for i in range(1000)})
    #df_fina_indicator = pd.read_csv(csv_fina_indicator, converters={'end_date': str})
    df_fina_indicator = pd.read_csv(csv_fina_indicator, converters={i: str for i in range(1000)})
    #df_fina_audit = pd.read_csv(csv_fina_audit, converters={'end_date': str})
    df_fina_audit = pd.read_csv(csv_fina_audit, converters={i: str for i in range(1000)})
    #df_fina_mainbz = pd.read_csv(csv_fina_mainbz_sum, converters={'end_date': str})
    df_fina_mainbz = pd.read_csv(csv_fina_mainbz_sum, converters={i: str for i in range(1000)})

    # do not need this df.
    #df_disclosure_date = pd.read_csv(csv_disclosure_date, converters={'end_date': str})
    #df_disclosure_date = pd.read_csv(csv_disclosure_date, converters={i: str for i in range(1000)})

    for i in lst:
        _merge_quarterly(i, df_income, df_balancesheet, df_cashflow, df_fina_indicator, df_fina_audit, df_fina_mainbz)


#'''


#''' COMMENT THIS FUC TO SEE IF ANY OTHER USES IT
def _merge_quarterly(end_date, df_income, df_balancesheet, df_cashflow, df_fina_indicator, df_fina_audit, df_fina_mainbz):
    if not os.path.exists(fund_base_merged):
        os.makedirs(fund_base_merged)

    output_csv = fund_base_merged + "/merged_all_" + end_date + ".csv"

    if (not force_run_global) and finlib.Finlib().is_cached(output_csv, day=6):
        logging.info("file has been updated in 2 days, will not calculate. " + output_csv)
        return

    i = 0
    logging.info("\n==== " + end_date + " ====")

    sys.stdout.write("\tdf_income, ")
    sys.stdout.flush()

    df_income = df_income[df_income['end_date'] == end_date]
    df_income = df_income.drop_duplicates()
    df_income = remove_dup_record(df_input=df_income, csv_name='df_income')

    df_result_d = df_income

    cols = str(df_result_d.columns.__len__())
    lens = str(df_result_d.__len__())
    logging.info("cols " + cols + ", lens " + lens)

    #logging.info(df_result_d[df_result_d['ts_code']=='000001.SZ'].__len__())

    for df_name in ['df_balancesheet', 'df_cashflow', 'df_fina_indicator', 'df_fina_audit', 'df_fina_mainbz']:
        #i += 1
        #suffix = "_x"+str(i)
        suffix = "_" + df_name
        sys.stdout.write("\t" + df_name + ", ")
        sys.stdout.flush()

        df = eval(df_name)

        df = df[df['end_date'] == end_date]
        df = df.drop('name', axis=1)
        #if 'ann_date' in list(df.columns):
        #    df = df.drop('ann_date', axis=1)
        df = df.drop_duplicates()
        df = remove_dup_record(df_input=df, csv_name=df_name)

        df_result_d = pd.merge(df_result_d, df, how='outer', on=['ts_code'], suffixes=('', suffix))
        cols = str(df_result_d.columns.__len__())
        lens = str(df_result_d.__len__())

        logging.info("cols " + cols + ", lens " + lens)

    df_result_d = df_result_d.drop_duplicates()
    df_result_d.replace('', 0, inplace=True)  #replace '' value to 0, otherwise will cause score to NaN in later analyse step1.
    df_result_d = df_result_d.fillna(0)
    df_result_d.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved " + output_csv + " . len " + str(df_result_d.__len__()))


#'''


#########################
#
#input:source/*.csv
#output: source/latest/*.csv
######################
def extract_latest():
    if not os.path.exists(fund_base_latest):
        os.makedirs(fund_base_latest)

    _extract_latest(csv_input=csv_income, csv_output=csv_income_latest, feature='income', col_name_list=col_list_income)
    _extract_latest(csv_input=csv_balancesheet, csv_output=csv_balancesheet_latest, feature='balancesheet', col_name_list=col_list_balancesheet)
    _extract_latest(csv_input=csv_cashflow, csv_output=csv_cashflow_latest, feature='cashflow', col_name_list=col_list_cashflow)
    _extract_latest(csv_input=csv_forecast, csv_output=csv_forecast_latest, feature='forecast', col_name_list=col_list_forecast)
    _extract_latest(csv_input=csv_dividend, csv_output=csv_dividend_latest, feature='dividend', col_name_list=col_list_dividend)
    _extract_latest(csv_input=csv_express, csv_output=csv_express_latest, feature='express', col_name_list=col_list_express)
    _extract_latest(csv_input=csv_fina_indicator, csv_output=csv_fina_indicator_latest, feature='fina_indicator', col_name_list=col_list_fina_indicator)
    _extract_latest(csv_input=csv_fina_audit, csv_output=csv_fina_audit_latest, feature='fina_audit', col_name_list=col_list_fina_audit)
    _extract_latest(csv_input=csv_fina_mainbz, csv_output=csv_fina_mainbz_latest, feature='fina_mainbz', col_name_list=col_list_fina_mainbz)
    _extract_latest(csv_input=csv_fina_mainbz_sum, csv_output=csv_fina_mainbz_sum_latest, feature='fina_mainbz_sum', col_name_list=col_list_fina_mainbz)
    _extract_latest(csv_input=csv_disclosure_date, csv_output=csv_disclosure_date_latest, feature='disclosure_date', col_name_list=col_list_disclosure_date)


def _extract_latest(csv_input, csv_output, feature, col_name_list, ts_code=None, end_date=None):
    if not os.path.exists(csv_input):
        logging.info("skip, input csv doesn't exist " + csv_input)
        return

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info("skip file, it been updated in 1 day. " + csv_output)
        return

    if os.stat(csv_input).st_size == 0:
        logging.info("skip, empty input file " + csv_input)
        return

    df_result = pd.DataFrame()
    df = pd.read_csv(csv_input, converters={i: str for i in range(100)})  #convert all columns as string

    if ts_code is not None:
        df = df[df['ts_code'] == ts_code]

    if end_date is not None:
        df = df[df['end_date'] == end_date]  #in format 20180630

    i = 1
    total = str(df['ts_code'].unique().__len__())

    for ts_code in df['ts_code'].unique():
        logging.info(str(i) + " of " + total + ", extracting latest " + feature + " of " + ts_code)
        i += 1
        df_tmp = df[df["ts_code"] == ts_code]
        max_date = df_tmp['end_date'].max()
        df_tmp = df_tmp[df_tmp["end_date"] == max_date]
        df_result = df_result.append(df_tmp)
        pass

        #df_tmp = df_tmp.sort_values(by='end_date', ascending=False).reset_index(drop=True).head(1)
        #df_result = df_result.append(df_tmp.iloc[0])

    cols = df_result.columns.tolist()
    name_list = list(reversed(col_name_list))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info("warning, no column named " + i + " in cols")

    df_result = df_result[cols]
    df_result.fillna(0, inplace=True)

    if df_result.__len__() > 0:
        logging.info("\n=== DataFrame " + feature + " ===")
        logging.info(df_result.iloc[0].astype(str))
        logging.info("\n")

    df_result.to_csv(csv_output, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved to " + csv_output + " . len " + str(df_result.__len__()))


def _analyze_step_1(end_date):

    logging.info("=== analyze step 1 ===")
    #end_date in format 20171231
    output_dir = fund_base_report + "/step1"
    csv_output = fund_base_report + "/step1/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    f = fund_base_merged + "/" + "merged_all_" + end_date + ".csv"

    if not os.path.isfile(f):
        logging.info("input file not found " + f)
        return

    #df['net_profit'].describe()
    df = pd.read_csv(f, converters={'ts_code': str, 'end_date': str})

    #profit > 1E+8.  Have exception while loading on haha_65
    #df = df[df.net_profit > 1E+6] #1 million
    #df = df[df.bz_profit > 1E+8] #0.1 billion
    #df = df[~df.name.str.contains("ST")] #remove ST

    #if debug_global or True:  #ryan debug
    if debug_global:
        df = df[df['ts_code']=='600519.SH'].reset_index().drop('index', axis=1)
        #df = df.loc[df['ts_code'].isin(['000501.SZ', '600511.SH', '600535.SH', '600406.SH', '600519.SH', '600520.SH', '600518.SH', '600503.SH', '600506.SH'])].reset_index().drop('index', axis=1)

    lst = list(df['ts_code'].unique())
    lst.sort()

    df = pd.DataFrame([0] * df.__len__(), columns=['finExpToGr']).join(df)  #财务费用/营业总收入, insert to header
    df = pd.DataFrame([0] * df.__len__(), columns=['optPrftM']).join(df)  #营业利润率
    df = pd.DataFrame([0] * df.__len__(), columns=['cashProfitM']).join(df)  #长周期来看（10年以上），净利润应该跟经营活动现金流量净额相等或近似相等，即净现比约等于1，当然越大越好。
    df = pd.DataFrame([0] * df.__len__(), columns=['lightAssert']).join(df)  #轻资产
    df = pd.DataFrame([0] * df.__len__(), columns=['sum_assert']).join(df)  #固定资产+在建工程+工程物资+无形资产里的土地
    df = pd.DataFrame([0] * df.__len__(), columns=['sumRcv']).join(df)  # sum_应收
    df = pd.DataFrame([0] * df.__len__(), columns=['sumRcvNet']).join(df)  #sum_应收-应收票据
    df = pd.DataFrame([0] * df.__len__(), columns=['srnAsstM']).join(df)  #（sum-应收票据）/资产总计
    df = pd.DataFrame([0] * df.__len__(), columns=['bzsAstM']).join(df)  #bz_sales/total_assets
    df = pd.DataFrame([0] * df.__len__(), columns=['revAbnM']).join(df)  # （c_fr_sale_sg + sum_应收）/revenue. 靠近0标准，-10~10正常，数字为正且大好，说明交税少
    df = pd.DataFrame([0] * df.__len__(), columns=['boolrevAbn']).join(df)  # bool_revAbn=1 if <-10 or >10
    df = pd.DataFrame([0] * df.__len__(), columns=['cashPrfAbn']).join(df)  #判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=['saleIncAbn']).join(df)  #判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=['cashInvAbn']).join(df)  #判断是否异常
    df = pd.DataFrame([0] * df.__len__(), columns=['cashInc']).join(df)  #　现金及现金等价物净增加额　＋ 分配股利、利润或偿付利息支付的现金
    df = pd.DataFrame([0] * df.__len__(), columns=['cashLiabM']).join(df)  #期末现金及现金等价物余额  / 负债合计 》 1
    df = pd.DataFrame([0] * df.__len__(), columns=['hen']).join(df)  #老母鸡型, =0 means not hen.
    df = pd.DataFrame([0] * df.__len__(), columns=['cow']).join(df)  #奶牛型, =0 means not cow
    df = pd.DataFrame([0] * df.__len__(), columns=['revCogM']).join(df)  #营业总收入/营业总成本
    df = pd.DataFrame([0] * df.__len__(), columns=['curAssliaM']).join(df)  #流动资产合计/流动负债合计
    df = pd.DataFrame([''] * df.__len__(), columns=['bonusReason']).join(df)
    df = pd.DataFrame([''] * df.__len__(), columns=['garbageReason']).join(df)  #垃圾股原因, insert to header
    df = pd.DataFrame([''] * df.__len__(), columns=['bonusCnt']).join(df)
    df = pd.DataFrame([''] * df.__len__(), columns=['garbageCnt']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['stopProcess']).join(df)  #垃圾股, insert to header

    #if ('name' in df.columns):  tmp_df = df['name']; df.drop('name', axis=1, inplace=True); df.insert(0, 'name', tmp_df)
    field = 'name'
    if (field in df.columns):
        tmp_df = df.pop(field)
        df.insert(0, field, tmp_df)

    field = 'ts_code'
    if (field in df.columns):
        tmp_df = df.pop(field)
        df.insert(0, field, tmp_df)

    basic_df = get_pro_basic()

    df_len = df.__len__()

    for i in range(0, df_len):
        sys.stdout.write("=== analyze step_1, " + str(i + 1) + " of " + str(df_len) + ". ")

        garbageReason = ''
        bonusReason = ''
        bonusCnt = 0
        garbageCnt = 0

        ####xiao xiong start
        ts_code = df.iloc[i]['ts_code']
        end_date = df.iloc[i]['end_date']

        sys.stdout.write(ts_code + " , " + end_date + " ===\n")
        sys.stdout.flush()

        if end_date == '20171231':
            pass  #debug

        if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
            logging.info("stock has been not on market. " + ts_code + " , " + end_date)
            #df = df[df['ts_code'] != ts_code]  #remove the ts_code from df that saved in csv. <<< bug introduced.
            continue

        #debug
        #logging.info("i is " + str(i))
        #continue

        # ryan debug start
        '''
        if not re.match("\d{6}", end_date):
            logging.info("end date wrong "+end_date)
        continue

        exit()
        #ryan debug end
        '''

        dict = _analyze_xiaoxiong_ct(ts_code=ts_code, end_date=end_date, basic_df=basic_df)

        garbageReason += dict['garbageReason']
        bonusReason += dict['bonusReason']
        bonusCnt += dict['bonusCnt']
        garbageCnt += dict['garbageCnt']

        #white horse
        dict = _analyze_white_horse_ct(ts_code=ts_code, end_date=end_date, basic_df=basic_df)
        garbageReason += dict['garbageReason']
        bonusReason += dict['bonusReason']
        bonusCnt += dict['bonusCnt']
        garbageCnt += dict['garbageCnt']
        ####xiao xiong end

        #audit_result
        #标准无保留意见|带强调事项段的无保留意见
        audit_result = df.iloc[i]['audit_result']
        audit_result = str(audit_result)

        if not (audit_result == "标准无保留意见" or audit_result == '0'):
            garbageReason += "audit_result:" + audit_result + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        n_income_attr_p = df.iloc[i]['n_income_attr_p']  #净利润(不含少数股东损益)
        net_profit = df.iloc[i]['net_profit']  #净利润

        if net_profit < 0:
            garbageReason += "net profit < 0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if net_profit > 0 and n_income_attr_p / net_profit < 0.5:
            garbageReason += "minor stock holder shares major(>50%) profit" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        #流动资产合计 > 流动负债合计
        total_cur_assets = df.iloc[i]['total_cur_assets']  #流动资产合计
        total_cur_liab = df.iloc[i]['total_cur_liab']  #流动负债合计
        total_nca = df.iloc[i]['total_nca']  #非流动资产合计
        total_ncl = df.iloc[i]['total_ncl']  #非流动负债合计
        total_revenue = df.iloc[i]['total_revenue']  #营业总收入
        total_cogs = df.iloc[i]['total_cogs']  #营业总成本
        n_income = df.iloc[i]['n_income']  #净利润(含少数股东损益)
        n_income_attr_p = df.iloc[i]['n_income_attr_p']  #归属于母公司所有者的净利润

        if n_income < 0:
            garbageReason += "n_income 净利润(含少数股东损益) <  0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if n_income_attr_p < 0:
            garbageReason += "n_income_attr_p 归属于母公司所有者的净利润 <  0" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if total_cur_assets < total_cur_liab:
            garbageReason += "total_cur_assets <  total_cur_liab" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if total_cur_assets < total_nca:
            garbageReason += "total_cur_assets <  total_nca" + ". "
            garbageCnt += 1

        if total_cogs < 0:
            garbageReason += "total_cogs <  0" + ". "
            garbageCnt += 1

        if total_revenue < total_cogs:
            garbageReason += "total_revenue <  total_cogs" + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        revCogM = finlib.Finlib().measureValue(total_revenue, total_cogs)  #营业总收入/营业总成本
        curAssliaM = finlib.Finlib().measureValue(total_cur_assets, total_cur_liab)  #流动资产合计/流动负债合计

        df.iloc[i, df.columns.get_loc('revCogM')] = round(revCogM, 1)
        df.iloc[i, df.columns.get_loc('curAssliaM')] = round(curAssliaM, 1)

        #finExpToGr

        fin_exp = df.iloc[i]['fin_exp']
        total_revenue = df.iloc[i]['total_revenue']
        if total_revenue != 0.0:
            finExpToGr = fin_exp * 100 / total_revenue
            df.iloc[i, df.columns.get_loc('finExpToGr')] = round(finExpToGr, 1)

        if total_revenue < 1000000:
            garbageReason += "total_revenue less than 1M:" + str(total_revenue) + ". "
            #df.iloc[i, df.columns.get_loc('garbageReason')] += garbageReason+". "
            garbageCnt += 1

        #optPrftM
        operate_profit = df.iloc[i]['operate_profit']
        revenue = df.iloc[i]['revenue']
        if revenue != 0.0:
            optPrftM = operate_profit * 100 / revenue
            df.iloc[i, df.columns.get_loc('optPrftM')] = round(optPrftM, 1)

        if operate_profit < 1000000:
            garbageReason += "operate_profit less than 1M:" + str(operate_profit) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if revenue < 1000000:
            garbageReason += "revenue less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

    #cashProfitM
        n_cashflow_act = df.iloc[i]['n_cashflow_act']
        net_profit = df.iloc[i]['net_profit']
        if net_profit != 0.0:
            cashProfitM = n_cashflow_act * 100 / net_profit
            df.iloc[i, df.columns.get_loc('cashProfitM')] = round(cashProfitM, 1)

        if n_cashflow_act < 1000000:
            garbageReason += "n_cashflow_act less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if net_profit < 1000000:
            garbageReason += "net_profit less than 1M:" + str(revenue) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        #bzsAstM
        bz_sales = df.iloc[i]['bz_sales']
        total_assets = df.iloc[i]['total_assets']
        if total_assets != 0.0:
            bzsAstM = bz_sales * 100 / total_assets
            df.iloc[i, df.columns.get_loc('bzsAstM')] = round(bzsAstM, 1)

        if bz_sales < 1000000:
            garbageReason += "bz_sales less than 1M:" + str(bz_sales) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        if total_assets < 1000000:
            garbageReason += "total_assets less than 1M:" + str(total_assets) + ". "
            garbageCnt += 1
            df.iloc[i, df.columns.get_loc('stopProcess')] = 1

        #cashLiabM
        c_cash_equ_end_period = df.iloc[i]['c_cash_equ_end_period']
        total_liab = df.iloc[i]['total_liab']
        if total_liab != 0.0:
            cashLiabM = finlib.Finlib().measureValue(c_cash_equ_end_period, total_liab)
            df.iloc[i, df.columns.get_loc('cashLiabM')] = round(cashLiabM, 1)

            if cashLiabM < 1:
                garbageReason += "cashLiabM less than 1:" + str(cashLiabM) + ". "
                garbageCnt += 1

        #cashInc
        n_incr_cash_cash_equ = df.iloc[i]['n_incr_cash_cash_equ']
        c_pay_dist_dpcp_int_exp = df.iloc[i]['c_pay_dist_dpcp_int_exp']
        cashInc = n_incr_cash_cash_equ + c_pay_dist_dpcp_int_exp
        df.iloc[i, df.columns.get_loc('cashInc')] = round(cashInc, 0)

        if cashInc < 0:
            garbageReason += "cashInc less than 0:" + str(cashInc) + ". "
            garbageCnt += 1

    #saleIncAbn
        c_fr_sale_sg = df.iloc[i]['c_fr_sale_sg']
        revenue = df.iloc[i]['revenue']

        saleIncAbn = c_fr_sale_sg - revenue
        df.iloc[i, df.columns.get_loc('saleIncAbn')] = round(saleIncAbn, 0)

        if saleIncAbn < 0:
            garbageReason += "saleIncAbn less than 0:" + str(saleIncAbn) + ". "
            garbageCnt += 1

    #hen/cow
        n_cashflow_act = df.iloc[i]['n_cashflow_act']
        n_cashflow_inv_act = df.iloc[i]['n_cashflow_inv_act']
        n_cash_flows_fnc_act = df.iloc[i]['n_cash_flows_fnc_act']

        val = abs(n_cashflow_act) + abs(n_cashflow_inv_act) + abs(n_cash_flows_fnc_act)
        val = round(val, 0)

        if n_cashflow_act > 0 and n_cashflow_inv_act >= 0 and n_cash_flows_fnc_act <= 0:
            df.iloc[i, df.columns.get_loc('hen')] = val
        elif n_cashflow_act > 0 and n_cashflow_inv_act <= 0 and n_cash_flows_fnc_act <= 0:
            df.iloc[i, df.columns.get_loc('cow')] = val

        if n_cashflow_act < 0:
            garbageReason += "n_cashflow_act less than 0:" + str(n_cashflow_act) + ". "
            garbageCnt += 1

        #lightAssert
        total_profit = df.iloc[i]['total_profit']
        fix_assets = df.iloc[i]['fix_assets']
        cip = df.iloc[i]['cip']
        const_materials = df.iloc[i]['const_materials']
        intan_assets = df.iloc[i]['intan_assets']

        sum_assert = fix_assets + cip + const_materials + intan_assets
        df.iloc[i, df.columns.get_loc('sum_assert')] = round(sum_assert, 1)

        if sum_assert != 0.0:
            lightAssert = total_profit * 100 / sum_assert
            df.iloc[i, df.columns.get_loc('lightAssert')] = round(lightAssert, 1)

            if lightAssert < 12:  #6%*2, none risky return
                garbageReason += "lightAssert less than 12:" + str(lightAssert) + ". "
                garbageCnt += 1

        #sumRcv, sumRcvNet, srnAsstM
        accounts_receiv = df.iloc[i]['accounts_receiv']
        oth_receiv = df.iloc[i]['oth_receiv']
        div_receiv = df.iloc[i]['div_receiv']
        int_receiv = df.iloc[i]['int_receiv']
        premium_receiv = df.iloc[i]['premium_receiv']
        reinsur_receiv = df.iloc[i]['reinsur_receiv']
        reinsur_res_receiv = df.iloc[i]['reinsur_res_receiv']
        lt_rec = df.iloc[i]['lt_rec']
        rr_reins_une_prem = df.iloc[i]['rr_reins_une_prem']
        rr_reins_outstd_cla = df.iloc[i]['rr_reins_outstd_cla']
        rr_reins_lins_liab = df.iloc[i]['rr_reins_lins_liab']
        rr_reins_lthins_liab = df.iloc[i]['rr_reins_lthins_liab']
        invest_as_receiv = df.iloc[i]['invest_as_receiv']
        acc_receivable = df.iloc[i]['acc_receivable']

        notes_receiv = df.iloc[i]['notes_receiv']

        total_assets = df.iloc[i]['total_assets']

        sumRcv = accounts_receiv + oth_receiv + div_receiv + int_receiv + premium_receiv + \
                 reinsur_receiv + reinsur_res_receiv + lt_rec + rr_reins_une_prem + rr_reins_outstd_cla + \
                 rr_reins_lins_liab + rr_reins_lthins_liab+invest_as_receiv+acc_receivable

        sumRcvNet = sumRcv - notes_receiv

        df.iloc[i, df.columns.get_loc('sumRcv')] = round(sumRcv, 1)
        df.iloc[i, df.columns.get_loc('sumRcvNet')] = round(sumRcvNet, 1)

        if total_assets != 0.0:
            srnAsstM = sumRcvNet * 100 / total_assets
            df.iloc[i, df.columns.get_loc('srnAsstM')] = round(srnAsstM, 1)

            if srnAsstM > 30:  #（sum-应收票据）/资产总计
                garbageReason += "srnAsstM great than 30:" + str(srnAsstM) + ". "
                garbageCnt += 1
                df.iloc[i, df.columns.get_loc('stopProcess')] = 1

    #revAbnM
        c_fr_sale_sg = df.iloc[i]['c_fr_sale_sg']
        revenue = df.iloc[i]['revenue']
        if revenue != 0.0:
            rst = (c_fr_sale_sg + sumRcv) / revenue
            revAbnM = (1.17 - rst) * 100.0 / 1.17  #靠近0标准，-10~10正常，（偏幅10%）数字为正且大好，说明交税少

            if revAbnM < -90 or revAbnM > 20:  #abnormal
                df.iloc[i, df.columns.get_loc('boolrevAbn')] = 1
                df.iloc[i, df.columns.get_loc('revAbnM')] = -10
                garbageReason += "revAbnM <-90 or >20:" + str(revAbnM) + ". "
                garbageCnt += 1

            else:
                df.iloc[i, df.columns.get_loc('boolrevAbn')] = 0
                df.iloc[i, df.columns.get_loc('revAbnM')] = round(revAbnM, 1)

        df.iloc[i, df.columns.get_loc('garbageCnt')] = garbageCnt
        df.iloc[i, df.columns.get_loc('garbageReason')] = garbageReason
        df.iloc[i, df.columns.get_loc('bonusCnt')] = bonusCnt
        df.iloc[i, df.columns.get_loc('bonusReason')] = bonusReason
        #df.to_csv(csv_output, encoding='UTF-8', index=False) #save csv every line
        #logging.info(csv_output)

    if not df.empty:
        df.to_csv(csv_output, encoding='UTF-8', index=False)  #only save when all complete. or save nothing. so we have intacted result.
        logging.info(__file__ + ": " + "analysze step 1 result saved to " + csv_output + " . len " + str(df.__len__()))


def _analyze_xiaoxiong_ct(ts_code, end_date, basic_df):
    logging.info("=== analyze _analyze_xiaoxiong_ct ===")

    # changtou xueyuan, xiaoxiong di li
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0

    dict_rtn = {}
    dict_rtn['garbageCnt'] = garbageCnt
    dict_rtn['bonusCnt'] = bonusCnt
    dict_rtn['bonusReason'] = bonusReason
    dict_rtn['garbageReason'] = garbageReason

    #if re.match('\d{4}0630$', end_date) or re.match('\d{4}1231$', end_date):
    #if re.match('20180630', end_date) or re.match('20171231', end_date): #ryan debug
    #if True:
    #    pass
    #else:
    #    logging.info("_analyze_xiaoxiong_ct: not handle Q1, Q3 report, " + end_date)
    #    return

    date_match = re.match('(\d{4})(\d{2})(\d{2})$', end_date)

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info("stock has been not on market. " + ts_code + " , " + end_date)
        return (dict_rtn)

    if not (date_match):
        logging.info("Error, date format unknown " + end_date)
        return (dict_rtn)

    year = int(date_match.group(1))
    month = int(date_match.group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)
    ann_date_this = tmp['ann_date']
    ann_date_4q_before = tmp['ann_date_4q_before']
    ann_date_8q_before = tmp['ann_date_8q_before']

    this_revenue = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='revenue', big_memory=big_memory_global)
    this_revenue_4q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field='revenue', big_memory=big_memory_global)
    this_revenue_8q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field='revenue', big_memory=big_memory_global)

    this_accounts_receiv = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='accounts_receiv', big_memory=big_memory_global)
    this_accounts_receiv_4q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field='accounts_receiv', big_memory=big_memory_global)
    this_accounts_receiv_8q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field='accounts_receiv', big_memory=big_memory_global)

    try:
        rule_1_year_1 = (this_revenue - this_revenue_4q_before) - (this_accounts_receiv - this_accounts_receiv_4q_before)
        rule_1_year_2 = (this_revenue_4q_before - this_revenue_8q_before) - (this_accounts_receiv_4q_before - this_accounts_receiv_8q_before)
        if rule_1_year_1 < 0 and rule_1_year_2 < 0:  #bigger is better
            #logging.info("garbage")
            #连续两年应收账款上升幅度超过营业收入上升幅度，没赚钱，收到白条。
            garbageReason += "Accounts receivable increased more than business income for two consecutive years. "
            garbageCnt += 1
        else:
            pass
            #logging.info("pass rule 1")
    except:
        pass

    this_inventories = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='inventories', big_memory=big_memory_global)
    this_inventories_4q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_4q_before, field='inventories', big_memory=big_memory_global)
    this_inventories_8q_before = get_ts_field(ts_code=ts_code, ann_date=ann_date_8q_before, field='inventories', big_memory=big_memory_global)

    try:
        rule_2_year_1 = (this_revenue - this_revenue_4q_before) - (this_inventories - this_inventories_4q_before)
        rule_2_year_2 = (this_revenue_4q_before - this_revenue_8q_before) - (this_inventories_4q_before - this_inventories_8q_before)
        if rule_2_year_1 < 0 and rule_2_year_2 < 0:  #bigger is better
            #连续两年存货上升幅度超过营业收入上升幅度，产品滞销
            garbageReason += "Increase of inventory  more than increase of business income for two consecutive years. "
            garbageCnt += 1
        else:
            pass
            #logging.info("pass rule 2")
    except:
        pass

    this_total_cur_liab = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='total_cur_liab', big_memory=big_memory_global)
    this_total_cur_assets = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='total_cur_assets', big_memory=big_memory_global)

    try:
        rule_3_year_1 = (this_total_cur_assets * 1.0 / (this_total_cur_liab + 1))
        if this_total_cur_liab > 0 and rule_3_year_1 < 1:  #bigger is better
            #流动负债远大于流动资产
            garbageReason += "Current liabilities far outweigh current assets. "
            garbageCnt += 1
        else:
            pass
            #logging.info("pass rule 3")
    except:
        pass

    this_c_inf_fr_operate_a = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='c_inf_fr_operate_a', big_memory=big_memory_global)  #经营活动现金流入小计
    this_st_cash_out_act = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='st_cash_out_act', big_memory=big_memory_global)  #经营活动现金流出小计
    this_net_profit = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='net_profit', big_memory=big_memory_global)  #净利润 (元，下同)

    try:
        rule_4_year_1 = (this_c_inf_fr_operate_a - this_st_cash_out_act) - this_net_profit
        if rule_4_year_1 > 0:  #bigger is better
            #经营活动现金流量净值大于净利润
            bonusReason += 'Net cash flow of operating activities > net profit. '
            bonusCnt += 1
            #logging.info("bonus, rule 4")
    except:
        pass

    this_free_cashflow = get_ts_field(ts_code=ts_code, ann_date=ann_date_this, field='free_cashflow', big_memory=big_memory_global)
    try:
        if this_free_cashflow > 0:  #bigger is better
            bonusReason += 'Free cashflow > 0. '
            bonusCnt += 1
            #logging.info("bonus, rule 5")
    except:
        pass

    dict_rtn['garbageCnt'] = garbageCnt
    dict_rtn['bonusCnt'] = bonusCnt
    dict_rtn['bonusReason'] = bonusReason
    dict_rtn['garbageReason'] = garbageReason

    return (dict_rtn)


def _analyze_white_horse_ct(ts_code, end_date, basic_df):
    logging.info("=== analyze _analyze_white_horse_ct ===")
    # changtou bai ma gu
    garbageReason = ""
    bonusReason = ""
    bonusCnt = 0
    garbageCnt = 0

    dict_rtn = {}
    dict_rtn['garbageCnt'] = garbageCnt
    dict_rtn['bonusCnt'] = bonusCnt
    dict_rtn['bonusReason'] = bonusReason
    dict_rtn['garbageReason'] = garbageReason

    if not finlib.Finlib().is_on_market(ts_code, end_date, basic_df):
        logging.info("stock has been not on market. " + ts_code + " , " + end_date)
        return (dict_rtn)

    if re.match('\d{4}0630$', end_date) or re.match('\d{4}1231$', end_date) or re.match('201[8|7|6]', end_date):
        #if re.match('20180630', end_date) or re.match('20171231', end_date):
        pass
    else:
        return (dict_rtn)

    year = int(re.match('(\d{4})(\d{2})(\d{2})$', end_date).group(1))
    month = int(re.match('(\d{4})(\d{2})(\d{2})$', end_date).group(2))

    tmp = finlib.Finlib().get_year_month_quarter(year=year, month=month)

    #### White Horse Stock of  Chang tou xue yuan
    this_roe = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date'], field='roe', big_memory=big_memory_global)
    #this_fund = finlib.Finlib().get_jaqs_field(ts_code=ts_code)
    this_fund = get_jaqs_field(ts_code=ts_code, date=end_date, big_memory=big_memory_global)
    this_pb = this_fund['pb']

    roe_1y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_1y_before'], field='roe', big_memory=big_memory_global)
    roe_2y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_2y_before'], field='roe', big_memory=big_memory_global)
    roe_3y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_3y_before'], field='roe', big_memory=big_memory_global)
    roe_4y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_4y_before'], field='roe', big_memory=big_memory_global)
    roe_5y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_5y_before'], field='roe', big_memory=big_memory_global)
    roe_6y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_6y_before'], field='roe', big_memory=big_memory_global)
    roe_7y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_7y_before'], field='roe', big_memory=big_memory_global)
    roe_8y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_8y_before'], field='roe', big_memory=big_memory_global)
    roe_9y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_9y_before'], field='roe', big_memory=big_memory_global)
    roe_10y = get_ts_field(ts_code=ts_code, ann_date=tmp['ann_date_10y_before'], field='roe', big_memory=big_memory_global)

    roeC = 15
    try:
        if (this_roe >= roeC and roe_1y >= roeC and roe_1y >= roeC and roe_2y >= roeC and roe_3y >= roeC and roe_4y >= roeC and roe_5y >= roeC and roe_6y >= roeC):
            bonusReason += 'ROE > ' + str(roeC) + ' consecutively (7years). '
            bonusCnt += 1
            logging.info("bonus. " + bonusReason)

            if this_pb > 0 and this_pb < 8:
                bonusReason += 'white horse '
                bonusCnt += 1
                logging.info("bonus. " + bonusReason + ' ' + ts_code + " " + end_date)

    except:
        pass

    dict_rtn['garbageCnt'] = garbageCnt
    dict_rtn['bonusCnt'] = bonusCnt
    dict_rtn['bonusReason'] = bonusReason
    dict_rtn['garbageReason'] = garbageReason

    return (dict_rtn)


def _analyze_step_2(end_date):
    #end_date in format 20171231

    #add columns to the sheet
    logging.info("=== analyze step 2 ===")

    csv_input = fund_base_report + "/step1/rpt_" + end_date + ".csv"
    output_dir = fund_base_report + "/step2"
    csv_output = output_dir + "/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if not os.path.isfile(csv_input):
        logging.info("input file not found " + csv_input)
        return

    if os.stat(csv_input).st_size < 10:
        logging.info('empty input file ' + csv_input)
        return

    df = pd.read_csv(csv_input, converters={'end_date': str})
    df = df[df['stopProcess'] != 1].reset_index().drop('index', axis=1)

    df = pd.DataFrame([0] * df.__len__(), columns=['scoreTotRev']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreGPM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreFiExpGr']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreSaExpGr']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreAdExpGr']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreTrYoy']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreSeExp']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreOpP']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreRevenue']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreOptPrftM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreNPN']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreNCA']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreNP']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCPM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreLA']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreSumAss']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreTotP']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreSumRcv']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreSumRcvNet']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoresSrnAsstM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreBzSa']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreBzAsM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCfrSSg']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreHen']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCow']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCI']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCLM']).join(df)

    df = pd.DataFrame([0] * df.__len__(), columns=['scoreRevCogM']).join(df)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreCurAssliaM']).join(df)

    df = pd.DataFrame([0] * df.__len__(), columns=['score']).join(df)  # the sum of all the score

    len = df.__len__()
    cols = df.columns.tolist()

    for i in range(len):
        sys.stdout.write("analyze step_2 " + str(i) + " of " + str(len) + ". ")
        sys.stdout.flush()

        scoreTotRev = round(stats.percentileofscore(df['total_revenue'], df.iloc[i]['total_revenue']), 2)
        df.iloc[i, df.columns.get_loc('scoreTotRev')] = scoreTotRev

        scoreRevCogM = round(stats.percentileofscore(df['revCogM'], df.iloc[i]['revCogM']), 2)
        df.iloc[i, df.columns.get_loc('scoreRevCogM')] = scoreRevCogM

        scoreCurAssliaM = round(stats.percentileofscore(df['curAssliaM'], df.iloc[i]['curAssliaM']), 2)
        df.iloc[i, df.columns.get_loc('scoreCurAssliaM')] = scoreCurAssliaM

        scoreGPM = round(stats.percentileofscore(df['grossprofit_margin'], df.iloc[i]['grossprofit_margin']), 2)
        df.iloc[i, df.columns.get_loc('scoreGPM')] = scoreGPM

        scoreFiExpGr = round(stats.percentileofscore(df['finExpToGr'], df.iloc[i]['finExpToGr']), 2)
        df.iloc[i, df.columns.get_loc('scoreFiExpGr')] = 100 - scoreFiExpGr

        scoreSaExpGr = round(stats.percentileofscore(df['saleexp_to_gr'], df.iloc[i]['saleexp_to_gr']), 2)
        df.iloc[i, df.columns.get_loc('scoreSaExpGr')] = 100 - scoreSaExpGr

        scoreAdExpGr = round(stats.percentileofscore(df['adminexp_of_gr'], df.iloc[i]['adminexp_of_gr']), 2)
        df.iloc[i, df.columns.get_loc('scoreAdExpGr')] = 100 - scoreAdExpGr

        scoreTrYoy = round(stats.percentileofscore(df['tr_yoy'], df.iloc[i]['tr_yoy']), 2)
        df.iloc[i, df.columns.get_loc('scoreTrYoy')] = scoreTrYoy

        scoreOpP = round(stats.percentileofscore(df['operate_profit'], df.iloc[i]['operate_profit']), 2)
        df.iloc[i, df.columns.get_loc('scoreOpP')] = scoreOpP

        scoreRevenue = round(stats.percentileofscore(df['revenue'], df.iloc[i]['revenue']), 2)
        df.iloc[i, df.columns.get_loc('scoreRevenue')] = scoreRevenue

        scoreOptPrftM = round(stats.percentileofscore(df['optPrftM'], df.iloc[i]['optPrftM']), 2)
        df.iloc[i, df.columns.get_loc('scoreOptPrftM')] = scoreOptPrftM

        scoreNPN = round(stats.percentileofscore(df['n_income_attr_p'], df.iloc[i]['n_income_attr_p']), 2)
        df.iloc[i, df.columns.get_loc('scoreNPN')] = scoreNPN

        scoreNCA = round(stats.percentileofscore(df['n_cashflow_act'], df.iloc[i]['n_cashflow_act']), 2)
        df.iloc[i, df.columns.get_loc('scoreNCA')] = scoreNCA

        scoreNP = round(stats.percentileofscore(df['net_profit'], df.iloc[i]['net_profit']), 2)
        df.iloc[i, df.columns.get_loc('scoreNP')] = scoreNP

        scoreCPM = round(stats.percentileofscore(df['cashProfitM'], df.iloc[i]['cashProfitM']), 2)
        df.iloc[i, df.columns.get_loc('scoreCPM')] = scoreCPM

        scoreLA = round(stats.percentileofscore(df['lightAssert'], df.iloc[i]['lightAssert']), 2)
        df.iloc[i, df.columns.get_loc('scoreLA')] = scoreLA

        scoreSumAss = round(stats.percentileofscore(df['sum_assert'], df.iloc[i]['sum_assert']), 2)
        df.iloc[i, df.columns.get_loc('scoreSumAss')] = scoreSumAss

        scoreTotP = round(stats.percentileofscore(df['total_profit'], df.iloc[i]['total_profit']), 2)
        df.iloc[i, df.columns.get_loc('scoreTotP')] = scoreTotP

        scoreSumRcv = round(stats.percentileofscore(df['sumRcv'], df.iloc[i]['sumRcv']), 2)
        df.iloc[i, df.columns.get_loc('scoreSumRcv')] = 100 - scoreSumRcv

        scoreSumRcvNet = round(stats.percentileofscore(df['sumRcvNet'], df.iloc[i]['sumRcvNet']), 2)
        df.iloc[i, df.columns.get_loc('scoreSumRcvNet')] = 100 - scoreSumRcvNet

        scoresSrnAsstM = round(stats.percentileofscore(df['srnAsstM'], df.iloc[i]['srnAsstM']), 2)
        df.iloc[i, df.columns.get_loc('scoresSrnAsstM')] = 100 - scoresSrnAsstM

        scoreBzSa = round(stats.percentileofscore(df['bz_sales'], df.iloc[i]['bz_sales']), 2)
        df.iloc[i, df.columns.get_loc('scoreBzSa')] = scoreBzSa

        scoreBzAsM = round(stats.percentileofscore(df['bzsAstM'], df.iloc[i]['bzsAstM']), 2)
        df.iloc[i, df.columns.get_loc('scoreBzAsM')] = scoreBzAsM

        scoreCfrSSg = round(stats.percentileofscore(df['c_fr_sale_sg'], df.iloc[i]['c_fr_sale_sg']), 2)
        df.iloc[i, df.columns.get_loc('scoreCfrSSg')] = scoreCfrSSg

        scoreHen = round(stats.percentileofscore(df['hen'], df.iloc[i]['hen']), 2)
        df.iloc[i, df.columns.get_loc('scoreHen')] = scoreHen

        scoreCow = round(stats.percentileofscore(df['cow'], df.iloc[i]['cow']), 2)
        df.iloc[i, df.columns.get_loc('scoreCow')] = scoreCow

        scoreCI = round(stats.percentileofscore(df['cashInc'], df.iloc[i]['cashInc']), 2)
        df.iloc[i, df.columns.get_loc('scoreCI')] = scoreCI

        scoreCLM = round(stats.percentileofscore(df['cashLiabM'], df.iloc[i]['cashLiabM']), 2)
        df.iloc[i, df.columns.get_loc('scoreCLM')] = scoreCLM

        final_score = scoreTotRev * 1.5 + scoreGPM *2 + scoreFiExpGr + scoreSaExpGr + \
                      scoreAdExpGr + scoreTrYoy + scoreOpP + scoreRevenue + \
                      scoreOptPrftM + scoreNPN + scoreNCA + scoreNP + \
                      scoreCPM * 2  + scoreLA + scoreSumAss + scoreTotP + \
                      scoreSumRcv + scoreSumRcvNet + scoresSrnAsstM + \
                      scoreBzSa*2 + scoreBzAsM *2 + scoreCfrSSg*2 + \
                      scoreHen*1.5 + scoreCow *2+ \
                      scoreCI + scoreCLM + \
                      scoreRevCogM *2  + scoreCurAssliaM * 2

        if df.iloc[i]['stopProcess'] == 1:
            final_score = -1

        df.iloc[i, df.columns.get_loc('score')] = final_score

    col_name_list_step2 = ['ts_code', 'name', 'score', 'garbageReason']
    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list_step2))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info("warning, no column named " + i + " in cols")

    df = df[cols]

    df = df.sort_values('score', ascending=False, inplace=False)
    df = df.reset_index().drop('index', axis=1)

    if not df.empty:
        df.to_csv(csv_output, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "analysze step 2 result saved to " + csv_output + " . len " + str(df.__len__()))


def _analyze_step_3(end_date):
    #end_date in format 20171231

    #add columns to the sheet
    logging.info("=== analyze step 3 ===")
    csv_input = fund_base_report + "/step2/rpt_" + end_date + ".csv"

    output_dir = fund_base_report + "/step3"
    csv_output = output_dir + "/rpt_" + end_date + ".csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=6):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if not os.path.isfile(csv_input):
        logging.info("input file not found " + csv_input)
        return

    df = pd.read_csv(csv_input, converters={'end_date': str})

    df = pd.DataFrame([0] * df.__len__(), columns=['sos']).join(df)  # score of score

    len = df.__len__()
    cols = df.columns.tolist()

    for i in range(len):
        sys.stdout.write("analyze step_3 " + str(i) + " of " + str(len) + ". ")
        sys.stdout.flush()

        sos = round(stats.percentileofscore(df['score'], df.iloc[i]['score']), 2)
        df.iloc[i, df.columns.get_loc('sos')] = sos

    col_name_list_step3 = ['ts_code', 'name', 'sos', 'score', 'garbageReason']
    cols = df.columns.tolist()
    name_list = list(reversed(col_name_list_step3))
    for i in name_list:
        if i in cols:
            cols.remove(i)
            cols.insert(0, i)
        else:
            logging.info("warning, no column named " + i + " in cols")

    df = df[cols]

    df = df.sort_values('score', ascending=False, inplace=False)
    df = df.reset_index().drop('index', axis=1)

    if not df.empty:
        df.to_csv(csv_output, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "analysze step 3 result saved to " + csv_output)

        sl_3 = "/home/ryan/DATA/result/latest_fundamental_year_pro.csv"
        if os.path.exists(sl_3):
            os.unlink(sl_3)
        os.symlink(csv_output, sl_3)
        logging.info("make symbol link " + sl_3 + " --> " + csv_output)


def _analyze_step_4():
    #end_date in format 20171231

    output_dir = fund_base_report + "/step4"
    csv_output = output_dir + "/multiple_years_score.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logging.info("=== analyze step 4 ===")

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    df = df_result = pd.DataFrame()

    x = os.listdir(fund_base_report + "/step3/")
    periord_list = []

    for f in x:
        csv_input = fund_base_report + "/step3/" + f  # f: rpt_200712313.csv

        #sys.stdout.write(csv_input)
        periord = re.match("rpt_(\d{6}).*.csv", f).group(1)  #periord: 200712
        periord_list.append(periord)

        df_1 = pd.read_csv(csv_input, converters={'end_date': str})
        df_1 = df_1[['ts_code', 'name', 'sos', 'end_date']]

        df = df.append(df_1)
        #logging.info(". len "+str(df.__len__()))

    uniq_ts_code = df['ts_code'].unique()

    periord_list.sort()
    for periord in periord_list:
        df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=[periord]).join(df_result)

    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=['number_in_top_30']).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=['score_avg']).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=['score_over_years']).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=['name']).join(df_result)
    df_result = pd.DataFrame([0] * uniq_ts_code.__len__(), columns=['ts_code']).join(df_result)

    i = 0
    for ts_code in uniq_ts_code:
        logging.info("=== " + ts_code + " " + str(i) + " of " + str(df_result.__len__()) + " ===")

        stock_df = df[df['ts_code'] == ts_code].sort_values(by='end_date', ascending=False)
        name = stock_df.iloc[0]['name']
        top_30_df = stock_df[stock_df['sos'] > 70].reset_index().drop('index', axis=1)
        top_30_df_num = top_30_df.__len__()

        df_result.loc[i] = pd.Series({'ts_code': ts_code, 'name': name, 'number_in_top_30': top_30_df_num})

        avg_n = avg_sum = 0

        score_over_years = 0

        #aaa = stock_df['end_date'].unique()
        #bbb = list(aaa).reverse()
        base = 0.95

        col_n = 0

        for d in stock_df['end_date'].unique():
            if (str(d) == '0') or (str(d) == '0.0') or (pd.isnull(d)):
                continue

            #sys.stdout.write(d + ". ")
            sd = re.match('(\d{6})\d\d', d).group(1)
            #sys.stdout.write(sd+". ")

            score_of_date = stock_df[stock_df['end_date'] == d].iloc[0]['sos']

            if np.isnan(score_of_date):
                #score_over_years = 0
                continue

            df_result.iloc[i, df_result.columns.get_loc(sd)] = score_of_date

            avg_n += 1
            avg_sum += score_of_date
            factor = base**col_n

            #logging.info("\tfactor "+str(factor)+". period "+str(sd))

            score_over_years += factor * score_of_date  #the latest * 1, next *0.7, next 0.7^2, next 0.7^3
            col_n += 1

        avg = 0
        if avg_n != 0:
            avg = round(avg_sum / avg_n, 2)

        df_result.iloc[i, df_result.columns.get_loc('score_avg')] = avg
        df_result.iloc[i, df_result.columns.get_loc('score_over_years')] = round(score_over_years)
        i += 1
        pass  #end of end_date for loop

    pass  #end of ts_code for loop

    if not df_result.empty:
        df_result = df_result.sort_values('score_over_years', ascending=False, inplace=False)
        df_result = df_result.reset_index().drop('index', axis=1)
        df_result.to_csv(csv_output, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "analysze step 4 result saved to " + csv_output + " , len " + str(df_result.__len__()))


def _analyze_step_5():

    csv_input = fund_base_report + "/step4/multiple_years_score.csv"

    output_dir = fund_base_report + "/step5"
    csv_output = output_dir + "/multiple_years_score.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    df = pd.DataFrame()

    logging.info("=== analyze step 5 ===")

    #logging.info("loading "+csv_input)
    df = pd.read_csv(csv_input)

    df = pd.DataFrame([0] * df.__len__(), columns=['scoreA']).join(df)  #score of score over years

    for i in range(df.__len__()):
        ts_code = df.iloc[i]['ts_code']
        sys.stdout.write("analyze_step_5 " + str(i) + " of " + str(df.__len__()) + ". ")
        sys.stdout.flush()

        #score_over_years
        score_soy = round(stats.percentileofscore(df['score_over_years'], df.iloc[i]['score_over_years']), 2)

        #score_avg
        score_sa = round(stats.percentileofscore(df['score_avg'], df.iloc[i]['score_avg']), 2)

        #number_in_top_30
        score_nit30 = round(stats.percentileofscore(df['number_in_top_30'], df.iloc[i]['number_in_top_30']), 2)

        df.iloc[i, df.columns.get_loc('scoreA')] = round((score_soy + score_sa + score_nit30) / 3.0, 2)

    if not df.empty:
        df = df.sort_values('scoreA', ascending=False, inplace=False)
        df = df.reset_index().drop('index', axis=1)
        df.to_csv(csv_output, encoding='UTF-8', index=False)
        logging.info(__file__ + ": " + "analysze step 5 result saved to " + csv_output + " , len " + str(df.__len__()))


def _analyze_step_6():
    csv_input_3 = fund_base_report + "/step5/multiple_years_score.csv"

    if debug_global:
        csv_input_1 = "/home/ryan/DATA/result/latest_fundamental_year_pro_debug.csv"  #symlink to /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_201806303.csv
    else:
        csv_input_1 = "/home/ryan/DATA/result/latest_fundamental_year_pro.csv"  #symlink to /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step3/rpt_20171231.csv

    csv_input_2 = "/home/ryan/DATA/result/latest_fundamental_quarter.csv"

    output_dir = fund_base_report + "/step6"
    csv_output = output_dir + "/multiple_years_score.csv"
    csv_selected_output = output_dir + "/multiple_years_score_selected.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logging.info("=== analyze step 6 ===")

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if (not force_run_global) and finlib.Finlib().is_cached(csv_selected_output, day=1) and (not overwrite):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_selected_output)
        return

    df = pd.DataFrame()

    #stock_code = "SZ000402"

    logging.info("loading " + csv_input_1)
    df_1 = pd.read_csv(csv_input_1)
    df_1 = finlib.Finlib().ts_code_to_code(df=df_1)  #code:SH600519
    #df_1 = df_1[df_1['code']==stock_code]

    logging.info("loading " + csv_input_2)
    df_2 = pd.read_csv(csv_input_2, converters={'code': str})
    df_2 = finlib.Finlib().add_market_to_code(df=df_2)  #code: SH600519

    if debug_global:
        df_2 = df_2[df_2['code'] == "SH600519"]

    logging.info("loading " + csv_input_3)
    df_3 = pd.read_csv(csv_input_3)
    df_3 = finlib.Finlib().ts_code_to_code(df=df_3)
    df_3 = df_3[['code', 'scoreA']]  #code: SH600519, scoreA:NaN

    df = pd.merge(df_1, df_2, how='outer', on=['code'], suffixes=('', '_stock_basics'))
    df = pd.merge(df, df_3, how='outer', on=['code'], suffixes=('', '_df_3'))
    df = df.fillna(0)

    #logging.info(df.head(10))

    #exit(0)

    if debug_global:
        df = df[df['code'] == "600519"]

    df = pd.DataFrame([0] * df.__len__(), columns=['ValuePrice']).join(df)  #the stock price should be
    df = pd.DataFrame([0] * df.__len__(), columns=['CurrentPrice']).join(df)  #the stock price  actually be
    df = pd.DataFrame([0] * df.__len__(), columns=['V_C_P']).join(df)  #how value is the price now. bigger is better
    df = pd.DataFrame([0] * df.__len__(), columns=['VCP_P']).join(df)  #percent of VCP
    df = pd.DataFrame([0] * df.__len__(), columns=['FCV']).join(df)  #企业自由现金流量估值
    df = pd.DataFrame([0] * df.__len__(), columns=['FCV_P']).join(df)  #企业自由现金流量估值
    df = pd.DataFrame([0] * df.__len__(), columns=['FCV_5V']).join(df)  #自由现金流量 5 years value
    df = pd.DataFrame([0] * df.__len__(), columns=['FCV_10V']).join(df)  #自由现金流量 10 years value
    #df = pd.DataFrame([0] * df.__len__(), columns=['FCV_20V']).join(df) #自由现金流量 20 years value
    df = pd.DataFrame([0] * df.__len__(), columns=['ROE_Cnt']).join(df)  #
    df = pd.DataFrame([0] * df.__len__(), columns=['ROE_Mean']).join(df)  #
    df = pd.DataFrame([200] * df.__len__(), columns=['FCV_NYears']).join(df)  #自由现金流量 after N years == current price
    df = pd.DataFrame([0] * df.__len__(), columns=['FCV_NYears_P']).join(df)  #percent of ( 0 ~100)
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreB']).join(df)  #scoreA + FCV_NYears_P + FCV_P + VCP_P
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreB_tmp']).join(df)  #
    df = pd.DataFrame([0] * df.__len__(), columns=['scoreAB']).join(df)  #

    for i in range(df.__len__()):
        code = df.iloc[i]['code']
        sys.stdout.write("analyze step_6 " + str(i) + " of " + str(df.__len__()) + ". ")
        sys.stdout.flush()

        guben = df.iloc[i]['totals'] * 10**8  #totals,总股本(亿)

        if guben == 0:
            logging.info("Fatal error, guben = 0, " + code)
            #exit(1)
            guben = 10**20  #a very huge/big number,so result is very small to ignored.

        total_cur_assets = int(df.iloc[i]['total_cur_assets'])  #流动资产合计
        total_nca = int(df.iloc[i]['total_nca'])  #非流动资产合计
        CurrentPrice = finlib.Finlib().get_price(code_m=code)
        CurrentPrice = round(CurrentPrice, 2)
        free_cashflow = int(df.iloc[i]['free_cashflow'])  #企业自由现金流量
        df.iloc[i, df.columns.get_loc('FCV')] = free_cashflow

        FCV_5V = FCV_10V = FCV_nV = 0
        Cash_Discount_Rate = 0.92

        if big_memory_global:
            regex_group = re.match("(\w{2})(\d{6})", code)  #sh600519 --> 600519.sh
            ts_code = regex_group.group(2) + "." + regex_group.group(1)

            df_the_code = df_all_ts_pro[df_all_ts_pro['ts_code'] == ts_code]
            df_the_code = df_the_code[df_the_code['roe'] != 0]
            roe_mean = df_the_code['roe'].mean()  #600519 --> roe_mean 21.25

            if not np.isnan(roe_mean):
                df.iloc[i, df.columns.get_loc('ROE_Mean')] = round(roe_mean, 2)
                df.iloc[i, df.columns.get_loc('ROE_Cnt')] = df_the_code.__len__()
                Profit_Increase_Rate = 1 + round(roe_mean / 100, 2)
            else:
                Profit_Increase_Rate = 1.10
        else:
            Profit_Increase_Rate = 1.10

        for t_cnt in range(5):
            FCV_5V += free_cashflow * (Profit_Increase_Rate**t_cnt) * (Cash_Discount_Rate**t_cnt)

        for t_cnt in range(10):
            FCV_10V += free_cashflow * (Profit_Increase_Rate**t_cnt) * (Cash_Discount_Rate**t_cnt)

        #for t_cnt in range(20):
        #    FCV_20V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        #for t_cnt in range(50):
        #    FCV_50V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        #for t_cnt in range(100):
        #    FCV_100V += free_cashflow*(Profit_Increase_Rate**t_cnt)*(Cash_Discount_Rate**t_cnt)

        for t_cnt in range(200):
            FCV_nV += free_cashflow * (Profit_Increase_Rate**t_cnt) * (Cash_Discount_Rate**t_cnt)
            if FCV_nV / guben > CurrentPrice:
                df.iloc[i, df.columns.get_loc('FCV_NYears')] = t_cnt

                break

        FCV_5V = FCV_5V / guben
        FCV_10V = FCV_10V / guben
        #FCV_20V = FCV_20V/guben
        #FCV_50V = FCV_50V/guben
        #FCV_100V = FCV_100V/guben

        df.iloc[i, df.columns.get_loc('FCV_5V')] = int(FCV_5V)
        df.iloc[i, df.columns.get_loc('FCV_10V')] = int(FCV_10V)

        total_cur_liab = df.iloc[i]['total_cur_liab']  #流动负债合计
        total_ncl = df.iloc[i]['total_ncl']  #非流动负债合计

        ValuePrice = (total_cur_assets + total_nca - total_cur_liab - total_ncl) / guben
        ValuePrice = round(ValuePrice, 2)
        df.iloc[i, df.columns.get_loc('ValuePrice')] = ValuePrice

        df.iloc[i, df.columns.get_loc('CurrentPrice')] = CurrentPrice

        if CurrentPrice == 0.0:
            V_C_P = 0
        else:
            V_C_P = round(ValuePrice / CurrentPrice, 2)

        df.iloc[i, df.columns.get_loc('V_C_P')] = V_C_P

    #loop_2 of step6, calculate VCP_P, FCV_P
    df = df.reset_index().drop('index', axis=1)

    df_cmp = df[df['FCV'] > 0]

    for i in range(df.__len__()):
        df.iloc[i, df.columns.get_loc('VCP_P')] = round(stats.percentileofscore(df['V_C_P'], df.iloc[i]['V_C_P']), 2)

        if df.iloc[i, df.columns.get_loc('FCV')] > 0:
            df.iloc[i, df.columns.get_loc('FCV_P')] = round(stats.percentileofscore(df_cmp['FCV'], df.iloc[i]['FCV']), 2)
            df.iloc[i, df.columns.get_loc('FCV_NYears_P')] = 100 - round(stats.percentileofscore(df_cmp['FCV_NYears'], df.iloc[i]['FCV_NYears']), 2)

        scoreB_tmp = 0
        scoreB_tmp += df.iloc[i, df.columns.get_loc('VCP_P')]
        scoreB_tmp += df.iloc[i, df.columns.get_loc('FCV_P')]
        scoreB_tmp += df.iloc[i, df.columns.get_loc('FCV_NYears_P')]
        df.iloc[i, df.columns.get_loc('scoreB_tmp')] = round(scoreB_tmp, 2)

#loop 3 of step 6,
    for i in range(df.__len__()):
        df.iloc[i, df.columns.get_loc('scoreB')] = round(stats.percentileofscore(df['scoreB_tmp'], df.iloc[i]['scoreB_tmp']), 2)
        df.iloc[i, df.columns.get_loc('scoreAB')] = round((df.iloc[i, df.columns.get_loc('scoreA')] + df.iloc[i, df.columns.get_loc('scoreB')]) / 2, 2)

    #df = df.sort_values('V_C_P', ascending=False, inplace=False)
    df = df.sort_values('scoreAB', ascending=False, inplace=False)
    #df = df.reset_index().drop('index', axis=1)
    #df = df[['code','name','scoreA','scoreB', 'VCP_P', 'FCV_P', 'FCV_NYears_P', 'FCV', 'FCV_NYears', 'V_C_P','ValuePrice','CurrentPrice', 'FCV_5V','FCV_10V','FCV_20V','FCV_50V','FCV_100V',]]
    df = df[['code', 'name', 'scoreAB', 'scoreA', 'scoreB', 'VCP_P', 'FCV_P', 'FCV_NYears_P', 'ROE_Mean', 'ROE_Cnt', 'FCV', 'FCV_NYears', 'V_C_P', 'ValuePrice', 'CurrentPrice', 'FCV_5V', 'FCV_10V']]

    df_selected = df
    df_selected = df_selected[df_selected['scoreAB'] > 90]
    df_selected = df_selected[df_selected['scoreA'] > 90]
    df_selected = df_selected[df_selected['scoreB'] > 50]
    df_selected = df_selected[df_selected['VCP_P'] > 50]
    df_selected = df_selected[df_selected['FCV_P'] > 50]
    df_selected = df_selected[df_selected['FCV_NYears_P'] > 50]

    df_selected = finlib.Finlib().remove_garbage(df_selected, code_filed_name='code', code_format='C2D6')

    df.to_csv(csv_output, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "analysze step 6 result saved to " + csv_output + " , len " + str(df.__len__()))

    df_selected.to_csv(csv_selected_output, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "analysze step 6 selected result saved to " + csv_selected_output + " , len " + str(df_selected.__len__()))


#def verify_fund_increase():
def _analyze_step_7():

    logging.info("=== analyze step 7 : verify_fund_increase ===")  #buy at month after the financial report,hold a quarter.

    output_dir = fund_base_report + "/step7"
    csv_output = output_dir + "/verify_fund_increase.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    csv_input = fund_base_report + "/step4/multiple_years_score.csv"
    df_input = pd.read_csv(csv_input, converters={'end_date': str})

    #debug:
    #df_input=df_input[df_input['ts_code'] == '600519.SH'].reset_index().drop('index', axis=1) #good stock
    #df_input=df_input[df_input['ts_code'] == '603709.SH'].reset_index().drop('index', axis=1) #worst stock
    #df_input=df_input[df_input['score_over_years']>=90].reset_index().drop('index', axis=1)
    #df_input=df_input.head(2)
    #df_input=df_input.tail(2).reset_index().drop('index', axis=1)

    df_result = df_input

    #make the columns of the result df
    col_list = []

    for c in df_input.columns:
        # fund score of the quarter
        if re.match('\d{6}', c):
            #logging.info(c)
            df_result = pd.DataFrame([-100] * df_result.__len__(), columns=["inc" + c]).join(df_result)
            ##col_list.append("score"+c)  #increase since the quarter to today
            col_list.append("inc" + c)  #increase since the quarter to today
            col_list.append(c)
        else:
            col_list.append(c)

    df_result = df_result[col_list]
    df_result = finlib.Finlib().ts_code_to_code(df=df_result)
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_inc_avg"]).join(df_result)
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_win_p"]).join(df_result)
    df_result = pd.DataFrame([0] * df_result.__len__(), columns=["ktr_cnt_win"]).join(df_result)

    len = df_result.__len__()

    for i in range(len):

        ktr_cnt_win = ktr_cnt_all = ktr_sum = 0.0

        logging.info("==== " + str(i) + " of " + str(len) + " ====")
        the_df = df_result.iloc[i]

        code = the_df['code']
        price_today = finlib.Finlib().get_price(code_m=code)

        col_list = list(df_input.columns)
        col_list.reverse()

        for c in col_list:
            mat = re.match('(\d{4})(\d{2})', c)
            if mat:
                logging.info(c)
                year = mat.group(1)
                month = mat.group(2)
                score = the_df[c]

                if str(year) < "2015":
                    logging.info("Not process year before 2015")
                    continue

                price_the_day = finlib.Finlib().get_price(code_m=code, date=year + "-" + month + "-" + "31")

                if np.isnan(score):
                    continue

                if score > 95:
                    # start of the buy and sell (ktr)
                    # compare price of +1month vs +4month
                    month_ktr_start = int(month) + 1  #ktr:know the report. suppose Q2 06.31 report published at 07.31
                    month_ktr_end = int(month) + 4  #next quarter publish data

                    year_ktr_start = year_ktr_end = int(year)
                    ktr_cnt_all += 1

                    if month_ktr_start > 12:
                        month_ktr_start = month_ktr_start % 12
                        year_ktr_start += 1

                    if month_ktr_end > 12:
                        month_ktr_end = month_ktr_end % 12
                        year_ktr_end += 1

                    if month_ktr_start < 10:
                        month_ktr_start = "0" + str(month_ktr_start)

                    if month_ktr_end < 10:
                        month_ktr_end = "0" + str(month_ktr_end)

                    price_ktr_start = finlib.Finlib().get_price(code_m=code, date=str(year_ktr_start) + "-" + str(month_ktr_start) + "-" + "31")
                    price_ktr_end = finlib.Finlib().get_price(code_m=code, date=str(year_ktr_end) + "-" + str(month_ktr_end) + "-" + "31")

                    if price_ktr_start != 0.0 and price_ktr_end != 0.0:
                        quarter_inc = (price_ktr_end - price_ktr_start) * 100 / price_ktr_start
                        quarter_inc = round(quarter_inc, 2)

                        ktr_sum += quarter_inc

                        if quarter_inc > 0:
                            ktr_cnt_win += 1

                        #logging.info("score "+str(score)+", "+str(quarter_inc)+": "+str(year_ktr_start)+str(month_ktr_start)+" -> "+str(year_ktr_end)+str(month_ktr_end))

                    #end of the buy and sell (ktr)

                if price_the_day == 0.0:
                    increase = None
                else:
                    increase = (price_today - price_the_day) * 100 / price_the_day
                    increase = round(increase, 2)

                logging.info(str(code) + " " + str(c) + ", score: " + str(score) + " inc: " + str(increase) + " from " + str(price_the_day) + " to " + str(price_today))
                df_result.iloc[i, df_result.columns.get_loc("inc" + c)] = increase
                pass  # end of the for loop of the column

        if ktr_cnt_all > 0.0:
            df_result.iloc[i, df_result.columns.get_loc("ktr_cnt_win")] = int(ktr_cnt_win)
            df_result.iloc[i, df_result.columns.get_loc("ktr_win_p")] = round(ktr_cnt_win * 100 / ktr_cnt_all, 2)
            df_result.iloc[i, df_result.columns.get_loc("ktr_inc_avg")] = round(ktr_sum / ktr_cnt_all)
        pass  #end of the for loop of the rows

    #df_result = df_result.sort_values('score_over_years', ascending=False, inplace=False)
    df_result = df_result.reset_index().drop('index', axis=1)
    df_result.to_csv(csv_output, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "verify_fund_increase result saved to " + csv_output + " , len " + str(df_result.__len__()))


def _analyze_step_8():
    #csv_input_1 = fund_base_report + "/step6/multiple_years_score_selected.csv" #scoreA>90, V_C_P >0.65
    csv_input_1 = fund_base_report + "/step6/multiple_years_score_selected.csv"
    csv_input_2 = fund_base_report + "/step7/verify_fund_increase.csv"

    output_dir = fund_base_report + "/step8"
    csv_output = output_dir + "/multiple_years_score.csv"
    csv_output_selected = output_dir + "/multiple_years_score_selected.csv"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output)
        return

    if (not force_run_global) and finlib.Finlib().is_cached(csv_output_selected, day=1):
        logging.info("file has been updated in 1 days, will not calculate. " + csv_output_selected)
        return

    df = pd.DataFrame()

    logging.info("=== analyze step 8 ===")

    #stock_code = "SZ000402"

    logging.info("loading " + csv_input_1)
    df_1 = pd.read_csv(csv_input_1)
    #df_1 = finlib.Finlib().ts_code_to_code(df=df_1)
    #logging.info("read line "+str(df_1.__len__()))

    logging.info("loading " + csv_input_2)
    df_2 = pd.read_csv(csv_input_2, converters={'code': str})
    #df_2 = finlib.Finlib().add_market_to_code(df=df_2)
    #logging.info("read line " + str(df_2.__len__()))

    df = pd.merge(df_1, df_2, how='inner', on=['code'], suffixes=('', '_stock_basics'))
    #   len = df.__len__()
    #logging.info("after merge len "+str(len))

    df.to_csv(csv_output, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "analysze step 8 result saved to " + csv_output + " . len " + str(df.__len__()))

    #scoreAB = 80  <<< This has been filted in step6/selected.csv
    #df = df[df['scoreAB']>scoreAB]
    #logging.info("scoreAB > "+str(scoreAB) +" " + str(df.__len__()))

    ktr_win_p = 50
    df = df[df['ktr_win_p'] >= ktr_win_p]
    logging.info("ktr_win_p >= " + str(ktr_win_p) + " " + str(df.__len__()))

    #ktr_inc_avg = 3
    #df = df[df['ktr_inc_avg']>ktr_inc_avg]
    #logging.info("ktr_inc_avg > "+str(ktr_inc_avg)+" " + str(df.__len__()))

    #ktr_cnt_win = 5
    #df = df[df['ktr_cnt_win']>ktr_cnt_win]
    #logging.info("ktr_cnt_win > "+str(ktr_cnt_win)+" " + str(df.__len__()))

    df.to_csv(csv_output_selected, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "analysze step 8 selected result saved to " + csv_output_selected + " , len " + str(df.__len__()))


def analyze(fully_a=False, daily_a=True, fast=True):
    #df = pd.read_csv(csv_income, converters={'end_date': str})

    #ed = list(df['end_date'].unique())

    report_status = finlib.Finlib().get_report_publish_status()
    period_list = report_status['period_to_be_checked_lst']  #period to be checked at this time point (based on month)

    time_map = finlib.Finlib().get_year_month_quarter()

    if fully_a:
        if fast:
            period_list = [finlib.Finlib().get_report_publish_status()['completed_year_rpt_date']]  #@2019.03.10, it is 20171231
        else:
            period_list = time_map['full_period_list_yearly']
    elif daily_a:
        #pass
        #print("Have not decide which period to check daily")
        period_list = time_map['fetch_most_recent_report_perid']  #@2019.03.10, it is 20181231
        #period_list = [finlib.Finlib().get_report_publish_status()['completed_quarter_date']] #@2019.03.10, it is 20180930
        #period_list = [finlib.Finlib().get_report_publish_status()['completed_year_rpt_date']] #@2019.03.10, it is 20171231

    period_list = list(set(period_list))
    #period_list.sort(reverse=False)
    period_list.sort(reverse=True)  #check from new to old. 20181231-->20001231

    #if debug_global:
    #    period_list=["20171231"]

    for e in period_list:
        logging.info("e is "+str(e))
        if e < '20151231':
            logging.info("not process date before 2015" +str(e))
            continue

        sys.stdout.write("end_date " + e + ". ")

        #continue

        # as many date lost on Q1, Q3 report, so only process half-year, and year report.
        # !!!! @todo ryan: parallary compare on yearly report; Q1, Q2, Q3 data can be used in self comparision.
        # !!!!
        if re.match('\d{4}1231$', e) or daily_a:  #daily_a check the most recent only(small scope), so daily_a check all steps.
            _analyze_step_1(end_date=e)  # field calculate
            _analyze_step_2(end_date=e)  # score
            _analyze_step_3(end_date=e)  # score of score

        else:
            logging.info("not handle Q1, Q2, Q3 report, " + e)
            continue

    _analyze_step_4()  #evaluate the stock score in mutliple years.
    _analyze_step_5()  #'scoreA'
    _analyze_step_6()  #under valued stock, valuePrice/actualPrice. scoreA,V_C_P,
    _analyze_step_7()
    _analyze_step_8()


'''
    if fully_a: #run quartly after quartly report released. month 4/31, 7/31, 10/31, 1/31
        for e in period_list:
            if e < '2010':
                continue

            sys.stdout.write("end_date " + e + ". ")

            #as many date lost on Q1, Q3 report, so only process half-year, and year report.

            #if True:
            #if re.match('\d{4}0630$', e) or re.match('\d{4}1231$', e) or re.match('201[8]', e) :
            if re.match('\d{4}1231$', e) or re.match('201[8]', e) :
            #if  re.match('201[8]', e) or re.match('201[7|6|5|4|3|2]1231', e):
            #if re.match('20181231', e) or re.match('20171231', e):
            #if re.match('\d{4}1231$', e):

                _analyze_step_1(end_date=e, overwrite=overwrite)  # field calculate
                _analyze_step_2(end_date=e, overwrite=overwrite)  # score
                _analyze_step_3(end_date=e, overwrite=overwrite)  # score of score
            else:
                logging.info("not handle Q1, Q3 report, "+e)
                continue


        _analyze_step_4(overwrite=overwrite) #evaluate the stock score in mutliple years.
        _analyze_step_5(overwrite=overwrite) #'scoreA'
        _analyze_step_6(overwrite=overwrite) #under valued stock, valuePrice/actualPrice. scoreA,V_C_P,
        _analyze_step_7(overwrite=overwrite)
        _analyze_step_8(overwrite=overwrite)

    if daily_a:
        _analyze_step_5(overwrite=overwrite)
        _analyze_step_6(overwrite=overwrite)
        _analyze_step_7(overwrite=overwrite)
        _analyze_step_8(overwrite=overwrite)
'''


def extract_white_horse():

    output_csv = fund_base_report + "/white_horse.csv"
    year_q = finlib.Finlib().get_year_month_quarter()

    stock_list = finlib.Finlib().get_A_stock_instrment()  #603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  #603999.SH
    stock_list.rename(columns={'code': 'ts_code'}, inplace=True)

    df_stock_list = stock_list

    for time_period in ['5y', '4y', '3y', '2y', '1y', '3q', '2q', '1q']:
        df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=[time_period]))

    df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=['cnt_sum_white_horse']))

    for time_period in ['5y', '4y', '3y', '2y', '1y', '3q', '2q', '1q']:
        ann_date = "ann_date_" + time_period + "_before"
        input_csv = fund_base_report + "/step1/rpt_" + year_q[ann_date] + ".csv"
        df_q = pd.DataFrame()

        if not os.path.exists(input_csv):
            continue
        else:
            logging.info("reading " + input_csv)
            df_q = pd.read_csv(input_csv, converters={'end_date': str})
            #df_q = df_q.fillna(0)
            df_q = df_q[df_q['bonusReason'].str.contains('white horse', na=False)]  #filter out the white horse

            for c in df_q['ts_code'].values:
                for v in df_stock_list[df_stock_list['ts_code'] == c].index.values:
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc(time_period)] += 1
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc('cnt_sum_white_horse')] += 1

    df_stock_list = df_stock_list[df_stock_list['cnt_sum_white_horse'] > 0]
    df_stock_list = df_stock_list.sort_values('cnt_sum_white_horse', ascending=False, inplace=False)
    df_stock_list = finlib.Finlib().ts_code_to_code(df_stock_list)

    df_stock_list = df_stock_list.reset_index().drop('index', axis=1)
    df_stock_list.to_csv(output_csv, encoding='UTF-8', index=False)

    logging.info(__file__ + ": " + "white horse csv saved to " + output_csv + " . len " + str(df_stock_list.__len__()))


def extract_high_freecashflow_price_ratio():
    csv_input = fund_base_report + "/step6/multiple_years_score_selected.csv"
    output_csv = fund_base_report + "/freecashflow_price_ratio.csv"

    if not os.path.exists(csv_input):
        logging.info("input csv doesn't exist, " + csv_input)
        return

    if not os.stat(csv_input).st_size >= 10:
        logging.info("input csv is empty. " + csv_input)
        return

    df = pd.read_csv(csv_input)  #no end_date column in the csv
    df.sort_values('FCV_NYears', ascending=True, inplace=False)
    df = df.reset_index().drop('index', axis=1)
    df.to_csv(output_csv, encoding='UTF-8', index=False)

    logging.info(__file__ + ": " + "freecashflow_price_rati csv saved to " + output_csv + " . len " + str(df.__len__()))


def extract_hen_cow():
    output_csv = fund_base_report + "/hen_cow.csv"
    year_q = finlib.Finlib().get_year_month_quarter()

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)  # 603999.SH
    stock_list.rename(columns={'code': 'ts_code'}, inplace=True)

    df_stock_list = stock_list

    for time_period in ['5y', '4y', '3y', '2y', '1y', '3q', '2q', '1q']:
        df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=[time_period]))

    df_stock_list = df_stock_list.join(pd.DataFrame([0] * df_stock_list.__len__(), columns=['cnt_sum_hen_cow']))

    for time_period in ['5y', '4y', '3y', '2y', '1y', '3q', '2q', '1q']:

        weight_map = {'5y': 1 / 8.0, '4y': 2 / 8.0, '3y': 3 / 8.0, '2y': 4 / 8.0, '1y': 5 / 8.0, '3q': 6 / 8.0, '2q': 7 / 8.0, '1q': 8 / 8.0}
        ann_date = "ann_date_" + time_period + "_before"
        input_csv = fund_base_report + "/step2/rpt_" + year_q[ann_date] + ".csv"
        df_q = pd.DataFrame()

        if not os.path.exists(input_csv):
            continue
        else:
            logging.info("reading " + input_csv)
            df_q = pd.read_csv(input_csv, converters={'end_date': str})
            df_q_hen = df_q[df_q['scoreHen'] > 80]  # filter out the white horse
            df_q_cow = df_q[df_q['scoreCow'] > 80]  # filter out the white horse
            df_q_hen_cow = df_q_cow.append(df_q_hen)
            #logging.info("df_q_hen len " + str(df_q_hen.__len__()))
            #logging.info("df_q_cow len " + str(df_q_cow.__len__()))
            #logging.info("hen_cow len "+str(df_q_hen_cow.__len__()))

            for c in df_q_hen_cow['ts_code'].values:
                for v in df_stock_list[df_stock_list['ts_code'] == c].index.values:
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc(time_period)] += 1
                    df_stock_list.iloc[v, df_stock_list.columns.get_loc('cnt_sum_hen_cow')] += 1 * weight_map[time_period]

    df_stock_list = df_stock_list[df_stock_list['cnt_sum_hen_cow'] > 0]
    df_stock_list = df_stock_list.sort_values('cnt_sum_hen_cow', ascending=False, inplace=False)
    df_stock_list = df_stock_list.head(100)
    df_stock_list = df_stock_list.reset_index().drop('index', axis=1)
    df_stock_list = finlib.Finlib().ts_code_to_code(df_stock_list)

    df_stock_list.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "hen cow csv saved to " + output_csv + " . len " + str(df_stock_list.__len__()))


#print recent express data
def express_notify():
    pass


#print recent express data
def disclosure_date_notify(days):

    input_csv = csv_disclosure_date_latest
    output_csv = csv_disclosure_date_latest_notify

    if not os.path.exists(input_csv):
        logging.info("file not exist, quit. " + input_csv)

    #df = pd.read_csv(input_csv, converters={'code':str,'name':str,'ann_date':str,'end_date':str, 'pre_date':str,'actual_date':str,'modify_date':str}, encoding="utf-8" )
    df = pd.read_csv(input_csv, converters={'code': str, 'ann_date': str, 'end_date': str, 'pre_date': str, 'actual_date': str, 'modify_date': str}, encoding="utf-8")

    import datetime

    todayS = datetime.datetime.today().strftime('%Y%m%d')

    endday = datetime.datetime.today() + datetime.timedelta(days)
    enddayS = endday.strftime('%Y%m%d')

    df_result = df
    df_result = df_result[df_result['pre_date'] >= todayS]
    df_result = df_result[df_result['pre_date'] <= enddayS]

    df_result = df_result.sort_values('pre_date', ascending=True, inplace=False)
    df_result = df_result.reset_index().drop('index', axis=1)

    df_result = finlib.Finlib().ts_code_to_code(df=df_result)
    df_result.to_csv(output_csv, encoding='UTF-8', index=False)

    logging.info(__file__ + ":  in " + str(days) + " days disclosure notify csv saved to " + output_csv + " . len " + str(df_result.__len__()))

    pass


def _fetch_pro_basic():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_basic.csv"

    if finlib.Finlib().is_cached(output_csv, 1):
        logging.info("not fetch basic as the file updated in 1 day. " + output_csv)
        return ()

    df = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    df.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info("pro basic saved to " + output_csv + " . len " + str(df.__len__()))
    return (df)


def get_pro_basic():
    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_basic.csv"
    return (pd.read_csv(output_csv))


def _fetch_pro_concept():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_concept.csv"

    if finlib.Finlib().is_cached(output_csv, 1) and (not force_run_global):
        logging.info("not fetch concept as the file updated in 1 day. " + output_csv)
        return ()

    #df_result = pd.DataFrame(columns=['cat_name', 'cat_code'])
    df_result = pd.DataFrame()

    df_c = pro.concept()
    cnt = df_c.__len__()
    i = 0
    for id in df_c['code']:
        i += 1

        #df_sub = pd.DataFrame()
        cat_name = df_c[df_c['code'] == id]['name'].iloc[0]
        logging.info("query concept details, " + str(i) + " of " + str(cnt) + ", id " + str(id) + " name " + cat_name)

        try:
            df_cd = pro.concept_detail(id=id, fields='ts_code,name')
            time.sleep(0.5)  #抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
            new_value_df = pd.DataFrame([id] * df_cd.__len__(), columns=['cat_code'])
            df_cd = df_cd.join(new_value_df)

            new_value_df = pd.DataFrame([cat_name] * df_cd.__len__(), columns=['cat_name'])
            df_cd = df_cd.join(new_value_df)

            df_result = pd.concat([df_result, df_cd], sort=False).reset_index().drop('index', axis=1)

        except:
            logging.info("exception in get_pro_concept")
        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(str(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                #logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                logging.info(sys.exc_value.message)  # print the human readable unincode
                logging.info("cat_id: " + id + " cat_name: " + cat_name)
                sys.exc_clear()

    df_result.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info("pro concept saved to " + output_csv + " . len " + str(df_result.__len__()))
    return (df_result)


def _fetch_cctv_news():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/cctv_news"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/cctv_news.csv"

    if finlib.Finlib().is_cached(output_csv, 1) and (not force_run_global):
        logging.info("not fetch cctv news as the file updated in 1 day")
        return ()

    df_result = pd.DataFrame(columns=['date', 'title', 'content'])

    if os.path.exists(output_csv):
        logging.info("loading " + output_csv)
        df_result = pd.read_csv(output_csv, converters={'date': str})

    #df_result = pd.DataFrame(columns=['cat_name', 'cat_code'])

    #数据开始于2006年6月，超过12年历史
    date = datetime.date(2006, 6, 15)
    today = datetime.date.today()

    while (date <= today):
        date_S = date.strftime("%Y%m%d")

        if df_result[df_result['date'] == date_S].__len__() > 0:
            logging.info("." + date_S)  #already have the records
            date = date + datetime.timedelta(1)
            continue

        logging.info("getting cctv news " + date_S)

        try:
            df_cctv_news = pro.cctv_news(date=date_S)
            df_result = df_result.append(df_cctv_news)
            logging.info("len " + str(df_result.__len__()))
            #            df_result.to_csv(output_csv, encoding='UTF-8', index=False)

            time.sleep(1)
        except:
            logging.info("exception in fetching cctv news")
        finally:
            if sys.exc_info() == (None, None, None):
                pass  # no exception
            else:
                logging.info(str(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                #logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8'))
                logging.info(sys.exc_value.message)  # print the human readable unincode
                logging.info("exception in fetching cctv news")
                sys.exc_clear()

        date = date + datetime.timedelta(1)

    df_result.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info("cctv news saved to " + output_csv + " . len " + str(df_result.__len__()))
    return (df_result)


def _fetch_stk_holdertrade(fast_fetch=False):
    fields = ['ts_code', 'ann_date', 'holder_name', 'holder_type', 'in_de', 'change_vol', 'change_ratio', 'after_share', 'after_ratio', 'avg_price', 'total_share', 'begin_date', 'close_date']

    today_S = datetime.date.today().strftime("%Y%m%d")

    stock_list = finlib.Finlib().get_A_stock_instrment()  # 603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=True, tspro_format=True)
    if debug_global:
        stock_list = stock_list[stock_list['code'] == "600519.SH"]

    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/holdertrade"  #股东增减持

    today_holder_trade_csv = "/home/ryan/DATA/result/today_holder_trade.csv"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    if (finlib.Finlib().is_cached(today_holder_trade_csv, 1)) and (not force_run_global):
        logging.info("file has been updated in 1 day, not fetch again " + today_holder_trade_csv)
    else:
        df_today = pro.stk_holdertrade(ann_date=today_S, fields=fields)
        df_today.to_csv(today_holder_trade_csv, encoding='UTF-8', index=False)
        logging.info("Saved today stock holder trade to " + today_holder_trade_csv + " . len " + str(df_today.__len__()))

        if debug_global:
            df_today = df_today[df_today['ts_code'] == "600519.SH"]

        #update each csv
        for ts_code in df_today['ts_code']:
            logging.info(ts_code)
            output_csv = dir + "/" + ts_code + ".csv"

            df_new = df_today[df_today['ts_code'] == ts_code]

            if os.path.exists(output_csv):
                logging.info("loading " + output_csv)
                df_base = pd.read_csv(output_csv, converters={'ann_date': str, 'begin_date': str, 'close_date': str})
                df_base = df_base.append(df_new)
            else:
                df_base = df_new

            df_base.to_csv(output_csv, encoding='UTF-8', index=False)
            logging.info(ts_code + " , append today stock holder trade to " + output_csv + " . len " + str(df_today.__len__()))

    #fetch all base
    if force_run_global:
        cnt = 0
        cnt_all = str(stock_list['code'].__len__())

        for ts_code in stock_list['code']:
            cnt += 1
            output_csv = dir + "/" + ts_code + ".csv"

            if finlib.Finlib().is_cached(output_csv, 5):
                logging.info("not fetch holder trade as the file updated in 5 day. " + output_csv)
                continue

            try:
                df = pro.stk_holdertrade(ts_code=ts_code, fields=fields)
                df.to_csv(output_csv, encoding='UTF-8', index=False)
                logging.info(str(cnt) + " of " + cnt_all + " , saved stock holder trade to " + output_csv + " . len " + str(df.__len__()))
                time.sleep(0.8)
            except:
                logging.info("exception, sleeping 30sec then renew the ts_pro connection")

            finally:

                if sys.exc_info() == (None, None, None):
                    pass  # no exception
                else:
                    # logging.info(unicode(traceback.print_exception(*sys.exc_info())).encode('utf8')) #python2
                    logging.info(str(traceback.print_exception(*sys.exc_info())).encode('utf8'))  #python3
                    logging.info(sys.exc_value.message)  # print the human readable unincode
                    logging.info("query: stk_holdertrade, ts_code: " + ts_code)
                    sys.exc_clear()

    return ()


def get_pro_concept():

    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_concept.csv"

    return (pd.read_csv(output_csv))


def _fetch_pro_repurchase():
    ts.set_token(myToken)
    pro = ts.pro_api()

    dir = fund_base_source + "/market"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    output_csv = dir + "/pro_repurchase.csv"

    if finlib.Finlib().is_cached(output_csv, 1):
        logging.info("not fetch repurchase as the file updated in 1 day")
        return ()

    #df = pro.repurchase(ann_date='', start_date='20190101', end_date='20180510')
    df = pro.repurchase()
    time.sleep(0.5)  #抱歉，您每分钟最多访问该接口200次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
    df.to_csv(output_csv, encoding='UTF-8', index=False)
    logging.info("pro repurchase saved to " + output_csv + " . len " + str(df.__len__()))
    return (df)


def get_pro_repurchase():
    dir = fund_base_source + "/market"
    output_csv = dir + "/pro_repurchase.csv"
    return (pd.read_csv(output_csv))


def concept_top():
    fund_csv = fund_base + '/merged/merged_all_20181231.csv'
    df_fund = pd.read_csv(fund_csv)

    input_csv = fund_base_source + "/market" + "/pro_concept.csv"
    df = pd.read_csv(input_csv)

    output_csv = '/home/ryan/DATA/result/concept_top.csv'
    df_out = pd.DataFrame()

    for cat_code in df['cat_code'].unique():
        #print(cat_code)
        df_sub = df[df['cat_code'] == cat_code].reset_index().drop('index', axis=1)
        cat_name = df_sub.iloc[0]['cat_name']

        df_fund_sub = df_fund[df_fund['ts_code'].isin(df_sub['ts_code'].to_list())].reset_index().drop('index', axis=1)
        roe_mean = df_fund_sub['roe'].mean()
        df_roe = df_fund_sub[df_fund_sub['roe'].rank(pct=True) > 0.85][['name', 'ts_code']]  #净资产收益率
        df_profit_dedt = df_fund_sub[df_fund_sub['profit_dedt'].rank(pct=True) > 0.85][['name', 'ts_code']]  #扣除非经常性损益后的净利润
        df_netprofit_margin = df_fund_sub[df_fund_sub['netprofit_margin'].rank(pct=True) > 0.85][['name', 'ts_code']]  #销售净利率
        df_npta = df_fund_sub[df_fund_sub['npta'].rank(pct=True) > 0.85][['name', 'ts_code']]  #总资产净利润
        df_netdebt = df_fund_sub[df_fund_sub['netdebt'].rank(pct=True) < 0.15][['name', 'ts_code']]  #净债务
        df_debt_to_assets = df_fund_sub[df_fund_sub['debt_to_assets'].rank(pct=True) < 0.15][['name', 'ts_code']]  #资产负债率
        df_q_roe = df_fund_sub[df_fund_sub['q_roe'].rank(pct=True) > 0.85][['name', 'ts_code']]  #净资产收益率(单季度)

        merged_inner = pd.merge(left=df_roe, right=df_profit_dedt, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_netprofit_margin, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_npta, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_netdebt, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_debt_to_assets, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)
        merged_inner = pd.merge(left=merged_inner, right=df_q_roe, how='inner', left_on='ts_code', right_on='ts_code', suffixes=('', '_x')).drop('name_x', axis=1)

        merged_inner.insert(2, 'cat_code', cat_code)
        merged_inner.insert(3, 'cat_name', cat_name)

        logging.info(str(cat_code) + " " + cat_name + ", " + str(merged_inner.__len__()) + " qualified stocks")

        if merged_inner.__len__() > 0:
            logging.info(merged_inner)
            pass

        df_out = pd.concat([df_out, merged_inner], sort=False).reset_index().drop('index', axis=1)

    df_out = finlib.Finlib().ts_code_to_code(df=df_out)
    df_out.to_csv(output_csv, encoding='UTF-8', index=False)

    logging.info('concept_top saved to ' + output_csv + " ,len " + str(df_out.__len__()))
    pass


def main():
    ########################
    #
    #########################

    logging.info("\n")
    logging.info("SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-f", "--fetch_data_all", action="store_true", dest="fetch_all_f", default=False, help="fetch all the quarterly fundatation history data")

    parser.add_option("--fetch_pro_basic", action="store_true", dest="fetch_pro_basic_f", default=False, help="")

    parser.add_option("--fetch_stk_holdertrade", action="store_true", dest="fetch_stk_holdertrade_f", default=False, help="")
    parser.add_option("--fetch", action="store_true", dest="fetch_f", default=False, help="fetch pro income,balance,cashflow,mainbz,dividend,indicator,audit,forecast,express,disclosure ")
    parser.add_option("--fetch_basic_quarterly", action="store_true", dest="fetch_basic_quarterly_f", default=False, help="")
    parser.add_option("--fetch_basic_daily", action="store_true", dest="fetch_basic_daily_f", default=False, help="")
    parser.add_option("--fetch_pro_concept", action="store_true", dest="fetch_pro_concept_f", default=False, help="")
    parser.add_option("--fetch_pro_repurchase", action="store_true", dest="fetch_pro_repurchase_f", default=False, help="")
    parser.add_option("--fetch_cctv_news", action="store_true", dest="fetch_cctv_news_f", default=False, help="")

    parser.add_option("-e", "--extract_latest", action="store_true", dest="extract_latest_f", default=False, help="extract latest quarter data")

    parser.add_option("-m", "--merge_quarterly", action="store_true", dest="merge_quarterly_f", default=False, help="merge date by quarter")

    parser.add_option("-a", "--analyze", action="store_true", dest="analyze_f", default=False, help="analyze based on the merged quarterly csv")

    parser.add_option("--concept_top", action="store_true", dest="concept_top_f", default=False, help="analyze top 3 stocks in each concept")

    parser.add_option("--overwrite", action="store_true", dest="overwrite_f", default=False, help="overwrite existing analyse output csv, using with -a")

    parser.add_option("--fully_a", action="store_true", dest="fully_a_f", default=False, help="run all the analyze steps, using with -a")

    parser.add_option("-d", "--daily_a", action="store_true", dest="daily_a_f", default=False, help="only run analyze_step_6, using with -a")

    parser.add_option("-s", "--sum_mainbz", action="store_true", dest="sum_mainbz_f", default=False, help="sum_mainbz, output fina_mainbz_sum.csv")

    parser.add_option("--percent_mainbz_f", action="store_true", dest="percent_mainbz_f", default=False, help="calc each item percent in the mainbz, output source/latest/fina_mainbz_percent.csv")

    parser.add_option("-c", "--fast_fetch", action="store_true", dest="fast_fetch_f", default=False, help="only fetch stocks whose high score >70.")

    parser.add_option("--wh_hencow_fcf", action="store_true", dest="white_horse_hencow_fcf_f", default=False, help="extract white horse, hen, cow, freecashflow.")

    parser.add_option("--merge_individual", action="store_true", dest="merge_individual_f", default=False, help="consolidate indiviaul from each period.")

    parser.add_option("--merge_local", action="store_true", dest="merge_local_f", default=False, help="consolidate individual csv to source/[*].csv")
    parser.add_option("--merge_local_basic", action="store_true", dest="merge_local_basic_f", default=False, help="consolidate daily basic to source/basic.csv")

    parser.add_option("--big_memory", action="store_true", dest="big_memory_f", default=False, help="consumes 4G memory to load all the jaqs and tushare data to two df")

    parser.add_option("-u", "--debug", action="store_true", dest="debug_f", default=False, help="debug mode, using merge.dev, report.dev folder")

    parser.add_option("--force_run", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--express_notify", action="store_true", dest="force_run_f", default=False, help="force fetch, force generate file, even when file exist or just updated")

    parser.add_option("--disclosure_date_notify_day", type="int", dest="disclosure_date_notify_day_f", default=None, help="generate stock list that will be disclosured in the give days.")

    #parser.add_option("-v", "--verify_fund_increase", action="store_true",
    #                  dest="verify_fund_increase_f", default=False,
    #                  help="verify quartly score and buy and increase to today")

    (options, args) = parser.parse_args()
    fetch_all_f = options.fetch_all_f
    extract_latest_f = options.extract_latest_f
    merge_quarterly_f = options.merge_quarterly_f
    analyze_f = options.analyze_f
    concept_top_f = options.concept_top_f
    overwrite_f = options.overwrite_f
    fully_a_f = options.fully_a_f
    daily_a_f = options.daily_a_f
    sum_mainbz_f = options.sum_mainbz_f
    percent_mainbz_f = options.percent_mainbz_f
    fast_fetch_f = options.fast_fetch_f
    white_horse_hencow_fcf_f = options.white_horse_hencow_fcf_f
    merge_individual_f = options.merge_individual_f
    merge_local_f = options.merge_local_f
    merge_local_basic_f = options.merge_local_basic_f
    big_memory_f = options.big_memory_f
    debug_f = options.debug_f
    force_run_f = options.force_run_f
    disclosure_date_notify_day_f = options.disclosure_date_notify_day_f

    #verify_fund_increase_f = options.verify_fund_increase_f

    logging.info("fetch_all_f: " + str(fetch_all_f))
    logging.info("extract_latest_f: " + str(extract_latest_f))
    logging.info("merge_quarterly_f: " + str(merge_quarterly_f))
    logging.info("analyze_f: " + str(analyze_f))
    logging.info("concept_top_f: " + str(concept_top_f))
    logging.info("overwrite_f: " + str(overwrite_f))
    logging.info("fully_a_f: " + str(fully_a_f))
    logging.info("daily_a_f: " + str(daily_a_f))
    logging.info("sum_mainbz_f: " + str(sum_mainbz_f))
    logging.info("percent_mainbz_f: " + str(percent_mainbz_f))
    logging.info("fast_fetch_f: " + str(fast_fetch_f))
    logging.info("white_horse_hencow_fcf_f: " + str(white_horse_hencow_fcf_f))
    logging.info("merge_individual_f: " + str(merge_individual_f))
    logging.info("merge_local_f: " + str(merge_local_f))
    logging.info("merge_local_basic_f: " + str(merge_local_basic_f))
    logging.info("big_memory_f: " + str(big_memory_f))
    logging.info("debug_f: " + str(debug_f))
    logging.info("force_run_f: " + str(force_run_f))
    logging.info("disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))
    #logging.info("disclosure_date_notify_day_f: " + str(disclosure_date_notify_day_f))

    set_global(debug=debug_f, big_memory=big_memory_f, force_run=force_run_f)

    if options.fetch_pro_basic_f:
        _fetch_pro_basic()

    if options.fetch_stk_holdertrade_f:
        _fetch_stk_holdertrade(fast_fetch=fast_fetch_f)

    if options.fetch_f:
        fetch(fast_fetch=fast_fetch_f)

    if options.fetch_basic_quarterly_f:
        fetch_basic_quarterly()

    if options.fetch_basic_daily_f:
        fetch_basic_daily(fast_fetch=fast_fetch_f)

    if options.fetch_pro_concept_f:
        _fetch_pro_concept()

    if options.fetch_pro_repurchase_f:
        _fetch_pro_repurchase()

    if options.fetch_cctv_news_f:
        _fetch_cctv_news()

    if options.concept_top_f:
        concept_top()

    if fetch_all_f:
        #fast first
        fetch_basic_daily(fast_fetch=fast_fetch_f)  #300 credits
        _fetch_pro_concept()  #300 credits
        _fetch_pro_repurchase()  #600 credits
        #_fetch_cctv_news() #120 credits.  5 times/minute

        #then timecosting
        _fetch_pro_basic()
        #_fetch_stk_holdertrade(fast_fetch=fast_fetch_f) #don't have 2000 api credits
        fetch(fast_fetch=fast_fetch_f)
        fetch_basic_quarterly()


    elif merge_individual_f:

        # generate source/individual_per_stock/*_basic.csv from source/basic.csv
        merge_individual_bash_basic(fast_fetch=fast_fetch_f)

        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
            logging.info("not processing fundermental data at this month. ")
            exit()
        else:
            # generate source/individual_per_stock/*.csv from source/*.csv
            merge_individual()

    elif merge_local_f:
        #generate source/*.csv, e.g source(source.dev)/income.csv;
        # combine source/income.csv ... from source/individual_per_stock/*.csv
        #merge_local()
        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
            logging.info("not processing fundermental data at this month. ")
            exit()
        else:
            merge_local_bash()
    elif merge_local_basic_f:
        merge_local_bash_basic(csv_basic, fast=fast_fetch_f)
    elif extract_latest_f:
        extract_latest()
    elif merge_quarterly_f:
        merge_local_bash_basic_quarterly()

        if (debug_f) or (big_memory_f):
            merge_quarterly(fast=fast_fetch_f)
        else:
            logging.info("merge quarterly requires lot memory, use with either --big_memory, or --debug")
    elif sum_mainbz_f:
        sum_fina_mainbz()
    elif percent_mainbz_f:
        percent_fina_mainbz()
    elif analyze_f:

        # not fetching/calculating fundermental data at month 5,6,9, 11, 12
        if not finlib.Finlib().get_report_publish_status()['process_fund_or_not']:
            logging.info("not processing fundermental data at this month. ")
            exit()
        else:
            analyze(fully_a=fully_a_f, daily_a=daily_a_f, fast=fast_fetch_f)
    #elif verify_fund_increase_f:
    #    verify_fund_increase()
    elif white_horse_hencow_fcf_f:
        #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/white_horse.csv
        extract_white_horse()

        #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/hen_cow.csv
        extract_hen_cow()

        #/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report.dev/freecashflow_price_ratio.csv
        extract_high_freecashflow_price_ratio()
    elif disclosure_date_notify_day_f:
        disclosure_date_notify(days=disclosure_date_notify_day_f)

    logging.info('script completed')
    os._exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
