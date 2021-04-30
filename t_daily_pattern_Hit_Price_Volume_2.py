import pandas as pd
import os
import finlib
import finlib_indicator
import logging
import tabulate
import tushare as ts
import re
import talib 



def _exam_a_stock(code,date_exam_day,
                            df_quarterly_merged_all,
                            df_quarterly_merged_all_1q_before,
                            df_quarterly_merged_all_1y_before
                            ):
    ##########################################################
    ##     超级绩效, 趋势模板. Check each stock, loop 3000+
    ##########################################################

    #code = "SH600519" #ryan debug

    # /home/ryan/DATA/DAY_Global/AG/SH600519.csv
    # 代码,时间,开盘价,最高价,最低价,收盘价,成交量(股),成交额(元),换手率
    df_a_stock_price_history = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/DAY_Global/AG/'+code+".csv")
    if df_a_stock_price_history.__len__() < 200:
        logging.info("No enough data for "+code+", expected 200+, actual "+str(df_a_stock_price_history.__len__()))
        return(None)

    # df must have column (code, date, open, low,high,close)
    df_a_stock_price_history = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_a_stock_price_history, short=15, middle=150, long=200)
    df_a_stock_price_history = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_a_stock_price_history, short=50, middle=60, long=260)
    df_a_stock_price_history = finlib_indicator.Finlib_indicator().add_tr_atr(df=df_a_stock_price_history, short=5, middle=10, long=20)
    df_a_stock_price_history = finlib_indicator.Finlib_indicator().add_rsi(df=df_a_stock_price_history, short=5, middle=14, long=20)



    df = df_a_stock_price_history

    #-2. pv, today_vol > last_15_days_down_volume. pocket pivot: up-volume equal to or greater than the largest down-volume day over the prior 10 days
    exp = finlib.Finlib().pocket_pivot_check(df=df)
    if (exp is not False):
        df['cond_pocket_pivot']=True
        df['pocket_pivot_maxvol_N_downdays'] = exp['pocket_pivot_this_vol_gt_N_records_of_down_vol'] #>10 or >15 is positive for Pocket pivot volume condition.
    else:
        df['cond_pocket_pivot'] = False


    #-1. today volumn is the min/max of last N days
    min_max = finlib.Finlib().count_min_max_value_days(df, 'volume')
    df['vol_is_min_of_last_n_records_LastRowValidOnly']=min_max['this_is_min_of_last_n_records']
    df['vol_is_max_of_last_n_records_LastRowValidOnly']=min_max['this_is_max_of_last_n_records']

    # 0. today close is the min/max of last N days
    min_max = finlib.Finlib().count_min_max_value_days(df, 'close')
    df['p_is_min_of_last_n_records_LastRowValidOnly']=min_max['this_is_min_of_last_n_records']
    df['p_is_max_of_last_n_records_LastRowValidOnly']=min_max['this_is_max_of_last_n_records']

    # 1. 股价高于150天(30周) 与 200天(40周) 移动平均
    df['cond_1_p_gt_ma150_ma200'] = (df['close'] > df['close_150_sma']) & (df['close'] > df['close_200_sma'])

    #2. 15MA高于200MA
    df['cond_2_ma15_gt_ma200'] = df['close_15_sma'] > df['close_200_sma']

    #3. 200MA至少有一个月处于上升状态(最好有4,5个月以上)
    df['cond_3_ma200_gt_ma200_50days_ago_LastRowValidOnly'] = df['close_200_sma'].iloc[-1] > df['close_200_sma'].iloc[-50]

    #4. 50MA > 150MA, 50MA>200MA
    df['cond_4_ma50_gt_ma150_ma200'] = (df['close_50_sma'] > df['close_150_sma']) & (df['close_50_sma'] > df['close_200_sma'])

    #5. P > 50MA
    df['cond_5_p_gt_ma50'] = df['close'] > df['close_50_sma']


    #6. (P - 52weeklowest)/(52weekhigh - 52weeklowest) > 0.3 股价比52周低点高30%. (很多最佳候选股在突破横向整理而展开大规模涨势之前,股价已经比52周低点高出100%, 300% 或者更多)
    _52weekhigh=df['close'][-260:].max()
    _52weeklow=df['close'][-260:].min()
    df['52weekhigh_LastRowValidOnly'] = _52weekhigh
    df['52weeklow_LastRowValidOnly'] = _52weeklow


    df['p_distance_to_52weeklow_LastRowValidOnly'] = round((df['close'].iloc[-1] - _52weeklow)/(_52weekhigh - _52weeklow),2)
    df['cond_6_p_gt_03_52weeklow_LastRowValidOnly'] = ((df['close'].iloc[-1] - _52weeklow)/(_52weekhigh - _52weeklow)) > 0.3

    #7. (52weekhigh - p)/(52weekhigh - 52weeklowest) < 0.15 (越接近越好)
    df['p_distance_to_52weekhigh_LastRowValidOnly'] = round((_52weekhigh - df['close'].iloc[-1])/(_52weekhigh - _52weeklow),2)
    df['cond_7_p_lt_015_52weekhigh_LastRowValidOnly'] = ((_52weekhigh - df['close'].iloc[-1])/(_52weekhigh - _52weeklow)) < 0.15

    #8. RSI > 70, 最好是80, 90
    df['cond_8_rsi_gt_70'] = df['rsi_middle_14'] > 70

    #9. P > 12 或者 20
    df['cond_9_p_gt_12'] =  df['close'] > 12



    #10. 销售增速, sales acceleration - last 2 Qtrs. S_t > S_t-1
    tr = 0
    tr_1q = 0
    tr_1y = 0

    if code in df_quarterly_merged_all['code'].values:
        tr =  df_quarterly_merged_all[df_quarterly_merged_all['code']==code]['tr_yoy'].iloc[0]
    if code in df_quarterly_merged_all_1q_before['code'].values:
        tr_1q = df_quarterly_merged_all_1q_before[df_quarterly_merged_all_1q_before['code']==code]['tr_yoy'].iloc[0]
    if code in df_quarterly_merged_all_1y_before['code'].values:
        tr_1y = df_quarterly_merged_all_1y_before[df_quarterly_merged_all_1y_before['code']==code]['tr_yoy'].iloc[0]

    df['tr_LastRowValidOnly'] = tr
    df['tr_1q_LastRowValidOnly'] = tr_1q
    df['tr_1y_LastRowValidOnly'] = tr_1y

    df['cond_10_sales_acceleration'] = (tr > 0) and (tr_1q > 0 ) and (tr_1y > 0)

    #11. 上季度销售增长>0.05.  高速成长股票. 未有盈余,正在抢占市场.
    #tr_yoy	float	Y	营业总收入同比增长率(%)
    df['cond_11_sales_acceleration_gt_5'] = tr > 5

    #12. 过去50天的平均成交量, 至少25000股以上
    df['volume_mean_50'] = df['volume'].rolling(window=50).mean()
    df['cond_12_volume_mean_50_gt_25000'] = df['volume'].rolling(window=50).mean() > 25000

    ### Following are not the condition of the video
    #13. df_30day_lowest
    _30dayhigh=df['close'][-30:].max()
    _30daylow=df['close'][-30:].min()

    df['30dayhigh_LastRowValidOnly'] = _30dayhigh
    df['30dayhigh_LastRowValidOnly'] = _30daylow

    #14. df_15day_standard_deviation_0.15low
    _14day_close_stable=df['close'][-15:-1].std()/df['close'][-15:-1].mean() #0.025
    _14day_vol_stable= df['volume'][-15:-1].std()/df['volume'][-15:-1].mean() #0.42

    df['14day_close_std_mean_ratio_LastRowValidOnly'] = _14day_close_stable
    df['14day_volume_std_mean_ratio_LastRowValidOnly'] = _14day_vol_stable

    # Merge the stock to the all stock list
    #print(df.columns.values)
    df_a_stock_price_history_last = df[df['date']==date_exam_day]
    #(df_a_stock_price_history_last.columns.values)
    return(df_a_stock_price_history_last)


def step1_generate_df(output_csv,date_exam_day):

    if finlib.Finlib().is_cached(output_csv, 1):
        logging.info("file updated in 1 days, "+output_csv)
        return()

    #code = finlib.Finlib().add_market_to_code_single(code_6d,dot_f=False,tspro_format=False) #SH600519
    #tscode = finlib.Finlib().add_market_to_code_single(code_6d,dot_f=True,tspro_format=True) #600519.SH

    date_rpt_q = finlib.Finlib().get_year_month_quarter()['ann_date_2q_before'] #20200630
    date_rpt_q1 = finlib.Finlib().get_year_month_quarter()['ann_date_3q_before'] #20200331
    date_rpt_y1 = finlib.Finlib().get_year_month_quarter()['stable_report_perid'] #20191231

    fund2_dir = '/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2'

    # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_daily/basic_20200820.csv
    # ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv
    df_daily_stocks_basic = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/source/basic_daily/basic_'+date_exam_day+".csv")

    df_daily_stocks_basic = finlib.Finlib().remove_garbage(df=df_daily_stocks_basic)

    #量比(quantity relative ratio)这个指标所反映出来的是当前盘口的成交力度与最近五天的成交力度的差别，这个差别的值越大表明盘口成交越趋活跃，
    #0.5倍以下的缩量情形也值得好好关注，其实严重缩量不仅显示了交易不活跃的表象，同时也暗藏着一定的市场机会
    #0.8-1.5倍，则说明成交量处于正常水平；
    #1.5-2.5倍之间则为温和放量;
    #2.5-5倍，则为明显放量，若股价相应地突破重要支撑或阻力位置，则突破有效的几率颇高，可以相应地采取行动；
    #5-10倍，则为剧烈放量，如果是在个股处于长期低位出现剧烈放量突破，涨势的后续空间巨大，是“钱”途无量的象征，
    #10倍以上的股票，一般可以考虑反向操作。在涨势中出现这种情形，说明见顶的可能性压倒一切，即使不是彻底反转，至少涨势会休整相当长一段时间。
    #20倍以上的情形基本上每天都有一两单，是极端放量的一种表现，这种情况的反转意义特别强烈，如果在连续的上涨之后，成交量极端放大，但股价出现“滞涨”现象，则是涨势行将死亡的强烈信号。

    df_daily_stocks_basic['volume_ratio_perc_rank'] = df_daily_stocks_basic['volume_ratio'].rank(pct=True) #量比
    df_daily_stocks_basic['total_mv_perc_rank'] = df_daily_stocks_basic['total_mv'].rank(pct=True) #总市值 （万元）
    df_daily_stocks_basic['circ_mv_perc_rank'] = df_daily_stocks_basic['circ_mv'].rank(pct=True) #流通市值（万元）
    df_daily_stocks_basic['pe_perc_rank'] = df_daily_stocks_basic['pe'].rank(pct=True) #市盈率（总市值/净利润， 亏损的PE为空）
    df_daily_stocks_basic['pe_ttm_perc_rank'] = df_daily_stocks_basic['pe_ttm'].rank(pct=True) #市盈率（TTM，亏损的PE为空）
    df_daily_stocks_basic['ps_ttm_perc_rank'] = df_daily_stocks_basic['ps_ttm'].rank(pct=True) #市销率（TTM）
    df_daily_stocks_basic['turnover_rate_f_perc_rank'] = df_daily_stocks_basic['turnover_rate_f'].rank(pct=True) #换手率（自由流通股）
    #df_daily_stocks_basic['dv_ttm_perc_rank'] = df_daily_stocks_basic['dv_ttm'].rank(pct=True) #股息率（TTM）（%）

    #remove small circ_mv stocks
    if False:
        logging.info("reduce exam stocks. keep circ_mv_perc_rank > 0.5 ")
        df_daily_stocks_basic = df_daily_stocks_basic[df_daily_stocks_basic['circ_mv_perc_rank'] > 0.50 ] #df len: 3923 --> 1962
        logging.info("reduce exam stocks. keep total_mv_perc_rank > 0.5 ")
        df_daily_stocks_basic = df_daily_stocks_basic[df_daily_stocks_basic['total_mv_perc_rank'] > 0.50 ] # 1962 --> 1690
        logging.info("reduce exam stocks. keep turnover_rate_f_perc_rank < 0.95 ")
        df_daily_stocks_basic = df_daily_stocks_basic[df_daily_stocks_basic['turnover_rate_f_perc_rank'] < 0.95 ] #need this? k  # 1690 -> 1636
        logging.info("reduce exam stocks. remove garbage ")
        df_daily_stocks_basic = finlib.Finlib().remove_garbage(df=df_daily_stocks_basic, code_field_name='code') #1636 -> 545

    # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/basic_quarterly/basic_20200630.csv
    # ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,total_mv,circ_mv


    df_quarterly_basic = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/source/basic_quarterly/basic_'+date_rpt_q+".csv")

    # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/individual_per_stock/600519.SH_income.csv
    # name,ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag

    # /home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/peg/20200630.csv
    # code,name,end_date,peg_1,peg_4,egr_1,egr_4,trade_date,close,eps,roe,end_date_1q,eps_1q,roe_1q,end_date_4q,eps_4q,roe_4q,open,high,low,pre_close,change,pct_chg,vol,amount,pe
    df_peg = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/source/peg/'+date_rpt_q+".csv")


    # /home/ryan/DATA/pickle/daily_update_source/2020-08-21.csv
    # ts_code,trade_date,open,high,low,close 收盘价,pre_close 昨收价,change 涨跌额,pct_chg 涨跌幅 未复权 ,vol 成交量 （手）,amount 成交额 （千元）
    df_daily_stocks_price = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv='/home/ryan/DATA/pickle/daily_update_source/ag_daily_'+date_exam_day+".csv")


    # /home/ryanDATA/pickle/Stock_Fundamental/fundamentals_2/merged/merged_all_20200630.csv
    # name,ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag,ann_date_df_balancesheet,f_ann_date_df_balancesheet,end_date_df_balancesheet,report_type_df_balancesheet,comp_type_df_balancesheet,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,update_flag_df_balancesheet,ann_date_df_cashflow,f_ann_date_df_cashflow,end_date_df_cashflow,comp_type_df_cashflow,report_type_df_cashflow,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag_df_cashflow,ann_date_df_fina_indicator,end_date_df_fina_indicator,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit_df_fina_indicator,ebitda_df_fina_indicator,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag_df_fina_indicator,ann_date_df_fina_audit,end_date_df_fina_audit,audit_result,audit_fees,audit_agency,audit_sign,end_date_df_fina_mainbz,bz_sales,bz_profit,bz_cost,curr_type,update_flag_df_fina_mainbz
    df_quarterly_merged_all = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/merged/merged_all_'+date_rpt_q+".csv")
    df_quarterly_merged_all_1q_before = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/merged/merged_all_'+date_rpt_q1+".csv")
    df_quarterly_merged_all_1y_before = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=fund2_dir+'/merged/merged_all_'+date_rpt_y1+".csv")



    df_exam_day_all = pd.merge(df_daily_stocks_basic, df_daily_stocks_price, on=['code','date'], how='inner', suffixes=('', '_x'))
    df_exam_day_all = finlib.Finlib().add_stock_name_to_df(df=df_exam_day_all, ts_pro_format=False)



    #df_exam_day_all = df_exam_day_all.iloc[10:13] #ryan debug

    df_rst_stocks_daily = pd.DataFrame()
    i = 0
    for code in df_exam_day_all['code']:
        i+=1
        logging.info("Exam start "+str(code)+" , "+str(i) + " of "+str(df_exam_day_all.__len__()))
        df_a_stock_price_history_last = _exam_a_stock(code=code, date_exam_day=date_exam_day,
                                                                df_quarterly_merged_all=df_quarterly_merged_all,
                                                                df_quarterly_merged_all_1q_before=df_quarterly_merged_all_1q_before,
                                                                df_quarterly_merged_all_1y_before=df_quarterly_merged_all_1y_before,
                                                                )

        if df_a_stock_price_history_last is not None:
            # print(df_a_stock_price_history_last[['code', 'cond_6_p_gt_03_52weeklow_LastRowValidOnly']])
            df_rst_stocks_daily = pd.concat([df_rst_stocks_daily, df_a_stock_price_history_last], axis=0)
            # print(df_rst_stocks_daily[['code', 'cond_6_p_gt_03_52weeklow_LastRowValidOnly']])
        logging.info("Exam completed " + str(code))

    df_exam_day_all = pd.merge(df_exam_day_all, df_rst_stocks_daily,  on=['code'], how='left', suffixes=('', '_x'))

    df_exam_day_all.to_csv(output_csv, encoding='UTF-8', index=False)


def step2_ana(df_exam_day_all):
    #   print(df_exam_day_all.columns.values)
    print(tabulate.tabulate(df_exam_day_all.head(1), headers='keys', tablefmt='psql'))

    exam_date = df_exam_day_all['date'].dropna().values[0]

    df_exam_day_all = finlib.Finlib().remove_garbage(df=df_exam_day_all,code_field_name='code', code_format='C2D6')

    # df_exam_day_all:
    # ['code' 'trade_date' 'close' 'turnover_rate' 'turnover_rate_f'
    # 'volume_ratio' 'pe' 'pe_ttm' 'pb' 'ps' 'ps_ttm' 'total_share'
    # 'float_share' 'total_mv' 'circ_mv' 'volume_ratio_perc_rank'
    # 'total_mv_perc_rank' 'circ_mv_perc_rank' 'pe_perc_rank'
    # 'pe_ttm_perc_rank' 'ps_ttm_perc_rank' 'turnover_rate_f_perc_rank' 'open'
    # 'high' 'low' 'close_x' 'pre_close' 'change' 'pct_chg' 'volume' 'amount'
    # 'date' 'open_x' 'high_x' 'low_x' 'close_x' 'volume_x' 'amount_x' 'tnv'
    # 'close_15_sma' 'sma_short_15' 'close_150_sma' 'sma_middle_150'
    # 'close_200_sma' 'sma_long_200' 'close_15_ema' 'ema_short_15'
    # 'close_150_ema' 'ema_middle_150' 'close_200_ema' 'ema_long_200'
    # 'close_50_sma' 'sma_short_50' 'close_60_sma' 'sma_middle_60'
    # 'close_260_sma' 'sma_long_260' 'close_50_ema' 'ema_short_50'
    # 'close_60_ema' 'ema_middle_60' 'close_260_ema' 'ema_long_260'
    # 'close_-1_s' 'tr' 'tr_5_smma' 'atr_5' 'atr_short_5' 'atr_middle_10'
    # 'atr_long_20' 'close_-1_d' 'closepm' 'closenm' 'closepm_5_smma'
    # 'closenm_5_smma' 'rs_5' 'rsi_5' 'rsi_short_5' 'rsi_middle_14'
    # 'rsi_long_20' 'cond_1_p_gt_ma150_ma200' 'cond_2_ma15_gt_ma200'
    # 'cond_3_ma200_gt_ma200_50days_ago_LastRowValidOnly'
    # 'cond_4_ma50_gt_ma150_ma200' 'cond_5_p_gt_ma50'
    # '52weekhigh_LastRowValidOnly' '52weeklow_LastRowValidOnly'
    # 'p_distance_to_52weeklow_LastRowValidOnly'
    # 'cond_6_p_gt_03_52weeklow_LastRowValidOnly'
    # 'p_distance_to_52weekhigh_LastRowValidOnly'
    # 'cond_7_p_lt_015_52weekhigh_LastRowValidOnly' 'cond_8_rsi_gt_70'
    # 'cond_9_p_gt_12' 'tr_LastRowValidOnly' 'tr_1q_LastRowValidOnly'
    # 'tr_1y_LastRowValidOnly' 'cond_10_sales_acceleration'
    # 'cond_11_sales_acceleration_gt_5' 'volume_mean_50'
    # 'cond_12_volume_mean_50_gt_25000' '30dayhigh_LastRowValidOnly'
    # '30day_close_std_mean_ratio_LastRowValidOnly'
    # '30day_volume_std_mean_ratio_LastRowValidOnly']


    ############################
    # VCP
    ############################
    df_vcp = df_exam_day_all

    check_list = [
        "cond_1_p_gt_ma150_ma200",
        "cond_2_ma15_gt_ma200",
        "cond_3_ma200_gt_ma200_50days_ago_LastRowValidOnly",
        "cond_4_ma50_gt_ma150_ma200",
        "cond_5_p_gt_ma50",
        "cond_6_p_gt_03_52weeklow_LastRowValidOnly",
        "cond_7_p_lt_015_52weekhigh_LastRowValidOnly",
        "cond_8_rsi_gt_70",
        "cond_9_p_gt_12",
        "cond_12_volume_mean_50_gt_25000",
        "cond_11_sales_acceleration_gt_5",
        "cond_10_sales_acceleration",
    ]

    for cn in check_list:
        if re.match("cond_", cn):

            #logging.info("fill False to NA to column "+cn)
            df_vcp[cn] = df_vcp[cn].fillna(value=False)
            df_vcp = df_vcp[df_vcp[cn]]
            logging.info("df_vcp len "+str(df_vcp.__len__())+" , filter " + cn)

            if df_vcp.__len__() == 0:
                logging.info("df_vcp is emtpy")
                break

            df_vcp.to_csv(rst_dir + "/vcp.csv", encoding='UTF-8', index=False)

    logging.info("vcp saved to "+rst_dir + "/vcp.csv"+" , len "+str(df_vcp.__len__()))
    logging.info(tabulate.tabulate(df_vcp.head(3), headers='keys', tablefmt='psql'))



    ############################
    # pe top, bottom
    ############################
    _df_filter_save(df = df_exam_day_all[df_exam_day_all['pe_perc_rank'] >0.7],
                    col_list=['code','name','date','pe','pe_perc_rank'],
                    sort_by_list=['pe_perc_rank'],sort_asc_list=[False], #False: Highest first
                    to_csv_f=rst_dir+"/pe_top_30p.csv"
                    )

    _df_filter_save(df = df_exam_day_all[df_exam_day_all['pe_perc_rank'] <0.3],
                    col_list=['code','name','date','pe','pe_perc_rank'],
                    sort_by_list=['pe_perc_rank'],sort_asc_list=[True], #True: Lowest first
                    to_csv_f=rst_dir+"/pe_bottom_30p.csv"
                    )

    ############################
    # zhangting, dieting
    ############################
    _df_filter_save(df = df_exam_day_all[df_exam_day_all['pct_chg'] > 9.9],
                    col_list=['code','name','date','close','pct_chg'],
                    sort_by_list=['pct_chg'],sort_asc_list=[False],
                    to_csv_f=rst_dir+"/zhang_ting.csv"
                    )

    _df_filter_save(df = df_exam_day_all[df_exam_day_all['pct_chg']< -9.9],
                    col_list=['code','name','date','close','pct_chg'],
                    sort_by_list=['pct_chg'],sort_asc_list=[False],
                    to_csv_f=rst_dir+"/die_ting.csv"
                    )

    ############################
    # volume_ratio_top_20p
    ############################
    _df_filter_save(df = df_exam_day_all[(df_exam_day_all['volume_ratio_perc_rank'] > 0.8) & (df_exam_day_all['volume_ratio'] > 1.5)],
                    col_list=['code','name','date','close','volume_ratio_perc_rank','volume_ratio'],
                    sort_by_list=['volume_ratio_perc_rank','volume_ratio'],sort_asc_list=[False,False],
                    to_csv_f=rst_dir+"/volume_ratio_top_20p.csv"
                    )

    ############################
    # volume_ratio_bottom_10p
    ############################
    _df_filter_save(df = df_exam_day_all[(df_exam_day_all['volume_ratio_perc_rank'] < 0.1) & (df_exam_day_all['volume_ratio'] < 0.5)],
                    col_list=['code','name','date','close','volume_ratio_perc_rank','volume_ratio'],
                    sort_by_list=['volume_ratio_perc_rank','volume_ratio'],sort_asc_list=[True,True],
                    to_csv_f=rst_dir+"/volume_ratio_bottom_10p.csv"
                    )



    ############################
    # volume_ratio_bottom_10p
    # df_volume_ration_00_10_neuro #涨停板时量比在1以下的股票，上涨空间无可限量，第二天开盘即封涨停的可能性极高。
    ############################
    _df_filter_save(df = df_exam_day_all[(df_exam_day_all['pct_chg'] > 9.9) & (df_exam_day_all['volume_ratio'] < 1)],
                    col_list=['code','name','date','close','pct_chg','volume_ratio_perc_rank','volume_ratio'],
                    sort_by_list=['volume_ratio_perc_rank','volume_ratio'],sort_asc_list=[False,False],
                    to_csv_f=rst_dir+"/zhangting_volume_ration_lt_1.csv"
                    )


    ############################
    # pocket pivot
    ############################
    # print(finlib.Finlib().pprint(df_exam_day_all[['cond_pocket_pivot']]))
    df_exam_day_all['cond_pocket_pivot'].fillna(value=False, inplace=True)
    # print("==============")
    # print(finlib.Finlib().pprint(df_exam_day_all[['cond_pocket_pivot']]))
    _df_filter_save(df = df_exam_day_all[df_exam_day_all['cond_pocket_pivot']],
                    col_list=['code','name','date','close','pct_chg',
                              'cond_pocket_pivot','pocket_pivot_maxvol_N_downdays' ,
                              '14day_close_std_mean_ratio_LastRowValidOnly','14day_volume_std_mean_ratio_LastRowValidOnly'],
                    sort_by_list=['pocket_pivot_maxvol_N_downdays'],sort_asc_list=[False],
                    to_csv_f=rst_dir+"/pocket_pivot.csv"
                    )


    ############################
    # stable price/vol 14 days
    ############################
    _df_filter_save(df = df_exam_day_all,
                    col_list=['code','name','date','close','pct_chg','14day_close_std_mean_ratio_LastRowValidOnly','14day_volume_std_mean_ratio_LastRowValidOnly'],
                    sort_by_list=['14day_volume_std_mean_ratio_LastRowValidOnly','14day_close_std_mean_ratio_LastRowValidOnly'],sort_asc_list=[True,True],
                    to_csv_f=rst_dir+"/stable_price_volume.csv"
                    )

    ############################
    # template
    ############################
    # _df_filter_save(df = df_exam_day_all[df_exam_day_all['pct_chg']< -9.9],
    #                 col_list=['code','name','date','close','pct_chg'],
    #                 sort_by_list=['pct_chg'],sort_asc_list=[False],
    #                 to_csv_f=rst_dir+"/die_ting.csv"
    #                 )


def _df_filter_save(df, col_list, sort_by_list, sort_asc_list, to_csv_f):
    df = df[col_list]
    df = df.sort_values(by=sort_by_list, ascending=sort_asc_list, inplace=False)
    df = df.reset_index().drop('index', axis=1)
    df.to_csv(to_csv_f, encoding='UTF-8', index=False)
    logging.info("saved "+to_csv_f+" , len "+str(df.__len__()))
    logging.info(tabulate.tabulate(df.head(3), headers='keys', tablefmt='psql'))




    ######################################
    #
    ######################################
    # df_52week_0.15low
    # df_52week_0.75high
    #
    #

    # df_high_minus_low__0.75high (max voliaty)

    # df_rsi
    # df_rsi_divergence

    # 长期低位出现剧烈放量突破长势的: 250 day 0.1 low, volume_ration in 5~10
    # 缩量创新高的股票多说是长庄股: volume_ration < 0.5, 30 days highest price,up_top_zhangting, volume_ration < 1

    # df_cheng_jiao_er_0.15low
    # df_cheng_jiao_er_0.75high
    # df_bar_cross
    # df_bar_long_head
    # df_bar_long_leg
    # df_bar_no_head_leg

    # df_volume_ration_0.15low
    # df_volume_ration_0.75high

    # df_??? 在跌停板的情况下，量比越小则说明杀跌动能未能得到有效宣泄，后市仍有巨大下跌空间。
    # df_??? 底部放量，价位不高的强势股，是我们讨论的重点，其股票换手率高的可信程度较高，表明新资金介入的迹象较为明显，未来的上涨空间相对较大，越是底部换手充分，上行中的抛压越轻。

    # df_volume_ration_00_05_little   #7、量比在0.5一下的缩量情形也值得好好关注，其实严重缩量不仅显示了交易不活跃的表象，同时也暗藏着一定的市场机会。缩量创新高的股票多说是长庄股，缩量能创出新高，说明庄家控盘程度相当高，而且可以排除拉高出货的可能。缩量调整的股票，特别是放量突破某个重要阻力位之后缩量回调的个股，常常是不可多得的买入对象。
    # df_volume_ration_00_10_neuro #涨停板时量比在1以下的股票，上涨空间无可限量，第二天开盘即封涨停的可能性极高。
    # df_volume_ration_08_15_neuro #若某日量比为0.8-1.5,则说明成交量处于正常水平。
    # df_volume_ration_12_15_mild #量比在1.2--2.5之间则为温和放量，如果股价也处于温和缓升状态，则升势相对健康，可继续持股，若股价下跌，则可认定跌势难以在短期内结束，从量的方面判断应可考虑停损退出。
    # df_volume_ration_25_50_obvious #量比在2.5--5，则为明显放量，若股价形影的突破重要支撑或阻力位置，则突破有效的机率颇高，可以相应的采取行动。
    # df_volume_ration_50_100_strong #量比达5--10则为剧烈放量，如果是在个股处于长期低位出现剧烈放量突破长势的后续空间巨大，是“钱”途无量的象征。
    # df_volume_ration_100_more_extreme #某日量比达到10倍以上的股票，一般可以考虑反向操作。在涨势中出现这种情况，说明见顶的可能性压倒一切，即使不是彻底反转，至少涨势会休整相当长一段时间。在股票处于绵绵阴跌的后期，突然出现的巨大量比，说明该股在目前的位置彻底释放了下跌动能。
    # df_volume_ration_200_more_extreme #量比达到20以上的情形基本上每天都有一两单，是极端放量的一种表现，这种情况的反转意义特别强烈，如果在连续的上涨之后，成交量极端放大，但股价出现“滞涨”现象，则是涨势行将死亡的强烈信号。当某只股票在跌势中出现极端放量，则是建仓的大好时机。




### MAIN ####
if __name__ == '__main__':
    date_exam_day = finlib.Finlib().get_last_trading_day() #20200821
#    date_exam_day="20200825" #ryan debug
    logging.info("examine date "+str(date_exam_day))

    rst_dir = '/home/ryan/DATA/result/pv_2'

    if not os.path.isdir(rst_dir):
        os.mkdir(rst_dir)

    rst_dir = '/home/ryan/DATA/result/pv_2/'+str(date_exam_day)
    sl_rst = '/home/ryan/DATA/result/pv_2/latest'

    if not os.path.isdir(rst_dir):
        os.mkdir(rst_dir)

    if os.path.lexists(sl_rst):
        os.unlink(sl_rst)
    os.symlink(rst_dir, sl_rst)
    logging.info(__file__ + " " + "make symbol link " + sl_rst + " --> " + rst_dir)

    step1_output_csv = rst_dir+"/step1.out.csv"
    step1_generate_df(output_csv=step1_output_csv, date_exam_day = date_exam_day)

    # compare between stocks and save result
    step2_ana(pd.read_csv(step1_output_csv, converters={'code': str, 'date': str}, encoding="utf-8"))

    logging.info("script completed!")
