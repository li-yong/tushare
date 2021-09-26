import pandas as pd
import finlib

import finlib_indicator

import tushare as ts
import datetime

import talib
from optparse import OptionParser

import logging
import time
import os
import matplotlib.pyplot as plt

logging.getLogger('matplotlib.font_manager').disabled = True


def prepare_data(days=30):
    ######### For TRIN, Advance/Decline LINE #######\
    dir = "/home/ryan/DATA/DAY_Global/AG_qfq"
    csv = dir+"/ag_all.csv"
    # csv = '/home/ryan/DATA/DAY_Global/AG/ag_all.csv'

    if not finlib.Finlib().is_cached(file_path=csv, day=1):

        print("generating csv from source.")

        cmd1 = "head -1 "+dir+"/SH600519.csv > "+csv
        cmd2 = "for i in `ls "+dir+"/SH*.csv`; do tail -"+str(days)+" $i |grep -vi code >> "+dir+"/ag_all.csv; done"
        cmd3 = "for i in `ls "+dir+"/SZ*.csv`; do tail -"+str(days)+" $i |grep -vi code >> "+dir+"/ag_all.csv; done"

        logging.info(cmd1)
        logging.info(cmd2) # for i in `ls SH*.csv`; do tail -300 $i >> ag_all.csv;done
        logging.info(cmd3) # for i in `ls SZ*.csv`; do tail -300 $i >> ag_all.csv;done

        os.system(cmd1)
        os.system(cmd2)
        os.system(cmd3)
        logging.info("generated "+csv)
    else:
        print("re-using csv as it generated in 1 days. "+csv)


    df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)
    return(df)

def market_adl_trin(df_in, df_out, days):
    ###----------- AG OverAll ------------
    a = df_in['date'].unique()
    a.sort()
    dates = a[-1*days:]

    ADL = 0
    previous_adv = 0
    previous_adv_perc = 0
    previous_vol_perc = 0
    previous_amt_perc = 0
    previous_net_amt = 0

    for d in dates:
        _df = df_in[df_in['date']==d]
        _df_adv = _df[_df['close'] > _df['open']]
        _df_dec = _df[_df['close'] < _df['open']]
        _adv_stocks = _df_adv.__len__()
        _dec_stocks = _df_dec.__len__()
        net_adv = _adv_stocks - _dec_stocks
        net_adv_perc = round((_adv_stocks - _dec_stocks)/_df.__len__(),4)

        if _dec_stocks == 0:
            ADR_Stocks_mkt = _adv_stocks
        else:
            ADR_Stocks_mkt = round(_adv_stocks/_dec_stocks,1)

        net_vol_perc = (_df_adv['volume'].sum() - _df_dec['volume'].sum())/(_df_adv['volume'].sum() + _df_dec['volume'].sum())
        net_amt = _df_adv['amount'].sum() - _df_dec['amount'].sum()
        # print("Debug: net_amt "+str(net_amt))
        amt_sum = _df_adv['amount'].sum() + _df_dec['amount'].sum()
        if net_amt != 0 and amt_sum != 0:
            amt_perc = net_amt/amt_sum
        else:
            amt_perc = 0

        # print("Debug: amt_perc "+str(amt_perc))


        ADL = net_adv + previous_adv
        ADL_perc = round(net_adv_perc + previous_adv_perc,4)
        vol_perc = round(net_vol_perc + previous_vol_perc,4)
        net_amt = round(net_amt + previous_net_amt,4)
        print("Debug: net_amt_acc "+str(net_amt))

        amt_perc = round(amt_perc + previous_amt_perc,4)
        # print("Debug: amt_perc_acc "+str(amt_perc))

        logging.debug("date "+d+", code SH000001 (AG_overall)"
              +", net_adv "+str(net_adv)
              +", PA "+str(previous_adv)
              +", ADL "+str(ADL)
              +", ADL_perc "+str(ADL_perc)
              +", vol_perc "+str(vol_perc)
              +", amt_perc "+str(amt_perc)
              +", amt "+str(net_amt)
              +", ADR_Stocks_mkt "+str(ADR_Stocks_mkt)
              )



        ### TRIN (Arms Index)
        adv_dec_cnt_ratio = round(_adv_stocks / _dec_stocks,2)
        adv_dec_vol_ratio = round(_df_adv['volume'].sum()/_df_dec['volume'].sum(),2)
        adv_dec_amt_ratio = round(_df_adv['amount'].sum()/_df_dec['amount'].sum(),2)

        # https://www.investopedia.com/terms/a/arms.asp
        TRIN = round(adv_dec_cnt_ratio/adv_dec_vol_ratio,2)

        # TRIN = round(adv_dec_amt_ratio/adv_dec_vol_ratio,2)

        logging.debug("date "+d+", code SH000001, ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))

        df_out = df_out.append({'date':d, 'code':'SH000001',
                               # 'net_adv_perc':net_adv_perc,
                                'ADL':ADL,
                                'ADL_perc':ADL_perc,
                                'vol_perc':vol_perc,
                                'amt_perc':amt_perc,
                                'net_amt':net_amt,
                                'TRIN':TRIN,
                                'ADR_Stocks_mkt':ADR_Stocks_mkt,
                                }, ignore_index=True)

        previous_adv = ADL
        previous_adv_perc = ADL_perc
        previous_vol_perc = vol_perc
        previous_amt_perc = amt_perc
        previous_net_amt = net_amt

    return(df_out)

def individual_adl_trin(df_in, df_out, days):
    ###----------- Individual ------------
    df = finlib.Finlib().remove_garbage(df_in, code_field_name='code', code_format='C2D6')

    codes = df['code'].unique()
    codes.sort()

    for c in codes:
        previous_adv = 0
        ADL = 0

        _df = df[df['code']==c]  #_df already has number of 'days' rows.

        if _df.__len__() < days-1:
            print("ignore code "+c+" has no enough bars. expected "+str(days)+", actual "+str(_df.__len__()))
            continue

        _last_date = _df[['date']].iloc[-1].values[0]
        _df_adv = _df[_df['close'] > _df['open']]
        _df_dec = _df[_df['close'] < _df['open']]
        _adv_days = _df_adv.__len__()
        _dec_days = _df_dec.__len__()
        net_adv = _adv_days - _dec_days
        net_adv_perc = round(net_adv/_df.__len__(),4)

        # ADR_B : Advance Decline Ratio (Bars)
        if _dec_days == 0:
            ADR_B = _adv_days
        else:
            ADR_B = round(_adv_days/_dec_days, 1)

        ADL = net_adv
        ADL_perc = round(net_adv/_df.__len__(), 4)
        logging.debug("date "+_last_date+", code "+c+", net_adv_perc "+str(net_adv_perc)+", PA "+str(previous_adv)+", ADL "+str(ADL)+", ADL_perc "+str(ADL_perc)+", ADR_B "+str(ADR_B))


        ### TRIN (Arms Index)
        if _dec_days == 0: # no decling days, very strong, set TRIN=0
            adv_dec_cnt_ratio = 0
            adv_dec_vol_ratio = 0
            adv_dec_amt_ratio = 0
            TRIN = 0
        else:
            adv_dec_cnt_ratio = round(_adv_days / _dec_days,2)
            adv_dec_vol_ratio = round(_df_adv['volume'].sum()/_df_dec['volume'].sum(),2)
            adv_dec_amt_ratio = round(_df_adv['amount'].sum()/_df_dec['amount'].sum(),2)

            TRIN = round(adv_dec_amt_ratio/adv_dec_vol_ratio,2)

        logging.debug("date "+_last_date+", code "+c+", ad_cnt_ratio "+str(adv_dec_cnt_ratio)+", ad_vol_ratio "+str(adv_dec_vol_ratio)+", ad_amt_ratio "+str(adv_dec_amt_ratio) +
              ", TRIN "+str(TRIN))



        # df_out = df_out.append({'date':d, 'code':'SH000001',
        #                        # 'net_adv_perc':net_adv_perc,
        #                         'ADL':ADL,
        #                         'ADL_perc':ADL_perc,
        #                         'vol_perc':vol_perc,
        #                         'amt_perc':amt_perc,
        #                         'net_amt':net_amt,
        #                         'TRIN':TRIN}, ignore_index=True)


        df_out = df_out.append({'date':_last_date,
                                'code':c,
                                'ADR_B':ADR_B,
                                #'net_adv_perc':net_adv_perc,
                                'ADL':ADL,
                                'ADL_perc':ADL_perc,
                                'vol_perc': round((_df_adv['volume'].sum() - _df_dec['volume'].sum())/(_df_adv['volume'].sum() + _df_dec['volume'].sum()),4),
                                'amt_perc': round((_df_adv['amount'].sum() - _df_dec['amount'].sum())/(_df_adv['amount'].sum() + _df_dec['amount'].sum()),4),
                                'net_amt':_df_adv['amount'].sum() - _df_dec['amount'].sum(),
                                'TRIN':TRIN}, ignore_index=True)

    return(df_out)

def _calc_adr_bar(chg_list):
    adr_bar = 0
    adv=0
    dec=0
    for chg in chg_list:
        if chg > 0:
            adv += 1
        else:
            dec += 1

    if dec > 0:
        adr_bar = round(adv/dec,1)
    else:
        adr_bar = adv

    return(adr_bar)

def _add_adr(df, verify_on_col, days):
    df['adr_bar'] = df[[verify_on_col]].rolling(window=days).apply(func=_calc_adr_bar)
    return(df)


def show_result(type, csv_out,root_dir,show_plot=False):

    df_out = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_out)
    logging.info("loading df from " + csv_out)

    if type == "market":
        # 'date', 'code', 'net_adv_perc', 'ADL', 'ADL_perc', 'vol_perc', 'amt_perc', 'net_amt', 'TRIN', 'ADR_Stocks_mkt'
        logging.info("\ntop 5 vol_perc stocks today:\n"+finlib.Finlib().pprint(df_out[-10:]))
        pass
    elif type == "index":
        df = df_out[['code','date','close','adr_bar']]
        df['date'] = pd.to_datetime(df_out['date'].values, format='%Y-%m-%d')
        df = df.set_index('date')

        # plt.tight_layout()
        if show_plot:
            ax = df.plot(subplots=True, sharex=True, grid=False)
            #
            #
            # fn = root_dir+"/adl_trin_plot_ag_index_plot"
            # ax.get_figure().savefig(fn)
            # logging.info("figure saved to " + fn + "\n")

    elif type == 'individual':
        df_individual = df_out[df_out['code'] != 'SH000001'].reset_index().drop('index', axis=1)

        df_tmp = df_individual.sort_values('ADL_perc', ascending=False, inplace=False)[0:5]
        logging.info("\ntop 5 ADL_perc stocks today:\n"+finlib.Finlib().pprint(df_tmp))
        csv_tmp = root_dir+"/adl_perc_top_5_stocks.csv"
        df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
        logging.info("saved to "+csv_tmp)

        df_tmp = df_individual.sort_values('vol_perc', ascending=False, inplace=False)[0:5]
        logging.info("\ntop 5 vol_perc stocks today:\n"+finlib.Finlib().pprint(df_tmp))
        csv_tmp = root_dir+"/vol_perc_top_5_stocks.csv"
        df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
        logging.info("saved to "+csv_tmp)


        df_tmp = df_individual.sort_values('amt_perc', ascending=False, inplace=False)[0:5]
        logging.info("\ntop 5 amt_perc stocks today:\n"+finlib.Finlib().pprint(df_tmp))
        csv_tmp = root_dir+"/amt_perc_top_5_stocks.csv"
        df_tmp.to_csv(csv_tmp, encoding='UTF-8', index=False)
        logging.info("saved to "+csv_tmp)


def check_adr_individual(days):
    df = prepare_data(days=days*2)
    df = finlib.Finlib().add_stock_name_to_df(df=df)

    df = finlib.Finlib().remove_garbage(df=df)

    codes = df['code'].unique()
    codes.sort()
    df_profit_report = pd.DataFrame()
    df_si_cha_report = pd.DataFrame()
    df_jin_cha_report = pd.DataFrame()


    for c in codes:
        df_sub = df[df['code']==c]
        df_sub = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_sub, short=4, middle=27, long=60)

        df_out = _add_adr(df=df_sub, verify_on_col='pct_chg', days=days)

        df_si_cha, df_jin_cha = finlib_indicator.Finlib_indicator().single_column_across(df=df_out, col_name='adr_bar', threshod=1)

        if df_si_cha.__len__() > 0:
            df_si_cha_report = df_si_cha_report.append(df_si_cha)

            today_adr_sicha = df_si_cha[
                df_si_cha['date'] == finlib.Finlib().get_last_trading_day()].reset_index().drop('index', axis=1)

            today_adr_sicha = today_adr_sicha[today_adr_sicha['close_4_sma'] > today_adr_sicha['close_27_sma']]

            if today_adr_sicha.__len__()>0:
                logging.info(finlib.Finlib().pprint(today_adr_sicha))
                logging.info("WOO, buy candidate")


        if df_jin_cha.__len__() > 0:
            df_jin_cha_report = df_jin_cha_report.append(df_jin_cha)

            today_adr_jincha = df_jin_cha_report[
                df_jin_cha_report['date'] == finlib.Finlib().get_last_trading_day()].reset_index().drop('index', axis=1)

            today_adr_jincha = today_adr_jincha[today_adr_jincha['close_4_sma'] < today_adr_jincha['close_27_sma']]

            if today_adr_jincha.__len__()>0:
                logging.info(finlib.Finlib().pprint(today_adr_jincha))
                logging.info("WOO, sell candidate")



        # evaluate strategy performance.
        # result show buy or sicha, sell on jincha is much better
        if df_jin_cha.__len__() > 0 and df_si_cha.__len__() > 0:
            # df_profit_sub = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_jin_cha,df_si_cha=df_si_cha)
            df_profit_sub = finlib_indicator.Finlib_indicator()._calc_jin_cha_si_cha_profit(df_jin_cha=df_si_cha,df_si_cha=df_jin_cha)
            logging.info(" " + finlib.Finlib().pprint(df=df_profit_sub))
            df_profit_report = df_profit_report.append(df_profit_sub)

    today_adr_sicha[today_adr_sicha['close_4_s']]

    pass

def main():
    parser = OptionParser()

    parser.add_option("-n", "--exam_days", type="int", dest="exam_days", default=14,
                      help="the number of the latest N days to be checked.")


    parser.add_option("-s","--show_plot", action="store_true", dest="show_plot", default=False,
                       help="show market index plot")

    (options, args) = parser.parse_args()


    days =options.exam_days
    show_plot =options.show_plot
    root_dir = "/home/ryan/DATA/result/adl"
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    check_adr_individual(days=days)

    ################################
    # Index ADR
    # SH000001 ADR of the last 14 days
    ################################
    csv_out_index = root_dir+'/ag_idx_adl_trin_'+str(days)+'d.csv'

    df_index = finlib.Finlib().regular_read_csv_to_stdard_df(
        data_csv='/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv')
    df_index = finlib.Finlib().ts_code_to_code(df_index)

    df_index = df_index.tail(days*2).reset_index().drop('index', axis=1)

    df_out = _add_adr(df=df_index, verify_on_col='pct_chg',days=days)
    df_out.to_csv(csv_out_index, encoding='UTF-8', index=False)
    df_sicha,df_jincha=finlib_indicator.Finlib_indicator().single_column_across(df=df_out,col_name='adr_bar',threshod=1)
    logging.info("AG Index ADL saved to " + csv_out_index)
    show_result(type="index", csv_out=csv_out_index, root_dir=root_dir, show_plot=show_plot)

    ################################
    # Market Overview ADR
    # overview of the market, count ADR: ratio of adv_stock_#/dec_stock_#
    ################################
    csv_out_mkt = root_dir+'/ag_mkt_adl_trin_'+str(days)+'d.csv'

    if not finlib.Finlib().is_cached(csv_out_mkt,1):
        df_in = prepare_data(days=days*2)
        df_out = pd.DataFrame( columns=['date','code','net_adv_perc','ADL','ADL_perc','vol_perc', 'amt_perc','net_amt','TRIN'])

        df_out = market_adl_trin(df_in=df_in, df_out=df_out, days=days)
        df_out.to_csv(csv_out_mkt, encoding='UTF-8', index=False)
        logging.info("AG Market ADL, TRIN saved to "+csv_out_mkt)

    show_result(type="market", csv_out=csv_out_mkt, root_dir=root_dir, show_plot=show_plot)

    ################################
    # Individual ADR
    # individual stock ADR of today
    ################################
    csv_out_individual = root_dir+'/ag_ind_adl_trin_'+str(days)+'d.csv'

    if not finlib.Finlib().is_cached(csv_out_individual, 1):
        df_in = prepare_data(days=days*2)
        df_out = pd.DataFrame(
            columns=['date', 'code', 'net_adv_perc', 'ADL', 'ADL_perc', 'vol_perc', 'amt_perc', 'net_amt', 'TRIN'])
        df_out = individual_adl_trin(df_in = df_in, df_out=df_out, days=days) #comment the line if don't need run on each invididual stocks
        df_out = finlib.Finlib().add_stock_name_to_df(df=df_out, ts_pro_format=False)
        df_out.to_csv(csv_out_individual, encoding='UTF-8', index=False)
        logging.info("Individual stock appended to "+csv_out_individual)

    show_result(type="individual", csv_out=csv_out_individual, root_dir=root_dir, show_plot=show_plot)

### MAIN ####
if __name__ == '__main__':
    main()
