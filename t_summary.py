# coding: utf-8
# encoding= utf-8

import sys

#import imp #not compatible with python3
#imp.reload(sys) #not compatible with python3
#sys.setdefaultencoding('utf-8') #not compatible with python3

import pandas as pd
import re
import itertools
import os
import finlib
import tushare as ts
from sqlalchemy import create_engine
import mysql.connector

from datetime import datetime, timedelta
from optparse import OptionParser
import logging
from tabulate import tabulate

global stock_global


def refine_df(input_df, has_db_record=False, force_pass=False, force_pass_open_reason='', insert_buy_record_to_db=False, debug=False):
    #the function re-organize ptn hit result from code view, reduce the result from 1000+ to 10~100.
    #has_db_record: db has the ptn perf record.
    #force_pass: when inserting df_ann, that insert db even db doesn't have the ptn perf records.
    #force_pass_open_reason: must be specified if force_pass =True

    ####if a code has hit _pvbreak_ pattern,  then the code will be insert to db and in return df as one record.<<<removed
    #input_df = input_df.drop('index', axis=1).drop_duplicates().reset_index().drop('index', axis=1)

    if debug:
        return (input_df)

    if input_df.__len__() == 0:
        return (input_df)

    if 'index' in input_df.columns:
        input_df = input_df.drop('index', axis=1)

    if 'Unnamed: 0' in input_df.columns:
        input_df = input_df.drop('Unnamed: 0', axis=1)

    if 'level_0' in input_df.columns:
        input_df = input_df.drop('level_0', axis=1)

    input_df = input_df.drop_duplicates().reset_index().drop('index', axis=1)
    #print(input_df)

    #d = input_df['code'].drop_duplicates()
    #d = d.reset_index().drop('index', axis=1)
    #d = input_df

    logging.info(__file__+" "+"function refine_df, input df records " + str(input_df.__len__()) + " total stocks in the list: " + str(input_df.__len__()))

    #df_rtn = pd.DataFrame(columns=input_df.columns).rename(columns={'op': 'hit_ptn_cnt'}).drop('op_rsn', axis=1)
    df_rtn = pd.DataFrame(columns=input_df.columns).rename(columns={'op': 'hit_ptn_cnt'})

    # engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
    # mysql_host = '127.0.0.1'
    # cnx = mysql.connector.connect(host=mysql_host, user='root', password='admin888.@_@', database="ryan_stock_db")
    # cursor = cnx.cursor()  # setup db connection before for loop.

    for i in input_df['code'].unique():
        code = i
        #code = i[1][0]
        # print(code)

        # if code == 'SZ300607':
        #    pass

        c = input_df.loc[input_df['code'] == code]

        if c.__len__() > 0:
            #if c.__len__() > 0 and has_db_record:
            #if c.__len__() > 1:
            #    pass
            #print("run into debug")

            ptn_list = ''

            ptn_list = '\',\''.join(c['op_rsn'].tolist())
            #print("ptn_list_0: " + ptn_list)

            if re.match("^\'.*", ptn_list):
                #print("replacing head \'")
                ptn_list = re.sub("^\'", '', ptn_list)

            if re.match(".*\'$", ptn_list):
                #print("replacing tail \'")
                ptn_list = re.sub("\'$", '', ptn_list)

            ptn_list = '\'' + ptn_list + '\''

            #print("ptn_list: "+ptn_list)

            avg2 = avg5 = sum2uc = sum2dc = sum5uc = sum5dc = 0

            # if has_db_record and insert_buy_record_to_db:  # AG only
            #     sql = "SELECT AVG(`2mea`) as avg2mea, AVG(`5mea`) as avg5mea," \
            #           " sum(`2uc`) as sum2uc , sum(`2dc`) as sum2dc , sum(`5uc`) as sum5uc, sum(`5dc`) as sum5dc " \
            #           " FROM `pattern_perf` WHERE `pattern` in ("+ptn_list+')'
            #     ptn_perf = pd.read_sql_query(sql, engine)
            #
            #     avg2 = ptn_perf.iloc[0]['avg2mea']
            #     avg5 = ptn_perf.iloc[0]['avg5mea']
            #     sum2uc = ptn_perf.iloc[0]['sum2uc']
            #     sum2dc = ptn_perf.iloc[0]['sum2dc']
            #     sum5uc = ptn_perf.iloc[0]['sum5uc']
            #     sum5dc = ptn_perf.iloc[0]['sum5dc']
            #
            #     if force_pass:
            #         pass
            #     #elif ('_pvbreak_' in ptn_list ): ## REMOVED, HAS df_pv_break
            #     # pass #
            #     elif ('None' == str(avg2)) or ('None' == str(avg5)):
            #         #if ('None' == str(avg2)) or ('None' == str(avg5)):
            #         logging.info('None of the patterns in the DB')
            #         continue
            #     elif (avg2 > 0.02) and (avg5 > 0.02) and (sum2uc > sum2dc * 3) and (sum5uc > sum5dc * 3):
            #         pass  #Don't stop, continue to running into codes below.
            #     else:
            #         logging.info(__file__+" "+"not qualified to buy")
            #         continue

        c_code = str(c['code'].values[0])
        c_name = str(c['name'].values[0].encode("utf-8"))
        c_close_p = c['close_p'].values[0]

        if force_pass:
            c_op_rsn = force_pass_open_reason
        else:
            c_op_rsn = 'refine_df_func. ' + ','.join(c['op_rsn'].values.tolist())

        c_date = str(c['date'].values[0])
        c_status = 'OPEN'

        if pd.isnull(c_close_p) or float(c_close_p) == 0.0:
            logging.info(__file__+" "+"not processing as c_close_p is 0. " + c_code + " " + c_name + " " + c_date)
            continue

        # #
        # if insert_buy_record_to_db:
        #     sql = 'SELECT * FROM `order_tracking_stock` WHERE `code` =\'' + c_code + '\' AND `status`=\'OPEN\''
        #     open_stocks = pd.read_sql_query(sql, engine)
        #     db_tbl = 'order_tracking_stock'
        #
        #     if open_stocks.__len__() > 0:
        #         logging.info(__file__+" "+"Have OPEN order on code " + c_code + ", +1 its buy_cnt")
        #         exist_buy_reason = open_stocks['buy_reason'].values[0]
        #         exist_buy_cnt = open_stocks['buy_cnt'].values[0]
        #
        #         update_sql = ("UPDATE `" + db_tbl + "`  " "SET buy_cnt = %(buy_cnt)s, buy_reason = %(buy_reason)s " " WHERE `id` =  %(id)s ")
        #         data_sql = {}
        #         data_sql['buy_cnt'] = int(exist_buy_cnt + 1)
        #         bs = exist_buy_reason + "; " + c_date + ":" + c_op_rsn
        #         data_sql['buy_reason'] = bs[0:3199]  #var char 3200, length limit
        #         data_sql['id'] = int(open_stocks['id'].values[0])
        #
        #         cursor.execute(update_sql, data_sql)
        #         cnx.commit()
        #
        #         #continue
        #     else:
        #         logging.info('buy candidate,' + c_code + ' ' + c_name + ' ' + c_close_p + ' ' + c_date)
        #
        #         update_sql = ("INSERT INTO `" + db_tbl + "`  " "SET code = %(code)s, name = %(name)s, buy_date = %(buy_date)s, " " buy_price = %(buy_price)s,  buy_cnt = %(buy_cnt)s, buy_reason = %(buy_reason)s, h_ptn_cnt = %(h_ptn_cnt)s, status = %(status)s ")
        #
        #         data_sql = {}
        #         data_sql['code'] = c_code
        #         data_sql['name'] = c_name
        #         data_sql['buy_date'] = c_date
        #         data_sql['buy_price'] = c_close_p
        #         data_sql['buy_cnt'] = 1
        #         data_sql['buy_reason'] = c_op_rsn
        #         data_sql['status'] = c_status
        #         data_sql['h_ptn_cnt'] = c['op_rsn'].__len__()
        #         #print("update_sql "+update_sql)
        #         #print("name "+c_name)
        #
        #         cursor.execute(update_sql, data_sql)
        #         cnx.commit()

        try:
            op_strength_group_sum = pd.to_numeric(c['op_strength']).sum()
            #print('buy candidate,' + c['code'].values[0] + ' ' + c['name'].values[0] + ', hit_cnt ' + str(
            #    c.__len__()) + ', avg2 ' + str(avg2) + ", avg5 " + str(avg5)+', op_strength '+str(op_strength_group_sum))

        except:
            #print("exception on op_strength " + c[0:1]['code'].values[0])
            op_strength_group_sum = 0
            #for i in c['op_strength'].iteritems():#python2
            for i in c['op_strength'].items():  #python3
                strength = i[1]
                if strength.find(',') >= -1:
                    strength = strength.split(',')[0]

                op_strength_group_sum += float(strength)
            logging.info(__file__+" "+"re-calc op_strength_group_sum to " + str(op_strength_group_sum) + ", " + c_code + " " + c_name)

        if has_db_record and False:
            df_rtn = df_rtn.append({
                "code": c[0:1]['code'].values[0],
                "name": c[0:1]['name'].values[0],
                "date": c[0:1]['date'].values[0],
                "close_p": c[0:1]['close_p'].values[0],
                "op_strength": op_strength_group_sum,
                "hit_ptn_cnt": c.__len__(),
                "op_rsn": ptn_list,
                "2mea": pd.to_numeric(c['2mea']).mean(),
                "5mea": pd.to_numeric(c['5mea']).mean(),
                "10mea": pd.to_numeric(c['10mea']).mean(),
                "20mea": pd.to_numeric(c['20mea']).mean(),
                "60mea": pd.to_numeric(c['60mea']).mean(),
                "120mea": pd.to_numeric(c['120mea']).mean(),
                "2uc": int(pd.to_numeric(c['2uc']).sum()),
                "2dc": int(pd.to_numeric(c['2dc']).sum()),
                "5uc": int(pd.to_numeric(c['5uc']).sum()),
                "5dc": int(pd.to_numeric(c['5dc']).sum()),
                "10uc": int(pd.to_numeric(c['10uc']).sum()),
                "10dc": int(pd.to_numeric(c['10dc']).sum()),
                "20uc": int(pd.to_numeric(c['20uc']).sum()),
                "20dc": int(pd.to_numeric(c['20dc']).sum()),
            }, ignore_index=True)
        else:
            df_rtn = df_rtn.append({
                "code": c[0:1]['code'].values[0],
                "name": c[0:1]['name'].values[0],
                "date": c[0:1]['date'].values[0],
                "close_p": c[0:1]['close_p'].values[0],
                "op_strength": op_strength_group_sum,
                "hit_ptn_cnt": c.__len__(),
                "op_rsn": ptn_list,
                "2mea": avg2,
                "5mea": avg5,
            }, ignore_index=True)

    #
    # engine.dispose()  #close db connection after for loop.
    # cursor.close()
    # cnx.close()
    # print(df_rtn)
    if 'index' in df_rtn.columns:
        df_rtn = df_rtn.drop('index', axis=1)

    if 'level_0' in df_rtn.columns:
        df_rtn = df_rtn.drop('level_0', axis=1)

    if 'Unnamed: 0' in df_rtn.columns:
        df_rtn = df_rtn.drop('Unnamed: 0', axis=1)

    df_rtn = df_rtn.sort_values(by=['hit_ptn_cnt', 'op_strength'], ascending=[False, False]).reset_index().drop('index', axis=1)
    return (df_rtn)


def my_sort(df, debug=False):

    if debug:
        return (df)

    if df.__len__() == 0:
        return df
    else:
        #logging.info(df.head(1))
        pass

    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1, inplace=False)

    finlib.Finlib().pprint(df)
    df = df.drop_duplicates().reset_index()

    if 'index' in df.columns:
        df = df.drop('index', axis=1)

    by_str = ''
    ascending_str = ''

    for k in ('hit_ptn_cnt', 'op_strength', '2mea'):
        if df.columns.tolist().__contains__(k):
            by_str += '\'' + k + '\','
            ascending_str += "False,"
    cmd = "df.sort_values(by=[" + by_str + "], ascending=[" + ascending_str + "], inplace=False)"

    if by_str.__len__() > 0 and df.__len__() > 0:
        df = eval(cmd)

    if 'code_x' in df.columns:
        df = df.rename(columns={"code_x": "code"}, inplace=False)

    if 'name_x' in df.columns:
        df = df.rename(columns={"name_x": "name"}, inplace=False)

    if 'close_p_x' in df.columns:
        df = df.rename(columns={"close_p_x": "close_p"}, inplace=False)

    if 'date_x' in df.columns:
        df = df.rename(columns={"date_x": "date"}, inplace=False)

    if 'index' in df.columns:
        df = df.drop('index', axis=1, inplace=False)

    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1, inplace=False)

    if 'name_y' in df.columns:
        df = df.drop('name_y', axis=1, inplace=False)

    if 'result_value_quarter_fundation_y' in df.columns:
        df = df.drop('result_value_quarter_fundation_y', axis=1, inplace=False)

    if 'esp_y' in df.columns:
        df = df.drop('esp_y', axis=1, inplace=False)

    if 'pe_y' in df.columns:
        df = df.drop('pe_y', axis=1, inplace=False)

    if 'profit_y' in df.columns:
        df = df.drop('profit_y', axis=1, inplace=False)

    #if 'hit_ptn_cnt_y' in df.columns: #KEEP THIS
    # df_rtn = df.drop('hit_ptn_cnt_y', axis=1)

    if '2mea_y' in df.columns:
        df = df.drop('2mea_y', axis=1, inplace=False)

    if '5mea_y' in df.columns:
        df = df.drop('5mea_y', axis=1, inplace=False)

    if 'close_p_y' in df.columns:
        df = df.drop('close_p_y', axis=1, inplace=False)

    if 'date_y' in df.columns:
        df = df.drop('date_y', axis=1, inplace=False)

    return df


def remove_unwant_columns(df):
    if 'index' in df.columns:
        df = df.drop('index', axis=1)
    if 'level_0' in df.columns:
        df = df.drop('level_0', axis=1)
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis=1)
    return (df)


def combin_filter(df, post_combine=False, debug=False):

    if debug:
        return (df)

    df = remove_unwant_columns(df)

    cols = []
    keep_df_cols = [
        'code',
        'name',
        'date',
        'op_rsn',
        'op_strength',
        'close_p',
        'result_value_sum',
        'scoreA',
        'scoreAB',
        'score_over_years',
        'score_avg',
        'number_in_top_30',
        'result_value_today',
        'result_value_quarter_fundation',
        'score_sum',
        'result_value_2',
        'report_date',
        'roe',
        'year_quarter',
        'ps',
        'pe',
        'peg_1',
        'peg_4',
        'pe',
        'pb',
        'current_date',
        'current_close',
        'up_cnt',
        'down_cnt',
        'perc_to_up',
        'perc_to_down',
        'up_p',
        'down_p',
        'day_to_up',
        'day_to_down',
        'long_enter',
        'long_quit',
        'long_expect_ear_perct',
        'delta_to_long_enter',
        'pre_date',
        'ann_date',
        'actual_date',
        'long_take_profit_percent',
        'long_stop_lost_percent',
        'hit_sum',
        'h_cnt',
        'l_cnt',
        'o_cnt',
        'c_cnt',
        'p_max',
        'p_min',
    ]

    for col in df.columns:
        if post_combine:
            if not re.match('.*_y[0-9]+', col):  #remove duplicate name *_y0 from combined df
                cols.append(col)
        else:
            if col in keep_df_cols:
                cols.append(col)

    if 'date' in cols:
        cols.insert(0, cols.pop(cols.index('date')))

    if 'name' in cols:
        cols.insert(0, cols.pop(cols.index('name')))

    if 'code' in cols:
        cols.insert(0, cols.pop(cols.index('code')))

    df = df[cols]
    return df


def generate_result_csv(full_combination=False, select=True, debug=False):
    #cons = ts.get_apis()
    #ts.close_apis(cons)  #ryan add 03/15, not sure if it impacts script running.
    today_d = datetime.today().strftime('%d')

    exam_date = finlib.Finlib().get_last_trading_day()
    #exam_date="2018-03-05"

    if select:
        rpt = "/home/ryan/DATA/result/report_" + exam_date + "_selected.txt"
    else:
        rpt = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + ".txt"
    rst = "\nGetting today's summary report " + exam_date + "\n\n"  #the string contains the report content.
    fh = open(rpt, "w")
    fh.write(rst)
    fh.close()

    logging.info(rst)

    pd.set_option("display.max_rows", 9999)
    pd.set_option("display.max_columns", 100)
    pd.set_option('expand_frame_repr', False)

    # f_fenghong = '/home/ryan/DATA/result/fenghong_score.csv'

    base = '/home/ryan/DATA/result/today'

    f_ann = base + "/" + "announcement.csv"
    f_p_m_div = base + "/" + "price_mfi_div.csv"
    f_t_p = base + "/" + "talib_pattern.csv"
    f_fund = base + "/" + "fundamentals.csv"
    f_moneyflow = base + "/" + "moneyflow.csv"

    f_moneyflow_top_amt_perc = "/home/ryan/DATA/result/today/mf_today_top5_large_amount.csv"
    f_moneyflow_top_amt_perc_selected = "/home/ryan/DATA/result/selected/mf_today_top5_large_amount.csv"



    #f_fund="/home/ryan/DATA/result/latest_fundamental_quarter.csv"

    #fundermental result based on tushare.pro, generated by t_daily_fundamentals_2.py -a
    #f_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step5/multiple_years_score.csv"
    #f_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step8/multiple_years_score_selected.csv"
    f_fund_2 = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/step6/multiple_years_score_selected.csv"

    f_pe_pb_roe_history = "/home/ryan/DATA/result/high_roe_top30.csv"

    f_p_r_div = base + "/" + "price_rsi_div.csv"
    #f_lanchou=base+"/"+"lanchou.csv"

    if not select:
        f_pv_db_buy_filter = base + "/" + "talib_and_pv_db_buy_filtered_" + stock_global + ".csv"
        f_pv_db_sell_filter = base + "/" + "talib_and_pv_db_sell_filtered_" + stock_global + ".csv"
        f_pv_no_filter = base + "/" + "talib_and_pv_no_db_filter_" + stock_global + ".csv"

    f_industry_top = "/home/ryan/DATA/result/industry_top.csv"
    f_area_top = "/home/ryan/DATA/result/area_top.csv"

    f_fund_peg_ps = "/home/ryan/DATA/result/latest_fundamental_peg_selected.csv"

    f_hkhs_index = "/home/ryan/DATA/pickle/INDEX_US_HK/hkhs.csv"
    f_sp500_index = "/home/ryan/DATA/pickle/INDEX_US_HK/sp500.csv"

    f_sp400_index = "/home/ryan/DATA/pickle/INDEX_US_HK/sp400.csv"
    f_dow_index = "/home/ryan/DATA/pickle/INDEX_US_HK/dow.csv"
    f_nasdqa100_index = "/home/ryan/DATA/pickle/INDEX_US_HK/nasdqa100.csv"
    #ag fib
    f_fib_ag = "/home/ryan/DATA/result/ag_fib.csv"

    #ag_index_fib
    f_fib_ag_index_selected = "/home/ryan/DATA/result/selected/ag_index_fib.csv"

    #ag fib selected
    f_fib_ag_selected = "/home/ryan/DATA/result/selected/ag_fib.csv"

    #us_index selected
    f_fib_us_index_selected = "/home/ryan/DATA/result/selected/us_index_fib.csv"

    #ag selected
    f_key_points_ag_selected = "/home/ryan/DATA/result/key_points_ag_today_selected.csv"
    f_pv_no_filter_ag_selected = "/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_ag.csv"
    f_fib_ag_selected = "/home/ryan/DATA/result/selected/ag_fib.csv"

    #us selected
    f_key_points_us_selected = "/home/ryan/DATA/result/key_points_us_today_selected.csv"
    f_pv_no_filter_us_selected = "/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_us.csv"
    f_fib_us_selected = "/home/ryan/DATA/result/selected/us_fib.csv"

    #hk selected
    f_key_points_hk_selected = "/home/ryan/DATA/result/key_points_hk_today_selected.csv"
    f_pv_no_filter_hk_selected = "/home/ryan/DATA/result/selected/talib_and_pv_no_db_filter_hk.csv"
    f_fib_hk_selected = "/home/ryan/DATA/result/selected/hk_fib.csv"

    #key_points_AG_today_selected.csv
    if not select:
        f_support_resist_line_today = "/home/ryan/DATA/result/key_points_" + str(stock_global).lower() + "_today_selected.csv"

    #disclosure_date in a week.
    f_disclosure_date_notify = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/source/latest/disclosure_date_notify.csv"

    #macd and kdj
    f_macd_m = "/home/ryan/DATA/result/macd_selection_M.csv"
    f_macd_w = "/home/ryan/DATA/result/macd_selection_W.csv"
    f_macd_d = "/home/ryan/DATA/result/macd_selection_D.csv"

    f_kdj_m = "/home/ryan/DATA/result/kdj_selection_M.csv"
    f_kdj_w = "/home/ryan/DATA/result/kdj_selection_W.csv"
    f_kdj_d = "/home/ryan/DATA/result/kdj_selection_D.csv"

    f_hsgt = "/home/ryan/DATA/result/hsgt_top_10_selected.csv"

    #fibonacci
    #f_fib_ag = "/home/ryan/DATA/result/ag_fib.csv" #previous defined same variable and value
    f_fib_index = "/home/ryan/DATA/result/selected/ag_index_fib.csv"

    #concept_top
    f_concept_top = "/home/ryan/DATA/result/concept_top.csv"

    arr = []

    #df_pv_db_buy_filter = pd.DataFrame(columns=['code'])
    #df_pv_no_filter = pd.DataFrame(columns=['code'])

    #len_df_pv_db_buy_filter_0 = 0
    #len_df_pv_db_sell_filter_0 = 0

    ##################################################
    # AG: load df
    ##################################################
    if select:
        if finlib.Finlib().is_cached(f_fib_ag_index_selected, day=2):
            df_fib_ag_index_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_ag_index_selected)
            arr.append('df_fib_ag_index_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_ag_index_selected)

        if finlib.Finlib().is_cached(f_fib_ag_selected, day=2):
            df_fib_ag_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_ag_selected)
            arr.append('df_fib_ag_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_ag_selected)

        if finlib.Finlib().is_cached(f_fib_us_index_selected, day=2):
            df_fib_us_index_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_us_index_selected)
            arr.append('df_fib_us_index_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_us_index_selected)
#---
        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_yunxian_sell.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_sell_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_sell_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_yunxian_buy.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_buy_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_buy_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)


        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_duotou_pailie.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_duotou_pailie_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_duotou_pailie_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_jincha_minor.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_minor_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_minor_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_jincha_major.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_major_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_major_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)


        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_very_strong_up_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_up_trend_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_up_trend_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/ag_junxian_barstyle_very_strong_down_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_down_trend_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_down_trend_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_)

#us
        # ---
        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_yunxian_sell.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_sell_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_sell_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_yunxian_buy.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_buy_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_buy_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_duotou_pailie.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_duotou_pailie_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_duotou_pailie_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_jincha_minor.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_minor_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_minor_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_jincha_major.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_major_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_major_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_very_strong_up_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_up_trend_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_up_trend_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/us_junxian_barstyle_very_strong_down_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_down_trend_selected_us = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_down_trend_selected_us')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        #hk
        # ---
        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_yunxian_sell.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_sell_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_sell_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_yunxian_buy.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_yunxian_buy_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_yunxian_buy_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_duotou_pailie.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_duotou_pailie_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_duotou_pailie_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_jincha_minor.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_minor_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_minor_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_jincha_major.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_jincha_major_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_jincha_major_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_very_strong_up_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_up_trend_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_up_trend_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        f_ = "/home/ryan/DATA/result/selected/hk_junxian_barstyle_very_strong_down_trend.csv"
        if finlib.Finlib().is_cached(f_, day=2):
            df_very_strong_down_trend_selected_hk = finlib.Finlib().regular_read_csv_to_stdard_df(f_)
            arr.append('df_very_strong_down_trend_selected_hk')
        else:
            logging.info(__file__ + " " + "NOT found file " + f_)

        ########## us
        if finlib.Finlib().is_cached(f_key_points_us_selected, day=2):
            df_key_points_us_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_key_points_us_selected)
            arr.append('df_key_points_us_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_key_points_us_selected)

        if finlib.Finlib().is_cached(f_pv_no_filter_us_selected, day=2):
            df_pv_no_filter_us_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_no_filter_us_selected)
            arr.append('df_pv_no_filter_us_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_pv_no_filter_us_selected)

        if finlib.Finlib().is_cached(f_fib_us_selected, day=2):
            df_fib_us_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_us_selected)
            arr.append('df_fib_us_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_us_selected)

        ######## hk
        if finlib.Finlib().is_cached(f_key_points_hk_selected, day=2):
            df_key_points_hk_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_key_points_hk_selected)
            arr.append('df_key_points_hk_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_key_points_hk_selected)

        if finlib.Finlib().is_cached(f_pv_no_filter_hk_selected, day=2):
            df_pv_no_filter_hk_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_no_filter_hk_selected)
            arr.append('df_pv_no_filter_hk_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_pv_no_filter_hk_selected)

        if finlib.Finlib().is_cached(f_fib_hk_selected, day=2):
            df_fib_hk_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_hk_selected)
            arr.append('df_fib_hk_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_hk_selected)

        ######## ag
        if finlib.Finlib().is_cached(f_key_points_ag_selected, day=2):
            df_key_points_ag_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_key_points_ag_selected)
            arr.append('df_key_points_ag_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_key_points_ag_selected)

        if finlib.Finlib().is_cached(f_pv_no_filter_ag_selected, day=2):
            df_pv_no_filter_ag_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_no_filter_ag_selected)
            arr.append('df_pv_no_filter_ag_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_pv_no_filter_ag_selected)

        if finlib.Finlib().is_cached(f_fib_ag_selected, day=2):
            df_fib_ag_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_ag_selected)
            arr.append('df_fib_ag_selected')
        else:
            logging.info(__file__+" "+"NOT found file " + f_fib_ag_selected)

    elif stock_global in ['AG']:
        logging.info(__file__+" "+"loading  df_pv_no_filter, " + f_pv_no_filter)
        if (os.path.isfile(f_pv_no_filter)) and os.stat(f_pv_no_filter).st_size >= 10:  # > 10 bytes
            df_pv_no_filter = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_no_filter)
            df_pv_no_filter.drop_duplicates(inplace=True)
            df_pv_no_filter = finlib.Finlib().remove_garbage(df_pv_no_filter, code_field_name='code', code_format='C2D6')
            arr.append('df_pv_no_filter')
        else:
            logging.info(__file__+" "+"NOT found file " + f_pv_no_filter)

        if 'df_pv_no_filter' in locals():
            if 'index' in df_pv_no_filter.columns:
                df_pv_no_filter = df_pv_no_filter.drop('index', axis=1)
            if 'level_0' in df_pv_no_filter.columns:
                df_pv_no_filter = df_pv_no_filter.drop('level_0', axis=1)
            if 'Unnamed: 0' in df_pv_no_filter.columns:
                df_pv_no_filter = df_pv_no_filter.drop('Unnamed: 0', axis=1)

            df_pv_no_filter = df_pv_no_filter.loc[df_pv_no_filter['close_p'] != '0.0']
            df_pv_no_filter = refine_df(df_pv_no_filter, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)

            len_df_pv_no_filter_0 = str(df_pv_no_filter.__len__())

            # ==== Low price ====
            logging.info(__file__+" "+"loading df_pv_break")
            df_pv_break = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_pvbreak_')]

            df_pv_break = refine_df(df_pv_break, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)

            if df_pv_break.__len__() <= 0:
                logging.info(__file__+" "+"empty df_pv_break")
                exit()
            else:
                df_pv_break = df_pv_break.loc[df_pv_break['close_p'] != '0.0']
                len_df_pv_break_0 = str(df_pv_break.__len__())

            # ====== max_daily_increase
            logging.info(__file__+" "+"loading df_max_daily_increase")
            df_max_daily_increase = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_max_daily_increase')]
            df_max_daily_increase = df_max_daily_increase.reset_index()
            df_max_daily_increase = refine_df(df_max_daily_increase, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_max_daily_increase = finlib.Finlib().remove_garbage(df_max_daily_increase, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_max_daily_increase length " + str(df_max_daily_increase.__len__()))

            # ====== max_daily_decrease
            logging.info(__file__+" "+"loading df_max_daily_decrease")
            df_max_daily_decrease = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_max_daily_decrease')]
            df_max_daily_decrease = df_max_daily_decrease.reset_index()
            df_max_daily_decrease = refine_df(df_max_daily_decrease, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_max_daily_decrease = finlib.Finlib().remove_garbage(df_max_daily_decrease, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_max_daily_decrease length " + str(df_max_daily_decrease.__len__()))

            # ====== decrease gap
            logging.info(__file__+" "+"loading df_decrease_gap")
            df_decrease_gap = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_decrease_gap')]
            df_decrease_gap = df_decrease_gap.reset_index()
            df_decrease_gap = refine_df(df_decrease_gap, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_decrease_gap = finlib.Finlib().remove_garbage(df_decrease_gap, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_decrease_gap length " + str(df_decrease_gap.__len__()))

            logging.info(__file__ + " " + "loading df_moneyflow_top_amt_perc")
            df_moneyflow_top_amt_perc = finlib.Finlib().regular_read_csv_to_stdard_df(f_moneyflow_top_amt_perc)
            arr.append('df_moneyflow_top_amt_perc')

            logging.info(__file__ + " " + "loading df_moneyflow_top_amt_perc_selected")
            df_moneyflow_top_amt_perc_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_moneyflow_top_amt_perc_selected)
            arr.append('df_moneyflow_top_amt_perc_selected')

            # ====== increase gap
            logging.info(__file__+" "+"loading df_increase_gap")
            df_increase_gap = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_increase_gap')]
            df_increase_gap = df_increase_gap.reset_index()
            df_increase_gap = refine_df(df_increase_gap, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_increase_gap = finlib.Finlib().remove_garbage(df_increase_gap, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_increase_gap length " + str(df_increase_gap.__len__()))

            # ====== 52 week low price
            logging.info(__file__+" "+"loading df_low_price_year")
            df_low_price_year = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_pvbreak_lp_year')]

            # if 'index' in df_low_price_year.columns: df_low_price_year = df_low_price_year.drop('index', axis=1)
            # if 'level_0' in df_low_price_year.columns: df_low_price_year = df_low_price_year.drop('level_0', axis=1)
            # if 'Unnamed: 0' in df_low_price_year.columns:  df_low_price_year = df_low_price_year.drop('Unnamed: 0', axis=1)
            df_low_price_year = df_low_price_year.reset_index()
            df_low_price_year = refine_df(df_low_price_year, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_low_price_year = finlib.Finlib().remove_garbage(df_low_price_year, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_low_price_year length " + str(df_low_price_year.__len__()))

            # ====== 52 week low volume
            logging.info(__file__+" "+"loading df_low_vol_year")
            df_low_vol_year = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_pvbreak_lv_year')]
            # if 'index' in df_low_vol_year.columns: df_low_vol_year = df_low_vol_year.drop('index', axis=1)
            # if 'level_0' in df_low_vol_year.columns: df_low_vol_year = df_low_vol_year.drop('level_0', axis=1)
            # if 'Unnamed: 0' in df_low_vol_year.columns:  df_low_vol_year = df_low_vol_year.drop('Unnamed: 0', axis=1)
            df_low_vol_year = df_low_vol_year.reset_index()
            df_low_vol_year = refine_df(df_low_vol_year, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_low_vol_year = finlib.Finlib().remove_garbage(df_low_vol_year, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_low_vol_year length " + str(df_low_vol_year.__len__()))

            # ====== 52 week high price
            '''
            logging.info(__file__+" "+"loading df_high_price_year")
            df_high_price_year = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_pvbreak_hp_year')]
            #if 'index' in df_high_price_year.columns: df_high_price_year = df_high_price_year.drop('index', axis=1)
            #if 'level_0' in df_high_price_year.columns: df_high_price_year = df_high_price_year.drop('level_0', axis=1)
            #if 'Unnamed: 0' in df_high_price_year.columns:  df_high_price_year = df_high_price_year.drop('Unnamed: 0', axis=1)
            df_high_price_year = df_high_price_year.reset_index()
            df_high_price_year = refine_df(df_high_price_year, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_high_price_year = finlib.Finlib().remove_garbage(df_high_price_year, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_high_price_year length "+str(df_high_price_year.__len__()))


            #====== 52 week high volume
            logging.info(__file__+" "+"loading df_high_vol_year")
            df_high_vol_year = df_pv_no_filter[df_pv_no_filter['op_rsn'].str.contains('_pvbreak_hv_year')]
            #if 'index' in df_high_vol_year.columns: df_high_vol_year = df_high_vol_year.drop('index', axis=1)
            #if 'level_0' in df_high_vol_year.columns: df_high_vol_year = df_high_vol_year.drop('level_0', axis=1)
            #if 'Unnamed: 0' in df_high_vol_year.columns:  df_high_vol_year = df_high_vol_year.drop('Unnamed: 0', axis=1)
            df_high_vol_year = df_high_vol_year.reset_index()
            df_high_vol_year = refine_df(df_high_vol_year, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
            df_high_vol_year = finlib.Finlib().remove_garbage(df_high_vol_year, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_high_vol_year length "+str(df_high_vol_year.__len__()))
            '''

        # common df shared by AG, MG, HK ..
        if 'df_max_daily_increase' in locals():
            arr.append('df_max_daily_increase')

        if 'df_max_daily_decrease' in locals():
            arr.append('df_max_daily_decrease')

        if 'df_pv_break' in locals():
            arr.append('df_pv_break')

        if 'df_low_price_year' in locals():
            arr.append('df_low_price_year')

        if 'df_low_vol_year' in locals():
            arr.append('df_low_vol_year')

        if 'df_high_price_year' in locals():
            arr.append('df_high_price_year')

        if 'df_high_vol_year' in locals():
            arr.append('df_high_vol_year')

        if 'df_decrease_gap' in locals():
            arr.append('df_decrease_gap')

        if 'df_increase_gap' in locals():
            arr.append('df_increase_gap')

        # df_fenghong = finlib.Finlib().regular_read_csv_to_stdard_df(f_fenghong)
        # df_fenghong = df_fenghong[df_fenghong['score'] >= 0.8]
        # df_fenghong = finlib.Finlib().remove_garbage(df_fenghong, code_field_name='code', code_format='C2D6')
        # logging.info(__file__+" "+"\t df_fenghong length " + str(df_fenghong.__len__()))

        #df_ann=pd.DataFrame(columns=['code'])  #removed since ann stop working after julang info web adjusted. 20190228

        df_p_m_div = pd.DataFrame(columns=['code'])
        df_moneyflow = pd.DataFrame(columns=['code'])

        df_talib_ptn = pd.DataFrame(columns=['code'])

        df_fund = pd.DataFrame(columns=['code'])
        df_fund_2 = pd.DataFrame(columns=['code'])
        df_pe_pb_roe_history = pd.DataFrame(columns=['code'])
        df_whitehorse = pd.DataFrame(columns=['code'])
        df_freecashflow_price_ratio = pd.DataFrame(columns=['code'])
        df_hen_cow = pd.DataFrame(columns=['code'])

        df_p_r_div = pd.DataFrame(columns=['code'])
        df_industry_top = pd.DataFrame(columns=['code'])

        #df_lanchou=pd.DataFrame(columns=['code'])

        #df_area_top = finlib.Finlib().regular_read_csv_to_stdard_df(f_area_top ) #removed to make report concise.
        #df_area_top = finlib.Finlib().remove_garbage(df_area_top, code_field_name='code', code_format='C2D6')
        #logging.info(__file__+" "+"\t df_area_top length " + str(df_area_top.__len__()))

        df_peg_ps = finlib.Finlib().regular_read_csv_to_stdard_df(f_fund_peg_ps)
        df_peg_ps = finlib.Finlib().add_market_to_code(df=df_peg_ps)
        df_peg_ps = finlib.Finlib().remove_garbage(df_peg_ps, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_peg_ps length " + str(df_peg_ps.__len__()))

        df_support_resist_line_today = finlib.Finlib().regular_read_csv_to_stdard_df(f_support_resist_line_today)
        df_support_resist_line_today = finlib.Finlib().remove_garbage(df_support_resist_line_today, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_support_resist_line_today length " + str(df_support_resist_line_today.__len__()))

        #df_disclosure_date_notify = finlib.Finlib().regular_read_csv_to_stdard_df(f_disclosure_date_notify)

        # CANNOT USE converters={'name':str}, it will cause error in tabulate. 'UnicodeDecodeError: 'ascii' codec can't decode byte 0xe6 in position 0: ordinal not in range(128)'
        # df_disclosure_date_notify = finlib.Finlib().regular_read_csv_to_stdard_df(f_disclosure_date_notify)
        # df_disclosure_date_notify = finlib.Finlib().remove_garbage(df_disclosure_date_notify, code_field_name='code', code_format='C2D6')
        # logging.info(__file__+" "+"\t df_disclosure_date_notify length " + str(df_disclosure_date_notify.__len__()))

        df_macd_m = finlib.Finlib().regular_read_csv_to_stdard_df(f_macd_m)
        df_macd_m = df_macd_m[df_macd_m.action.str.contains('BUY')]
        df_macd_m = finlib.Finlib().remove_garbage(df_macd_m, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_macd_m length " + str(df_macd_m.__len__()))

        df_macd_w = finlib.Finlib().regular_read_csv_to_stdard_df(f_macd_w)
        df_macd_w = df_macd_w[df_macd_w.action.str.contains('BUY')]
        df_macd_w = finlib.Finlib().remove_garbage(df_macd_w, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_macd_w length " + str(df_macd_w.__len__()))

        df_macd_d = finlib.Finlib().regular_read_csv_to_stdard_df(f_macd_d)
        df_macd_d = df_macd_d[df_macd_d.action.str.contains('BUY')]
        df_macd_d = finlib.Finlib().remove_garbage(df_macd_d, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_macd_d length " + str(df_macd_d.__len__()))

        df_kdj_m = finlib.Finlib().regular_read_csv_to_stdard_df(f_kdj_m)
        df_kdj_m = df_kdj_m[df_kdj_m.action.str.contains('BUY')]
        df_kdj_m = finlib.Finlib().remove_garbage(df_kdj_m, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_kdj_m length " + str(df_kdj_m.__len__()))

        df_kdj_w = finlib.Finlib().regular_read_csv_to_stdard_df(f_kdj_w)
        df_kdj_w = df_kdj_w[df_kdj_w.action.str.contains('BUY')]
        df_kdj_w = finlib.Finlib().remove_garbage(df_kdj_w, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_kdj_w length " + str(df_kdj_w.__len__()))

        df_kdj_d = finlib.Finlib().regular_read_csv_to_stdard_df(f_kdj_d)
        df_kdj_d = df_kdj_d[df_kdj_d.action.str.contains('BUY')]
        df_kdj_d = finlib.Finlib().remove_garbage(df_kdj_d, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_kdj_d length " + str(df_kdj_d.__len__()))

        df_hsgt = finlib.Finlib().regular_read_csv_to_stdard_df(f_hsgt)
        df_hsgt = finlib.Finlib().remove_garbage(df_hsgt, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_hsgt length " + str(df_hsgt.__len__()))

        df_fib_ag = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_ag)
        df_fib_ag = finlib.Finlib().remove_garbage(df_fib_ag, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_fib_ag length " + str(df_fib_ag.__len__()))

        df_fib_ag_selected = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_ag_selected)
        df_fib_ag_selected = finlib.Finlib().remove_garbage(df_fib_ag_selected, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_fib_ag_selected length " + str(df_fib_ag_selected.__len__()))

        df_fib_index = finlib.Finlib().regular_read_csv_to_stdard_df(f_fib_index)
        logging.info(__file__+" "+"\t df_fib_index length " + str(df_fib_index.__len__()))

        df_concept_top = finlib.Finlib().regular_read_csv_to_stdard_df(f_concept_top)
        logging.info(__file__+" "+"\t df_concept_top length " + str(df_concept_top.__len__()))
        '''remove to make result concise
        dump= "/home/ryan/DATA/pickle/sme.csv"
        if (not os.path.isfile(dump)):
            logging.info(__file__+" "+"exit.  file not exist "+dump)
            exit(1)
        else:
            logging.info(__file__+" "+"read csv from "+dump)
            #df_sme=pd.read_pickle(dump)
            df_sme=finlib.Finlib().regular_read_csv_to_stdard_df(dump)

        df_sme = df_sme[['code', 'name']]
        df_sme = finlib.Finlib().remove_garbage(df_sme, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_sme length " + str(df_sme.__len__()))


        dump= "/home/ryan/DATA/pickle/gem.csv"
        if (not os.path.isfile(dump)):
            logging.info(__file__+" "+"exit.  file not exist "+dump)
            exit(1)
        else:
            logging.info(__file__+" "+"read csv from "+dump)
            df_gem=finlib.Finlib().regular_read_csv_to_stdard_df(dump)
        df_gem = df_gem[['code', 'name']]
        df_gem = finlib.Finlib().remove_garbage(df_gem, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_gem length " + str(df_gem.__len__()))



        dump= "/home/ryan/DATA/pickle/sz50.csv"
        if (not os.path.isfile(dump)):
            logging.info(__file__+" "+"exit.  file not exist "+dump)
            exit(1)
        else:
            logging.info(__file__+" "+"read csv from "+dump)
            df_sz50=finlib.Finlib().regular_read_csv_to_stdard_df(dump)
        df_sz50 = df_sz50[['code', 'name']]
        df_sz50 = finlib.Finlib().remove_garbage(df_sz50, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_sz50 length " + str(df_sz50.__len__()))



        dump= "/home/ryan/DATA/pickle/hs300.csv"
        if (not os.path.isfile(dump)):
            logging.info(__file__+" "+"exit.  file not exist "+dump)
            exit(1)
        else:
            logging.info(__file__+" "+"read csv from "+dump)
            df_hs300=finlib.Finlib().regular_read_csv_to_stdard_df(dump)
        df_hs300 = df_hs300[['code', 'name']]
        df_hs300 = finlib.Finlib().remove_garbage(df_hs300, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_hs300 length " + str(df_hs300.__len__()))



        dump= "/home/ryan/DATA/pickle/ZZ500.csv"
        if (not os.path.isfile(dump)):
            logging.info(__file__+" "+"exit.  file not exist "+dump)
            exit(1)
        else:
            logging.info(__file__+" "+"read csv from "+dump)
            df_zz500=finlib.Finlib().regular_read_csv_to_stdard_df(dump)

        df_zz500 = df_zz500[['code', 'name']] #elimate date in the df as it is not necessary and not update to date.
        df_zz500 = finlib.Finlib().remove_garbage(df_zz500, code_field_name='code', code_format='C2D6')
        logging.info(__file__+" "+"\t df_zz500 length " + str(df_zz500.__len__()))
        '''
        '''
        logging.info(__file__+" "+"loading df_ann "+f_ann)
        if (os.path.isfile(f_ann) ):
            df_ann=finlib.Finlib().regular_read_csv_to_stdard_df(f_ann,dtype=str);
            df_ann = finlib.Finlib().remove_garbage(df_ann, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_ann length " + str(df_ann.__len__()))
        else:
            logging.info(__file__+" "+"no such file "+f_ann +". Using Dummy instead")
            #print("stop and exit")
            #exit(1)
        '''
        logging.info(__file__+" "+"loading df_industry_top " + f_industry_top)
        if (os.path.isfile(f_industry_top) and os.stat(f_industry_top).st_size >= 10):
            df_industry_top = finlib.Finlib().regular_read_csv_to_stdard_df(f_industry_top)
            df_industry_top = finlib.Finlib().remove_garbage(df_industry_top, code_field_name='code', code_format='C2D6')
        else:
            logging.warning(__file__+" "+"file not exist or no content " + f_industry_top)

        logging.info(__file__+" "+"\t df_industry_top length " + str(df_industry_top.__len__()))

        logging.info(__file__+" "+"loading df_moneyflow " + f_moneyflow)
        if (os.path.isfile(f_moneyflow) and os.stat(f_moneyflow).st_size >= 10):
            df_moneyflow = finlib.Finlib().regular_read_csv_to_stdard_df(f_moneyflow)
            df_moneyflow = finlib.Finlib().remove_garbage(df_moneyflow, code_field_name='code', code_format='C2D6')
        else:
            logging.warning(__file__+" "+"file not exist or no content " + f_moneyflow)

        logging.info(__file__+" "+"\t df_moneyflow length " + str(df_moneyflow.__len__()))

        logging.info(__file__+" "+"loading df_fund " + f_fund) #f_fund is no longer exist.
        if (os.path.isfile(f_fund) and os.stat(f_fund).st_size >= 10):
            df_fund = finlib.Finlib().regular_read_csv_to_stdard_df(f_fund)
            df_fund = finlib.Finlib().add_market_to_code(df_fund)
            df_fund = df_fund[df_fund['result_value_quarter_fundation'] > 0.8]
            df_fund = finlib.Finlib().remove_garbage(df_fund, code_field_name='code', code_format='C2D6')

            cols = df_fund.columns.tolist()
            #cols = ['code',
            #        'name', 'result_value_sum', 'result_value_today', 'result_value_quarter_fundation', 'pe',
            #        'amount', 'esp', 'profit' ]

            cols = ['code', 'name', 'result_value_sum', 'result_value_today', 'result_value_quarter_fundation', 'pe', 'amount', 'esp', 'profit']
            df_fund = df_fund[cols]
            df_fund = df_fund.sort_values('result_value_sum', ascending=False)
            df_fund = df_fund.reset_index().drop('index', axis=1)
            logging.info(__file__+" "+"\t df_fund length " + str(df_fund.__len__()))

        else:
            logging.info(__file__+" "+"no such file " + f_fund)
            # logging.info(__file__+" "+"stop and exit")
            # exit(1)

        logging.info(__file__+" "+"loading df_fund_2, " + f_fund_2)
        if (os.path.isfile(f_fund_2)) and os.stat(f_fund_2).st_size >= 10:  # > 10 bytes
            df_fund_2 = finlib.Finlib().regular_read_csv_to_stdard_df(f_fund_2)
            #df_fund_2 = df_fund_2[df_fund_2['scoreAB']>90]
            df_fund_2 = finlib.Finlib().remove_garbage(df_fund_2, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_fund_2 length " + str(df_fund_2.__len__()))

            #df_fund_2 = finlib.Finlib().ts_code_to_code(df_fund_2)
            #df_fund_2 = df_fund_2[['code', 'name', 'scoreA', 'V_C_P', 'ValuePrice', 'CurrentPrice',
            #                       'score_over_years', 'score_avg', 'number_in_top_30',
            #                       'ktr_cnt_win', 'ktr_win_p', 'ktr_inc_avg']]

            df_fund_2 = df_fund_2.drop_duplicates()
            df_fund_2 = df_fund_2.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__+" "+"no such file " + f_fund_2)
            logging.info(__file__+" "+"stop and exit")
            exit(0)

        logging.info(__file__+" "+"loading df_pe_pb_roe_history, " + f_pe_pb_roe_history)
        if (os.path.isfile(f_pe_pb_roe_history)) and os.stat(f_pe_pb_roe_history).st_size >= 10:  # > 10 bytes
            df_pe_pb_roe_history = finlib.Finlib().regular_read_csv_to_stdard_df(f_pe_pb_roe_history)
            df_pe_pb_roe_history = finlib.Finlib().remove_garbage(df_pe_pb_roe_history, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_pe_pb_roe_history length " + str(df_pe_pb_roe_history.__len__()))

            df_pe_pb_roe_history = df_pe_pb_roe_history.drop_duplicates()
            df_pe_pb_roe_history = df_pe_pb_roe_history.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__+" "+"no such file " + f_pe_pb_roe_history)
            logging.info(__file__+" "+"stop and exit")
            exit(0)

        f_whitehorse = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/white_horse.csv"
        logging.info(__file__+" "+"loading whitehorse, " + f_whitehorse)
        if (os.path.isfile(f_whitehorse)) and os.stat(f_whitehorse).st_size >= 10:  # > 10 bytes
            df_whitehorse = finlib.Finlib().regular_read_csv_to_stdard_df(f_whitehorse)
            df_whitehorse = finlib.Finlib().remove_garbage(df_whitehorse, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_whitehorse length " + str(df_whitehorse.__len__()))

            df_whitehorse = df_whitehorse.drop_duplicates()
            df_whitehorse = df_whitehorse.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__+" "+"no such file " + f_whitehorse)

        f_freecashflow_price_ratio = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/freecashflow_price_ratio.csv"
        logging.info(__file__+" "+"loading f_freecashflow_price_ratio, " + f_freecashflow_price_ratio)
        if (os.path.isfile(f_freecashflow_price_ratio)) and os.stat(f_freecashflow_price_ratio).st_size >= 10:  # > 10 bytes
            df_freecashflow_price_ratio = finlib.Finlib().regular_read_csv_to_stdard_df(f_freecashflow_price_ratio)
            df_freecashflow_price_ratio = finlib.Finlib().remove_garbage(df_freecashflow_price_ratio, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_freecashflow_price_ratio length " + str(df_freecashflow_price_ratio.__len__()))

            df_freecashflow_price_ratio = df_freecashflow_price_ratio.drop_duplicates()
            df_freecashflow_price_ratio = df_freecashflow_price_ratio.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__+" "+"no such file " + f_freecashflow_price_ratio)

        f_hen_cow = "/home/ryan/DATA/pickle/Stock_Fundamental/fundamentals_2/report/hen_cow.csv"
        logging.info(__file__+" "+"loading f_hen_cow, " + f_hen_cow)
        if (os.path.isfile(f_whitehorse)) and os.stat(f_whitehorse).st_size >= 10:  # > 10 bytes
            df_hen_cow = finlib.Finlib().regular_read_csv_to_stdard_df(f_hen_cow)

            df_hen_cow = finlib.Finlib().remove_garbage(df_hen_cow, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_hen_cow length " + str(df_hen_cow.__len__()))

            df_hen_cow = df_hen_cow.drop_duplicates()
            df_hen_cow = df_hen_cow.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__+" "+"no such file " + f_hen_cow)

        logging.info(__file__+" "+"loading df_pv_db_buy_filter, "+ f_pv_db_buy_filter)
        if (os.path.isfile(f_pv_db_buy_filter)) and os.stat(f_pv_db_buy_filter).st_size >= 10:  # > 10 bytes
            df_pv_db_buy_filter = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_db_buy_filter)
            df_pv_db_buy_filter.drop_duplicates(inplace=True)

            df_pv_db_buy_filter.sort_values('2mea', ascending=False, inplace=True)
            # already sorted by Increase_2D in t_daily_pattern_Hit_Price_Volume.py
            df_pv_db_buy_filter = df_pv_db_buy_filter.loc[df_pv_db_buy_filter['close_p'] != '0.0']
            len_df_pv_db_buy_filter_0 = str(df_pv_db_buy_filter.__len__())
            df_pv_db_buy_filter = finlib.Finlib().remove_garbage(df_pv_db_buy_filter, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_pv_db_buy_filter length " + str(df_pv_db_buy_filter.__len__()))
        else:
            logging.info(__file__+" "+"ERROR: NOT found file " + f_pv_db_buy_filter)
            # exit(0)

        logging.info(__file__+" "+"loading  df_pv_db_sell_filter")
        if (os.path.isfile(f_pv_db_sell_filter)) and os.stat(f_pv_db_sell_filter).st_size >= 10:  # > 10 bytes
            df_pv_db_sell_filter = finlib.Finlib().regular_read_csv_to_stdard_df(f_pv_db_sell_filter)
            df_pv_db_sell_filter.drop_duplicates(inplace=True)

            df_pv_db_sell_filter.sort_values('2mea', ascending=True, inplace=True)
            # already sorted by Increase_2D in t_daily_pattern_Hit_Price_Volume.py
            df_pv_db_sell_filter = df_pv_db_sell_filter.loc[df_pv_db_sell_filter['close_p'] != '0.0']
            len_df_pv_db_sell_filter_0 = str(df_pv_db_sell_filter.__len__())
            df_pv_db_sell_filter = finlib.Finlib().remove_garbage(df_pv_db_sell_filter, code_field_name='code', code_format='C2D6')
            logging.info(__file__+" "+"\t df_pv_db_sell_filter length " + str(df_pv_db_sell_filter.__len__()))

        else:
            logging.info(__file__+" "+"ERROR: NOT found file " + f_pv_db_sell_filter)
#====

        f_yunxian_sell = '/home/ryan/DATA/result/ag_junxian_barstyle_yunxian_sell.csv'
        logging.info(__file__ + " " + "loading f_yunxian_sell, " + f_yunxian_sell)
        if (os.path.isfile(f_yunxian_sell)) and os.stat(f_yunxian_sell).st_size >= 10:  # > 10 bytes
            df_yunxian_sell = finlib.Finlib().regular_read_csv_to_stdard_df(f_yunxian_sell)

            df_yunxian_sell = finlib.Finlib().remove_garbage(df_yunxian_sell, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_yunxian_sell length " + str(df_yunxian_sell.__len__()))

            df_yunxian_sell = df_yunxian_sell.drop_duplicates()
            df_yunxian_sell = df_yunxian_sell.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_yunxian_sell)


        f_yunxian_buy = '/home/ryan/DATA/result/ag_junxian_barstyle_yunxian_buy.csv'
        logging.info(__file__ + " " + "loading f_yunxian_buy, " + f_yunxian_buy)
        if (os.path.isfile(f_yunxian_buy)) and os.stat(f_yunxian_buy).st_size >= 10:  # > 10 bytes
            df_yunxian_buy = finlib.Finlib().regular_read_csv_to_stdard_df(f_yunxian_buy)

            df_yunxian_buy = finlib.Finlib().remove_garbage(df_yunxian_buy, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_yunxian_buy length " + str(df_yunxian_buy.__len__()))

            df_yunxian_buy = df_yunxian_buy.drop_duplicates()
            df_yunxian_buy = df_yunxian_buy.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_yunxian_buy)

    #===
        f_duotou_pailie = '/home/ryan/DATA/result/ag_junxian_barstyle_duotou_pailie.csv'
        logging.info(__file__ + " " + "loading f_yunxian_sell, " + f_duotou_pailie)
        if (os.path.isfile(f_duotou_pailie)) and os.stat(f_duotou_pailie).st_size >= 10:  # > 10 bytes
            df_duotou_pailie = finlib.Finlib().regular_read_csv_to_stdard_df(f_duotou_pailie)

            df_duotou_pailie = finlib.Finlib().remove_garbage(df_duotou_pailie, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_duotou_pailie length " + str(df_duotou_pailie.__len__()))

            df_duotou_pailie = df_duotou_pailie.drop_duplicates()
            df_duotou_pailie = df_duotou_pailie.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_duotou_pailie)


    #====
        f_jincha_minor = '/home/ryan/DATA/result/ag_junxian_barstyle_jincha_minor.csv'
        logging.info(__file__ + " " + "loading f_yunxian_sell, " + f_jincha_minor)
        if (os.path.isfile(f_jincha_minor)) and os.stat(f_jincha_minor).st_size >= 10:  # > 10 bytes
            df_jincha_minor = finlib.Finlib().regular_read_csv_to_stdard_df(f_jincha_minor)

            df_jincha_minor = finlib.Finlib().remove_garbage(df_jincha_minor, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_jincha_minor length " + str(df_jincha_minor.__len__()))

            df_jincha_minor = df_jincha_minor.drop_duplicates()
            df_jincha_minor = df_jincha_minor.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_jincha_minor)


    ##===
        f_jincha_major = '/home/ryan/DATA/result/ag_junxian_barstyle_jincha_major.csv'
        logging.info(__file__ + " " + "loading f_yunxian_sell, " + f_jincha_major)
        if (os.path.isfile(f_jincha_major)) and os.stat(f_jincha_major).st_size >= 10:  # > 10 bytes
            df_jincha_major = finlib.Finlib().regular_read_csv_to_stdard_df(f_jincha_major)

            df_jincha_major = finlib.Finlib().remove_garbage(df_jincha_major, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_jincha_major length " + str(df_jincha_major.__len__()))

            df_jincha_major = df_jincha_major.drop_duplicates()
            df_jincha_major = df_jincha_major.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_jincha_major)


    #====
        f_very_strong_up_trend = '/home/ryan/DATA/result/ag_junxian_barstyle_very_strong_up_trend.csv'
        logging.info(__file__ + " " + "loading f_very_strong_up_trend, " + f_very_strong_up_trend)
        if (os.path.isfile(f_very_strong_up_trend)) and os.stat(f_very_strong_up_trend).st_size >= 10:  # > 10 bytes
            df_very_strong_up_trend = finlib.Finlib().regular_read_csv_to_stdard_df(f_very_strong_up_trend)

            df_very_strong_up_trend = finlib.Finlib().remove_garbage(df_very_strong_up_trend, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_very_strong_up_trend length " + str(df_very_strong_up_trend.__len__()))

            df_very_strong_up_trend = df_very_strong_up_trend.drop_duplicates()
            df_very_strong_up_trend = df_very_strong_up_trend.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_very_strong_up_trend)



    #====
        f_very_strong_down_trend = '/home/ryan/DATA/result/ag_junxian_barstyle_very_strong_down_trend.csv'
        logging.info(__file__ + " " + "loading f_very_strong_down_trend, " + f_very_strong_down_trend)
        if (os.path.isfile(f_very_strong_down_trend)) and os.stat(f_very_strong_down_trend).st_size >= 10:  # > 10 bytes
            df_very_strong_down_trend = finlib.Finlib().regular_read_csv_to_stdard_df(f_very_strong_down_trend)

            df_very_strong_down_trend = finlib.Finlib().remove_garbage(df_very_strong_down_trend, code_field_name='code', code_format='C2D6')
            logging.info(__file__ + " " + "\t df_very_strong_down_trend length " + str(df_very_strong_down_trend.__len__()))

            df_very_strong_down_trend = df_very_strong_down_trend.drop_duplicates()
            df_very_strong_down_trend = df_very_strong_down_trend.reset_index().drop('index', axis=1)
        else:
            logging.info(__file__ + " " + "no such file " + f_very_strong_down_trend)

    elif stock_global in ['HK', 'KG']:

        arr.append('df_hkhs_index')

    elif stock_global in ['MG', 'US', 'CN']:
        df_sp500_index = finlib.Finlib().regular_read_csv_to_stdard_df(f_sp500_index)
        arr.append('df_sp500_index')

        df_sp400_index = finlib.Finlib().regular_read_csv_to_stdard_df(f_sp400_index)
        arr.append('df_sp400_index')

        df_nasdqa100_index = finlib.Finlib().regular_read_csv_to_stdard_df(f_nasdqa100_index)
        arr.append('df_nasdqa100_index')

        df_dow_index = finlib.Finlib().regular_read_csv_to_stdard_df(f_dow_index)
        arr.append('df_dow_index')

    ##################################################
    # AG: df filter then arr append
    ##################################################
    if stock_global in ['AG']:
        if 'df_pv_break' in locals():  # it has already been added to arr, do not arr.append again.
            logging.info(__file__+" "+"filte df_pv_break")
            df_pv_break = finlib.Finlib().df_filter(df_pv_break)

        # if 'df_fenghong' in locals():
        #     df_fenghong = finlib.Finlib().add_market_to_code(df_fenghong)
        #     df_fenghong = finlib.Finlib().df_filter(df_fenghong)
        #     arr.append('df_fenghong')

        if 'df_ann' in locals():
            df_ann = finlib.Finlib().add_market_to_code(df_ann)
            df_ann = finlib.Finlib().df_filter(df_ann)
            arr.append('df_ann')

        if 'df_fund' in locals():
            #df_fund=finlib.Finlib().add_market_to_code(df_fund)
            df_fund = finlib.Finlib().df_filter(df_fund)
            arr.append('df_fund')

        if 'df_fund_2' in locals():
            arr.append('df_fund_2')

        if 'df_pe_pb_roe_history' in locals():
            arr.append('df_pe_pb_roe_history')

        if 'df_whitehorse' in locals():
            arr.append('df_whitehorse')

        if 'df_freecashflow_price_ratio' in locals():
            arr.append('df_freecashflow_price_ratio')

        if 'df_hen_cow' in locals():
            arr.append('df_hen_cow')

        if 'df_pv_db_buy_filter' in locals():  #code already in 'SH/SZ' format
            len_df_pv_db_buy_filter_0 = str(df_pv_db_buy_filter.__len__())
            logging.info(__file__+" "+"filtering df_pv_db_buy_filter")
            df_pv_db_buy_filter = finlib.Finlib().df_filter(df_pv_db_buy_filter)
            arr.append('df_pv_db_buy_filter')

        if 'df_pv_db_sell_filter' in locals():  #code already in 'SH/SZ' format
            arr.append('df_pv_db_sell_filter')
            len_df_pv_db_sell_filter_0 = str(df_pv_db_sell_filter.__len__())
            logging.info(__file__+" "+"filtering df_pv_db_sell_filter")
            df_pv_db_sell_filter = finlib.Finlib().df_filter(df_pv_db_sell_filter)

        if 'df_pv_no_filter' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_pv_no_filter")
            df_pv_no_filter = finlib.Finlib().df_filter(df_pv_no_filter)
            arr.append('df_pv_no_filter')

        if 'df_hs300' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_hs300")
            df_hs300 = finlib.Finlib().df_filter(df_hs300)
            arr.append('df_hs300')

        if 'df_sz50' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_sz50")
            df_sz50 = finlib.Finlib().df_filter(df_sz50)
            arr.append('df_sz50')

        if 'df_zz500' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_zz500")
            df_zz500 = finlib.Finlib().df_filter(df_zz500)
            arr.append('df_zz500')

        if 'df_sme' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_sme")
            df_sme = finlib.Finlib().df_filter(df_sme)
            arr.append('df_sme')

        if 'df_gem' in locals():  #code already in 'SH/SZ' format
            logging.info(__file__+" "+"filtering df_gem")
            df_gem = finlib.Finlib().df_filter(df_gem)
            arr.append('df_gem')

        if 'df_industry_top' in locals():
            logging.info(__file__+" "+"filtering df_industry_top")
            df_industry_top = finlib.Finlib().add_market_to_code(df_industry_top)
            df_industry_top = finlib.Finlib().df_filter(df_industry_top)
            arr.append('df_industry_top')

        if 'df_area_top' in locals():
            logging.info(__file__+" "+"filtering df_area_top")
            df_area_top = finlib.Finlib().add_market_to_code(df_area_top)
            arr.append('df_area_top')
            df_area_top = finlib.Finlib().df_filter(df_area_top)

        if 'df_peg_ps' in locals():
            logging.info(__file__+" "+"filtering df_peg_ps")
            df_peg_ps = finlib.Finlib().df_filter(df_peg_ps)  #market code should in code, e.g.  SH600001
            arr.append('df_peg_ps')

        if 'df_support_resist_line_today' in locals():
            logging.info(__file__+" "+"filtering df_support_resist_line_today")
            df_support_resist_line_today = finlib.Finlib().df_filter(df_support_resist_line_today)
            arr.append('df_support_resist_line_today')

        if 'df_disclosure_date_notify' in locals():
            logging.info(__file__+" "+"filtering df_disclosure_date_notify")
            df_disclosure_date_notify = finlib.Finlib().df_filter(df_disclosure_date_notify)
            arr.append('df_disclosure_date_notify')

        if 'df_macd_m' in locals():
            logging.info(__file__+" "+"filtering df_macd_m")
            df_macd_m = finlib.Finlib().df_filter(df_macd_m)
            arr.append('df_macd_m')

        if 'df_macd_w' in locals():
            logging.info(__file__+" "+"filtering df_macd_w")
            df_macd_w = finlib.Finlib().df_filter(df_macd_w)
            arr.append('df_macd_w')

        if 'df_macd_d' in locals():
            logging.info(__file__+" "+"filtering df_macd_d")
            df_macd_d = finlib.Finlib().df_filter(df_macd_d)
            arr.append('df_macd_d')

        if 'df_kdj_m' in locals():
            logging.info(__file__+" "+"filtering df_kdj_m")
            df_kdj_m = finlib.Finlib().df_filter(df_kdj_m)
            arr.append('df_kdj_m')

        if 'df_kdj_w' in locals():
            logging.info(__file__+" "+"filtering df_kdj_w")
            df_kdj_w = finlib.Finlib().df_filter(df_kdj_w)
            arr.append('df_kdj_w')

        if 'df_kdj_d' in locals():
            logging.info(__file__+" "+"filtering df_kdj_d")
            df_kdj_d = finlib.Finlib().df_filter(df_kdj_d)
            arr.append('df_kdj_d')

        if 'df_hsgt' in locals():
            logging.info(__file__+" "+"filtering df_hsgt")
            df_hsgt = finlib.Finlib().df_filter(df_hsgt)
            arr.append('df_hsgt')

        if 'df_fib_ag' in locals():
            logging.info(__file__+" "+"filtering df_fib_ag")
            df_fib_ag = finlib.Finlib().df_filter(df_fib_ag)
            arr.append('df_fib_ag')

        if 'df_fib_ag_selected' in locals():
            logging.info(__file__+" "+"filtering df_fib_ag_selected")
            df_fib_ag_selected = finlib.Finlib().df_filter(df_fib_ag_selected)
            arr.append('df_fib_ag_selected')

        if 'df_fib_index' in locals():
            logging.info(__file__+" "+"filtering df_fib_index")
            df_fib_index = finlib.Finlib().df_filter(df_fib_index)
            arr.append('df_fib_index')

        if 'df_concept_top' in locals():
            logging.info(__file__+" "+"filtering df_concept_top")
            df_concept_top = finlib.Finlib().df_filter(df_concept_top)
            arr.append('df_concept_top')

        if 'df_yunxian_sell' in locals():
            logging.info(__file__+" "+"filtering df_yunxian_sell")
            df_concept_top = finlib.Finlib().df_filter(df_yunxian_sell)
            arr.append('df_yunxian_sell')

        if 'df_yunxian_buy' in locals():
            logging.info(__file__+" "+"filtering df_yunxian_buy")
            df_concept_top = finlib.Finlib().df_filter(df_yunxian_buy)
            arr.append('df_yunxian_buy')

        if 'df_duotou_pailie' in locals():
            logging.info(__file__+" "+"filtering df_duotou_pailie")
            df_concept_top = finlib.Finlib().df_filter(df_duotou_pailie)
            arr.append('df_duotou_pailie')

        if 'df_jincha_minor' in locals():
            logging.info(__file__+" "+"filtering df_jincha_minor")
            df_concept_top = finlib.Finlib().df_filter(df_jincha_minor)
            arr.append('df_jincha_minor')

        if 'df_jincha_major' in locals():
            logging.info(__file__+" "+"filtering df_jincha_major")
            df_jincha_major = finlib.Finlib().df_filter(df_jincha_major)
            arr.append('df_jincha_major')

        if 'df_very_strong_up_trend' in locals():
            logging.info(__file__+" "+"filtering df_very_strong_up_trend")
            df_concept_top = finlib.Finlib().df_filter(df_very_strong_up_trend)
            arr.append('df_very_strong_up_trend')

        if 'df_very_strong_down_trend' in locals():
            logging.info(__file__+" "+"filtering df_very_strong_down_trend")
            df_concept_top = finlib.Finlib().df_filter(df_very_strong_down_trend)
            arr.append('df_very_strong_down_trend')





    ##################################################
    # AG: refine df
    ##################################################
    if stock_global in ['AG']:
        #insert (merged) df_ann, df_pv_no_filter, df_pv_db_buy_filter into the order_tracking_stock.
        #df_ann = refine_df(df_ann,has_db_record=False, force_pass=True)
        if 'df_ann' in locals():
            if stock_global in ['AG']:
                logging.info(__file__+" "+"will refine_df df_ann announcement")
                #df_ann = refine_df(df_ann,has_db_record=False, force_pass=True, force_pass_open_reason='announcement')  #because the df_ann doesn't have duplicate on 'code' column
                #df_ann = refine_df(df_ann,has_db_record=False, force_pass=True, force_pass_open_reason='announcement')  #because the df_ann doesn't have duplicate on 'code' column
                df_ann = refine_df(df_ann, has_db_record=False, force_pass=True, force_pass_open_reason='announcement', insert_buy_record_to_db=False, debug=debug)  #because the df_ann doesn't have duplicate on 'code' column

        if 'df_pv_no_filter' in locals():
            if stock_global in ['AG']:
                logging.info(__file__+" "+"will refine_df df_pv_no_filter")
                #df_pv_no_filter = refine_df(df_pv_no_filter,has_db_record=False)  #ryan comment to debug
                df_pv_no_filter = refine_df(df_pv_no_filter, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
                #refine_df(df_pv_no_filter,has_db_record=False) <<<<< LAEGE RESULT 9000
                logging.info(__file__+" "+"Len df_pv_no_filter " + str(len_df_pv_no_filter_0) + " => " + str(df_pv_no_filter.__len__()))

            #if stock_global in ['MG','US','CN']:
            else:
                logging.info(__file__+" "+"will refine_df df_pv_no_filter")
                df_pv_no_filter = refine_df(df_pv_no_filter, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
                logging.info(__file__+" "+"Len df_pv_no_filter " + str(len_df_pv_no_filter_0) + " => " + str(df_pv_no_filter.__len__()))

        if 'df_pv_db_buy_filter' in locals():
            if stock_global in ['AG']:
                logging.info(__file__+" "+"will refine_df df_pv_db_buy_filter")
                #df_pv_db_buy_filter = refine_df(df_pv_db_buy_filter,has_db_record=True)
                #df_pv_db_buy_filter = refine_df(df_pv_db_buy_filter,has_db_record=False) #ryan debug
                df_pv_db_buy_filter = refine_df(df_pv_db_buy_filter, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
                logging.info(__file__+" "+"Len df_pv_db_buy_filter " + str(len_df_pv_db_buy_filter_0) + " => " + str(df_pv_db_buy_filter.__len__()))

            else:
                logging.info(__file__+" "+"will refine_df df_pv_db_buy_filter")
                df_pv_db_buy_filter = refine_df(df_pv_no_filter, has_db_record=False, force_pass=True, insert_buy_record_to_db=False, debug=debug)
                logging.info(__file__+" "+"Len df_pv_db_buy_filter " + str(len_df_pv_db_buy_filter_0) + " => " + str(df_pv_db_buy_filter.__len__()))

    ####################
    todayS_ymd = datetime.today().strftime('%Y%m%d')
    day_3_before_date = datetime.strptime(todayS_ymd, '%Y%m%d') - timedelta(5)
    day_3_before_date_ymd = day_3_before_date.strftime('%Y%m%d')

    #skip_sets = ('df_sz50', 'df_zz500', 'df_sme', 'df_hs300', 'df_gem')
    skip_sets = ('df_zz500', 'df_sme', 'df_hs300', 'df_gem', 'df_high_price_year')  #remove sz50 as 'df_sz50 df_sme' would make sense.
    buy_sets = ('df_fund', 'df_pv_break', 'df_low_price_year', 'df_low_vol_year')

    #ryan add 2018.08.26, reduce columns in df
    for d in arr:
        logging.info(d)
        tmp_df = combin_filter(eval(d), debug=debug)
        s = d + " = tmp_df"
        exec(s)

    ### Ryan debug start
    if 'df_max_daily_increase' in locals():
        print(df_max_daily_increase.head(2))
    if 'df_max_daily_decrease' in locals():
        print(df_max_daily_decrease.head(2))
    if 'df_pv_break' in locals(): print(df_pv_break.head(2))
    if 'df_low_price_year' in locals(): print(df_low_price_year.head(2))
    if 'df_low_vol_year' in locals(): print(df_low_vol_year.head(2))
    #print(df_high_price_year.head(2))
    #print(df_high_vol_year.head(2))
    if 'df_decrease_gap' in locals(): print(df_decrease_gap.head(2))
    if 'df_increase_gap' in locals(): print(df_increase_gap.head(2))
    if 'df_pv_no_filter' in locals(): print(df_pv_no_filter.head(2))

    if stock_global in ['AG']:
        # if 'df_fenghong' in locals(): print(df_fenghong.head(2))
        #print(df_ann.head(2))
        if 'df_fund' in locals(): print(df_fund.head(2))
        if 'df_fund_2' in locals(): print(df_fund_2.head(2))
        if 'df_pe_pb_roe_history' in locals():
            print(df_pe_pb_roe_history.head(2))
        if 'df_whitehorse' in locals(): print(df_whitehorse.head(2))
        if 'df_freecashflow_price_ratio' in locals():
            print(df_freecashflow_price_ratio.head(2))
        if 'df_hen_cow' in locals(): print(df_hen_cow.head(2))

        if 'df_pv_db_buy_filter' in locals():
            print(df_pv_db_buy_filter.head(2))
        if 'df_pv_db_sell_filter' in locals():
            print(df_pv_db_sell_filter.head(2))
        '''
        print(df_hs300.head(2))
        print(df_sz50.head(2))
        print(df_zz500.head(2))
        print(df_sme.head(2))
        print(df_gem.head(2))
        print(df_industry_top.head(2))
        #print(df_area_top.head(2))
        '''
        if 'df_peg_ps' in locals(): print(df_peg_ps.head(2))
        if 'df_disclosure_date_notify' in locals():
            print(df_disclosure_date_notify.head(2))

        if 'df_support_resist_line_today' in locals():
            print(df_support_resist_line_today.head(2))
        if 'df_macd_m' in locals(): print(df_macd_m.head(2))
        if 'df_macd_w' in locals(): print(df_macd_w.head(2))
        if 'df_macd_d' in locals(): print(df_macd_d.head(2))
        if 'df_kdj_m' in locals(): print(df_kdj_m.head(2))
        if 'df_kdj_w' in locals(): print(df_kdj_w.head(2))
        if 'df_kdj_d' in locals(): print(df_kdj_d.head(2))
        if 'df_fib' in locals(): print(df_fib.head(2))
        if 'df_fib_index' in locals(): print(df_fib_index.head(2))
        if 'df_concept_top' in locals(): print(df_concept_top.head(2))
    ### Ryan debug end

    ################################################
    # Df properties defination.
    ################################################
    df_dict = {}

    df_dict["df_reduced_quarter_year"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_fund_2"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_whitehorse"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_freecashflow_price_ratio"] = {"term": "LONG TERM", "price": "CHEAP"}
    df_dict["df_hen_cow"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_fenghong"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_area_top"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_industry_top"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_hs300"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_sz50"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_zz500"] = {"term": "LONG TERM", "price": "NA"}

    df_dict["df_fund"] = {"term": "MIDDLE TERM", "price": "NA"}
    df_dict["df_pe_pb_roe_history"] = {"term": "MIDDLE TERM", "price": "CHEAP"}
    df_dict["df_peg_ps"] = {"term": "MIDDLE TERM", "price": "NA"}  #1q and 4q

    df_dict["df_max_daily_increase"] = {"term": "SHORT TERM", "price": "EXPENSIVE"}
    df_dict["df_max_daily_decrease"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_pv_break"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_low_price_year"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_low_vol_year"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_high_price_year"] = {"term": "SHORT TERM", "price": "EXPENSIVE"}
    df_dict["df_high_vol_year"] = {"term": "SHORT TERM", "price": "EXPENSIVE"}
    df_dict["df_decrease_gap"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_increase_gap"] = {"term": "SHORT TERM", "price": "EXPENSIVE"}
    df_dict["df_ann"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_pv_db_buy_filter"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_pv_db_sell_filter"] = {"term": "SHORT TERM", "price": "EXPENSIVE"}
    df_dict["df_pv_no_filter"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_support_resist_line_today"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_disclosure_date_notify"] = {"term": "SHORT TERM", "price": "NA"}

    df_dict["df_macd_m"] = {"term": "LONG TERM", "price": "CHEAP"}  #_m =>Month, is long term for macd.
    df_dict["df_macd_w"] = {"term": "MIDDLE TERM", "price": "CHEAP"}
    df_dict["df_macd_d"] = {"term": "SHORT TERM", "price": "CHEAP"}

    df_dict["df_kdj_m"] = {"term": "LONG TERM", "price": "CHEAP"}
    df_dict["df_kdj_w"] = {"term": "MIDDLE TERM", "price": "CHEAP"}
    df_dict["df_kdj_d"] = {"term": "SHORT TERM", "price": "CHEAP"}

    df_dict["df_hsgt"] = {"term": "SHORT TERM", "price": "CHEAP"}
    df_dict["df_fib"] = {"term": "LONG TERM", "price": "CHEAP"}
    df_dict["df_fib_index"] = {"term": "LONG TERM", "price": "CHEAP"}
    df_dict["df_concept_top"] = {"term": "MIDDLE TERM", "price": "NA"}

    df_dict["df_sme"] = {"term": "NA", "price": "NA"}
    df_dict["df_gem"] = {"term": "NA", "price": "NA"}
    df_dict["df_gem"] = {"term": "NA", "price": "NA"}

    df_dict["df_hkhs_index"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_sp500_index"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_sp400_index"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_nasdqa100_index"] = {"term": "LONG TERM", "price": "NA"}
    df_dict["df_dow_index"] = {"term": "LONG TERM", "price": "NA"}

    df_dict["df_fib_ag"] = {"term": "NA", "price": "NA"}
    df_dict["df_fib_ag_index_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_fib_ag_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_fib_us_index_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_key_points_us_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_pv_no_filter_us_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_fib_us_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_key_points_hk_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_pv_no_filter_hk_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_fib_hk_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_pv_no_filter_ag_selected"] = {"term": "NA", "price": "NA"}
    df_dict["df_key_points_ag_selected"] = {"term": "NA", "price": "NA"}

    df_dict["df_moneyflow_top_amt_perc"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_moneyflow_top_amt_perc_selected"] = {"term": "SHORT TERM", "price": "NA"}

    df_dict["df_yunxian_sell"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_yunxian_buy"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_duotou_pailie"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_minor"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_major"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_up_trend"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_down_trend"] = {"term": "SHORT TERM", "price": "NA"}

    df_dict["df_yunxian_sell_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_yunxian_buy_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_duotou_pailie_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_minor_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_major_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_up_trend_selected"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_down_trend_selected"] = {"term": "SHORT TERM", "price": "NA"}


    df_dict["df_yunxian_sell_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_yunxian_buy_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_duotou_pailie_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_minor_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_major_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_up_trend_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_down_trend_selected_hk"] = {"term": "SHORT TERM", "price": "NA"}


    df_dict["df_yunxian_sell_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_yunxian_buy_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_duotou_pailie_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_minor_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_jincha_major_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_up_trend_selected_us"] = {"term": "SHORT TERM", "price": "NA"}
    df_dict["df_very_strong_down_trend_selected_us"] = {"term": "SHORT TERM", "price": "NA"}




    #df_dict["df_key_points_ag_selected"] = {"term": "NA", "price": "NA"}

    if (stock_global in ['AG']) and (not full_combination):  #for AG

        df_combined_year = pd.DataFrame()
        df_reduced_year = pd.DataFrame()

        #gen yearly df
        if 'df_fund_2' in locals():
            df_combined_year = df_fund_2
            df_reduced_year = df_fund_2
            #arr.remove('df_fund_2')
        else:
            logging.info(__file__+" "+"Base df_fund_2 doesn't exists, quit the program.")

        if 'df_whitehorse' in locals() and df_whitehorse.__len__()>0:
            df_combined_year = pd.concat([df_combined_year, df_whitehorse], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            df_reduced_year = pd.merge(df_reduced_year, df_whitehorse, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
            logging.info(__file__+" "+"After df_whitehorse, df_combined_year " + str(df_combined_year.__len__()) + ", df_reduced_year " + str(df_reduced_year.__len__()))
            #arr.remove('df_whitehorse')

        if 'df_hen_cow' in locals() and df_hen_cow.__len__()>0:
            df_combined_year = pd.concat([df_combined_year, df_hen_cow], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            df_reduced_year = pd.merge(df_reduced_year, df_hen_cow, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
            logging.info(__file__+" "+"After df_hen_cow, df_combined_year " + str(df_combined_year.__len__()) + ", df_reduced_year " + str(df_reduced_year.__len__()))
            #arr.remove('df_hen_cow')

        # if 'df_fenghong' in locals() and df_fenghong.__len__()>0:
        #     df_combined_year = pd.concat([df_combined_year, df_fenghong], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
        #     df_reduced_year = pd.merge(df_reduced_year, df_fenghong, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
        #     logging.info(__file__+" "+"After df_fenghong, df_combined_year " + str(df_combined_year.__len__()) + ", df_reduced_year " + str(df_reduced_year.__len__()))
        #     #arr.remove('df_fenghong')

        #gen quarterly df
        df_combined_quarter = pd.DataFrame()
        df_reduced_quarter = pd.DataFrame()

        if 'df_fund' in locals():
            df_combined_quarter = df_fund
            df_reduced_quarter = df_fund
            #arr.remove('df_fund')

        if 'df_peg_ps' in locals() and df_peg_ps.__len__()>50:
            df_combined_quarter = pd.concat([df_combined_quarter, df_peg_ps], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            df_reduced_quarter = pd.merge(df_reduced_quarter, df_peg_ps, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
            logging.info(__file__+" "+"After df_peg_ps, df_combined_quarter " + str(df_combined_quarter.__len__()) + ", df_reduced_quarter " + str(df_reduced_quarter.__len__()))
            #arr.remove('df_peg_ps')

        if 'df_freecashflow_price_ratio' in locals() and df_freecashflow_price_ratio.__len__()>50:
            df_combined_quarter = pd.concat([df_combined_quarter, df_freecashflow_price_ratio], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            df_reduced_quarter = pd.merge(df_reduced_quarter, df_freecashflow_price_ratio, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
            logging.info(__file__+" "+"After df_freecashflow_price_ratio, df_combined_quarter " + str(df_combined_quarter.__len__()) + ", df_reduced_quarter " + str(df_reduced_quarter.__len__()))
            #arr.remove('df_freecashflow_price_ratio')

        if 'df_industry_top' in locals() and df_industry_top.__len__()>50:
            df_combined_quarter = pd.concat([df_combined_quarter, df_industry_top], sort=False).drop_duplicates().reset_index().drop('index', axis=1)
            df_reduced_quarter = pd.merge(df_reduced_quarter, df_industry_top, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
            logging.info(__file__+" "+"After df_industry_top, df_combined_quarter " + str(df_combined_quarter.__len__()) + ", df_reduced_quarter " + str(df_reduced_quarter.__len__()))
            arr.remove('df_industry_top')

        #Treat this is the good candicates, while filter garbage filt the garbage only.
        # df_reduced_quarter_year = pd.merge(df_combined_quarter, df_combined_year, on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)
        df_reduced_quarter_year = pd.merge(df_combined_quarter, df_combined_year, on='code', how='inner', suffixes=('', '_x'))
        df_reduced_quarter_year = df_reduced_quarter_year[['code', 'name']]
        df_reduced_quarter_year = df_reduced_quarter_year.drop_duplicates().reset_index().drop('index', axis=1)

        #
        #''' No need to filter every df with df_reduced_quarter_year which are the quarter and yearly good stock
        for a in arr:
            logging.info(__file__+" "+"=== Inner Merge  " + a + " with df_reduced_quarter_year ====")
            #cmd = "pd.merge(" + a + ", df_reduced_quarter_year, on='code',how='inner',suffixes=('','_x')).drop('name_x', axis=1)"
            cmd = "pd.merge(" + a + ", df_reduced_quarter_year, on='code',how='inner',suffixes=('','_x'))"

            tmp_df = eval(cmd)
            logging.info(__file__+" "+"length after merge :" + str(tmp_df.__len__()))

            s = a + " = tmp_df"  #df_max_daily_increase = tmp_df
            exec(s)
        #'''

        arr.append('df_reduced_quarter_year')

        pass

    df_daily = finlib.Finlib().get_last_n_days_daily_basic(ndays=1, dayE=finlib.Finlib().get_last_trading_day())
    df_ts_all = finlib.Finlib().add_ts_code_to_column(df=finlib.Finlib().load_fund_n_years())
    # df_ts_all = self.load_all_ts_pro()

    dict_df = {}

    logging.info(__file__+" "+"------ Combination 1 -------")
    for a in arr:
        logging.info(__file__+" "+"=== " + a + " ====")
        if (a in ('df_zz500', 'df_sme', 'df_hs300', 'df_gem', 'sz50')):
            logging.info(__file__+" "+"skip list set: " + a)
            continue

        #cmd = a+".__len__()"
        #len= eval(cmd)
        #len=str(len)
        tmp = eval(a)
        tmp = combin_filter(tmp, post_combine=True, debug=debug)

        if tmp.__len__() == 0:
            logging.info(__file__+" "+"empty " + a)
            continue

        if a == 'df_fund_2':
            pass

        if ('date' in tmp.columns):
            tmp = tmp[tmp['date'] >= day_3_before_date_ymd]

        logging.info(__file__+" "+"sorting " + a)
        tmp = my_sort(tmp, debug=debug)
        tmp = finlib.Finlib().add_amount_mktcap(df=tmp)
        tmp = finlib.Finlib().add_tr_pe(df=tmp,df_daily=df_daily, df_ts_all=df_ts_all)
        tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')

        len = str(tmp.__len__())


        #tmp_df=tmp_df.head(10)  #list all, as the final result should be keep for a long time. archive.

        long_term = 0
        middle_term = 0
        short_term = 0

        cheap_cnt = 0
        expensive_cnt = 0

        if df_dict[a]['term'] == "LONG TERM":
            long_term += 1
        elif df_dict[a]['term'] == "MIDDLE TERM":
            middle_term += 1
        elif df_dict[a]['term'] == "SHORT TERM":
            short_term += 1

        if df_dict[a]['price'] == "CHEAP":
            cheap_cnt += 1
        elif df_dict[a]['price'] == "EXPENSIVE":
            expensive_cnt += 1

        dict_df[a] = tmp
        if tmp.__len__() > 0:
            rst = "\n==== " + str(cheap_cnt) + " cheap " + str(expensive_cnt) + " exp, " + str(short_term) + "s " + str(middle_term) + "m " + str(long_term) + "l " + "len " + len + ". " + a
            #rst += "\n"+str(tmp) + "\n"

            rst += "\n" + tabulate(tmp, headers='keys', tablefmt='psql', disable_numparse=True) + "\n"

            fh = open(rpt, "a")
            fh.write(rst)
            fh.close()
            logging.info(rst)

    for combi in range(2, arr.__len__() + 1):
        #def _proc_combination(arr, combi, skip_sets, df_dict, day_3_before_date, debug, rpt):
        logging.info(__file__+" "+"------ Combination " + str(combi) + " -------")
        for subset in itertools.combinations(arr, combi):
            if ('df_low_price_year' in subset) and ('df_high_price_year' in subset):
                continue

            if ('df_low_vol_year' in subset) and ('df_high_vol_year' in subset):
                continue

            # every(all) subset are in skipset
            if subset.__len__() == set(subset).intersection(set(skip_sets)).__len__():
                logging.info(__file__+" "+"skip list set combination: " + " ".join(set(subset)))
                continue

            long_term = 0
            middle_term = 0
            short_term = 0

            cheap_cnt = 0
            expensive_cnt = 0

            for sub_item in subset:
                if df_dict[sub_item]['term'] == "LONG TERM":
                    long_term += 1
                elif df_dict[sub_item]['term'] == "MIDDLE TERM":
                    middle_term += 1
                elif df_dict[sub_item]['term'] == "SHORT TERM":
                    short_term += 1

                if df_dict[sub_item]['price'] == "CHEAP":
                    cheap_cnt += 1
                elif df_dict[sub_item]['price'] == "EXPENSIVE":
                    expensive_cnt += 1

            comb_df_name = subset[0]
            for _ in range(1, subset.__len__() - 1):
                comb_df_name += "_" + subset[_]
            # print(comb_df_name)

            if comb_df_name in dict_df.keys():
                tmp = dict_df[comb_df_name]
                #logging.info(("reuse saved combined df " + comb_df_name))
            else:
                #logging.info(__file__+" "+"combination no instance, " + comb_df_name)
                continue

            dup_suffix = "_y" + str(subset.__len__() - 2)
            tmp = eval("pd.merge(tmp," + subset[subset.__len__() - 1] + ", on='code',how='inner',suffixes=('','" + dup_suffix + "'))")
            tmp = combin_filter(tmp, post_combine=True, debug=debug)

            if 'date' in tmp.columns:
                tmp = tmp[tmp['date'] >= day_3_before_date_ymd]

            tmp = my_sort(tmp, debug=debug)
            tmp = finlib.Finlib().add_amount_mktcap(df=tmp)
            tmp = finlib.Finlib().add_tr_pe(df=tmp,df_daily=df_daily, df_ts_all=df_ts_all)

            tmp = finlib.Finlib().df_format_column(df=tmp, precision='%.1e')

            comb_df_name = comb_df_name + "_" + subset[subset.__len__() - 1]

            if tmp.__len__() > 0:
                dict_df[comb_df_name] = tmp
                logging.info(("saved combined df " + comb_df_name))
                rst = "\n==== " + str(cheap_cnt) + " cheap " \
                      + str(expensive_cnt) + " exp, " \
                      + str(short_term) + "s " \
                      + str(middle_term) + "m " \
                      + str(long_term) + "l " \
                      + "len " + str(tmp.__len__()) + ". " + ", ".join(set(subset))

                rst += "\n" + tabulate(tmp, headers='keys', tablefmt='psql', disable_numparse=True) + "\n"
                fh = open(rpt, "a")
                fh.write(rst)
                fh.close()
                logging.info(rst)

    logging.info(("result saved to " + rpt))
    logging.info(__file__+" "+"script completed")

    os._exit(0)


def ana_result():
    exam_date = finlib.Finlib().get_last_trading_day()
    input_f = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + ".txt"

    output_f = "/home/ryan/DATA/result/report_" + exam_date + "_" + stock_global + "_short.csv"
    df_rpt_short = pd.DataFrame(columns=['code', 'name', 'cnt'])
    dict = {}

    if not os.path.exists(input_f):
        logging.info(__file__+" "+"file not exist, " + input_f)
        return

    fh = open(input_f, "r")
    logging.info(__file__+" "+"loading " + input_f)

    skip_section = False

    for line in fh:
        #print(line)

        if re.match('.* code .*', line):
            continue

        if re.match("====", line):
            if re.match("====.* 0 exp", line):
                skip_section = False
            else:
                skip_section = True  #skip count expensive results

        if skip_section:
            continue

        lst = line.split('|')

        if lst.__len__() >= 4:
            code = lst[2]
            name = lst[3]

            #if code in dict.keys(): #python2
            if code in list(dict.keys()):  #python3
                dict[code]['cnt'] += 1
            else:
                dict[code] = {'cnt': 1, 'name': name}

        pass
    fh.close()
    df_rpt_short = pd.DataFrame.from_dict(dict).T.reset_index().rename(columns={"index": "code"})
    df_rpt_short = df_rpt_short.sort_values('cnt', ascending=False, inplace=False)
    df_rpt_short.to_csv(output_f, encoding='UTF-8', index=False)
    logging.info(__file__ + ": " + "saved " + output_f + " . len " + str(df_rpt_short.__len__()))


def main():
    global stock_global

    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)
    # logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(A G)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("-f", "--full_combination", action="store_true", default=False, dest="full_combination", help="combine all the df from 2 to 12, Total df 24. Using with -x AG,  Report size could be 6GB ")

    parser.add_option("-d", "--debug", action="store_true", default=False, dest="debug", help="skip refine_df sub, ")
    parser.add_option("--select", action="store_true", default=False, dest="select", help="Analyze selected stocks only")

    parser.add_option("--action", dest="action", help="[generate_report|analyze_report] ")

    (options, args) = parser.parse_args()

    stock_global = options.stock_global
    full_combination = options.full_combination
    debug = options.debug
    action = options.action
    select = options.select

    if select:
        logging.info(__file__+" "+"select flag is set, stock_global flag will be ignored.")

    if (not select) and (stock_global is None):
        logging.info(__file__+" "+"-x --stock_global is None, check help for available options, program exit")
        exit(0)

    if (full_combination) and (stock_global != 'AG'):
        logging.info(__file__+" "+"--full_combination using with -x AG only")
        exit(0)

    if action == 'generate_report':
        generate_result_csv(full_combination=full_combination, select=select, debug=debug)
    elif action == 'analyze_report':
        ana_result()


### MAIN ####
if __name__ == '__main__':
    main()
