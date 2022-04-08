# coding: utf-8

import os
import tushare as ts
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import finlib
from optparse import OptionParser

####  regeneated font cache
#import matplotlib.font_manager
#matplotlib.font_manager._rebuild()

#### list the font matplot can use. #
# <Font 'AaTEST (Non-Commercial Use)' (AaDouBanErTi-2.ttf) normal normal 400 normal>,
# <Font 'Noto Serif CJK JP' (NotoSerifCJK-Black.ttc) normal normal black normal>,
# matplotlib.font_manager.fontManager.ttflist

#use the font
plt.rcParams['font.family'] = ['WenQuanYi Micro Hei']

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import logging
logging.getLogger('matplotlib.font_manager').disabled = True
# ---- Read , test


def check_fibo(df, code, name, begin_date='2018-01-01', show_fig_f=False, save_fig_f=False, min_sample=500):

    rtn_dict = {}

    df = df[df['date'] >= pd.Timestamp(datetime.date.fromisoformat(begin_date))]

    #print(df.__len__())

    df = df[df['close'] >= 0.1].reset_index().drop('index', axis=1)  #some csv close price is 0.0 or 0
    #print(df.__len__())

    if df.__len__() < min_sample:
        #print("code "+code+", name "+ name+". no enough record. len "+str(df.__len__()))
        return (rtn_dict)

    r = finlib.Finlib().fibonocci(df, cri_percent=5, cri_hit=0.01)

    if r['closest'] in ["NA", "00"]:
        #print("ignore as it's NA or at the lowest 00 line")
        return (rtn_dict)

    y_axis = np.array(df['close'])
    x_axis = np.array(df['date'])

    the_day = pd.to_datetime(x_axis[-1]).strftime("%Y-%m-%d")

    #if not r['hit']:
    #    print("code " + code + ", name " + name
    #          + ", hit " + str(r['hit']))

    if r['hit']:
        #print("code " + code + ", name " + name
        #      + ", hit " + str(r['hit'])
        #      + ", price " + str(r['pri_cur'])
        #      + ", percent " + str(r['per_cur'])
        #      + ", history hit " + str(r['current_hit_cnt'])
        #      )

        suggestion = code + " " + name+" long at " + str( r['long_enter_price'])\
                      + ", tp " + str(r['long_take_profit_price'])\
                      + ", sl " + str(r['long_stop_lost_price'])

        print("suggestion: " + suggestion)

        fig, ax = plt.subplots()
        ax.plot(x_axis, y_axis)
        plt.axhline(y=r['p00'], label=r['p00'])
        plt.axhline(y=r['p23'])
        plt.axhline(y=r['p38'])
        plt.axhline(y=r['p50'])
        plt.axhline(y=r['p61'])
        plt.axhline(y=r['p100'])

        plt.ylabel("Price")

        style = dict(size=15, color='black')
        ax.text(x_axis[-1], r['p00'], "0% " + str(r['p00']) + " hit " + str(r['p00_cnt']['sum_cnt']), **style)
        ax.text(x_axis[-1], r['p23'], "23.6% " + str(r['p23']) + " hit " + str(r['p23_cnt']['sum_cnt']), **style)
        ax.text(x_axis[-1], r['p38'], "38.2% " + str(r['p38']) + " hit " + str(r['p38_cnt']['sum_cnt']), **style)
        ax.text(x_axis[-1], r['p50'], "50% " + str(r['p50']) + " hit " + str(r['p50_cnt']['sum_cnt']), **style)
        ax.text(x_axis[-1], r['p61'], "61.8% " + str(r['p61']) + " hit " + str(r['p61_cnt']['sum_cnt']), **style)
        ax.text(x_axis[-1], r['p100'], "100% " + str(r['p100']) + " hit " + str(r['p100_cnt']['sum_cnt']), **style)

        plt.title(the_day + "_" + code + " " + name + " " + str(y_axis[-1]))

        props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
        ax.text(0.05, 0.95, suggestion, transform=ax.transAxes, fontsize=14, verticalalignment='top', bbox=props)

        if save_fig_f:
            fn = "/home/ryan/DATA/result/fib_plot/" + code + "_" + name + "_" + the_day + ".png"
            fig.savefig(fn, bbox_inches='tight')
            print("figure saved to " + fn + "\n")

        if show_fig_f:
            plt.show()

        plt.close('all')
        plt.clf()

        rtn_dict['code'] = code
        rtn_dict['name'] = name
        rtn_dict['date'] = the_day
        rtn_dict['cur_price'] = r['pri_cur']
        rtn_dict['percent'] = r['per_cur']
        rtn_dict['long_in_p'] = r['long_enter_price']
        rtn_dict['long_tp_p'] = r['long_take_profit_price']
        rtn_dict['long_sl_p'] = r['long_stop_lost_price']
        rtn_dict['hit_sum'] = r['current_hit_cnt']['sum_cnt']
        rtn_dict['h_cnt'] = r['current_hit_cnt']['h_cnt']
        rtn_dict['l_cnt'] = r['current_hit_cnt']['l_cnt']
        rtn_dict['o_cnt'] = r['current_hit_cnt']['o_cnt']
        rtn_dict['c_cnt'] = r['current_hit_cnt']['c_cnt']

    return (rtn_dict)


def main():
    ########################
    #
    #########################

    parser = OptionParser()

    parser.add_option("-v", "--verify", action="store_true", dest="verify_fibo_f", default=True, help="verify if current price hit Fibo serie")

    parser.add_option("-b", "--begin_date", type="string", action="store", dest="begin_date_f", default="2018-01-01", help="begin date to use Fibo")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=500, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")

    parser.add_option("--save_fig", action="store_true", dest="save_fig_f", default=False, help="save the matplot figure ")

    parser.add_option("--show_fig", action="store_true", dest="show_fig_f", default=False, help="display the matplot figure ")

    parser.add_option("-i", "--index", action="store_true", dest="index_f", default=False, help="verify the index SH000001 etc ")

    parser.add_option("--log_price", action="store_true", dest="log_price_f", default=False, help="log for y-axis price ")

    df_rtn = pd.DataFrame()

    (options, args) = parser.parse_args()
    verify_fibo_f = options.verify_fibo_f
    debug_f = options.debug_f
    index_f = options.index_f
    begin_date_f = options.begin_date_f
    show_fig_f = options.show_fig_f
    save_fig_f = options.save_fig_f
    min_sample_f = options.min_sample_f
    log_price_f = options.log_price_f

    if index_f:
        out_f = "/home/ryan/DATA/result/fib_index.csv"
        d = {
            'code': ['000001.SH', '000300.SH', '000905.SH'],
            'name': ['上证综指', '沪深300', '中证500'],
        }
        stock_list = pd.DataFrame(data=d)
        #stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  # 603999.SH
    else:
        out_f = "/home/ryan/DATA/result/fib.csv"
        stock_list = finlib.Finlib().get_A_stock_instrment()  #603999
        stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False)  #603999.SH
        stock_list = finlib.Finlib().remove_garbage(stock_list, code_field_name='code', code_format='C2D6')

    #debug_f = True
    #verify_fibo_f = True

    if debug_f:
        stock_list = stock_list[stock_list['code']=="SH600519"]
        # stock_list = stock_list[stock_list['code'] == "SZ300350"]

    csv_dir = "/home/ryan/DATA/DAY_Global/AG_qfq"
    if index_f:
        csv_dir = "/home/ryan/DATA/DAY_Global/AG_INDEX"

    i = 0
    for index, row in stock_list.iterrows():
        i += 1
        print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        name, code = row['name'], row['code']

        csv_f = csv_dir + "/" + code + ".csv"
        print(csv_f)

        if not os.path.exists(csv_f):
            print("csv_f not exist, " + csv_f)
            continue

        if verify_fibo_f:
            if index_f:
                df = pd.read_csv(csv_f, skiprows=1, header=None, names=['code', 'date', 'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'], converters={'code': str})
                df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y%m%d'))

                #check_fibo(df, code, name,begin_date_f,show_fig_f)
            else:
                df = pd.read_csv(csv_f, skiprows=1, header=None, names=['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'ratio'], converters={'code': str})

                if (df.__len__() < min_sample_f):
                    continue

                # date int to datetime
                df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y-%m-%d'))

                code_name_map = stock_list

                code = df.iloc[0]['code']
                name = code_name_map[code_name_map['code'] == code].iloc[0]['name']

            if log_price_f:
                df['open'] = df['open'].apply(lambda _d: np.log(_d))
                df['close'] = df['close'].apply(lambda _d: np.log(_d))
                df['high'] = df['high'].apply(lambda _d: np.log(_d))
                df['low'] = df['low'].apply(lambda _d: np.log(_d))

            rtn_dict_t = check_fibo(df, code, name, begin_date_f, show_fig_f, save_fig_f, min_sample_f)
            df_t = pd.DataFrame(data=rtn_dict_t, index=[0])
            #print(df_t)
            if not df_t.empty:
                df_rtn = pd.concat([df_rtn, df_t], sort=False).reset_index().drop('index', axis=1)

    df_rtn.to_csv(out_f, encoding='UTF-8', index=False)
    print(df_rtn)
    print("output saved to " + out_f)

    exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
