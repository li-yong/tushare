# coding: utf-8

import os
import tushare as ts
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import finlib
from optparse import OptionParser
import logging

####  regenerated font cache
#import matplotlib.font_manager
#matplotlib.font_manager._rebuild()

# instal font in Ubuntu
# https://help.ubuntu.com/community/Fonts#Chinese.2C_Japanese.2C_and_Korean_Fonts
# sudo apt-get install ttf-wqy-microhei
# fc-list |grep wqy
# /usr/share/fonts/truetype/wqy/wqy-microhei.ttc: 文泉驿微米黑,文泉驛微米黑,WenQuanYi Micro Hei:style=Regular
# /usr/share/fonts/truetype/wqy/wqy-microhei.ttc: 文泉驿等宽微米黑,文泉驛等寬微米黑,WenQuanYi Micro Hei Mono:style=Regular


#### list the font matplot can use. #
# <Font 'WenQuanYi Micro Hei' (wqy-microhei.ttc) normal normal 400 normal>,

#matplotlib.font_manager.fontManager.ttflist

#use the font
logging.getLogger('matplotlib.font_manager').disabled = True
plt.rcParams['font.family'] = ['WenQuanYi Micro Hei']

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import logging
logging.getLogger('matplotlib.font_manager').disabled = True
# ---- Read , test


def check_fibo(df, code, name, begin_date='20180101', show_fig_f=False, save_fig_f=False, min_sample=500):

    rtn_dict = {}

    df = df[df['date'] >= str(begin_date)]
    #df = df[df['date'] >= pd.Timestamp(datetime.date.fromisoformat())]

    #print(df.__len__())

    df = df[df['close'] >= 0.1].reset_index().drop('index', axis=1)  #some csv close price is 0.0 or 0
    #print(df.__len__())

    if df.__len__() < min_sample:
        #print("code "+code+", name "+ name+". no enough record. len "+str(df.__len__()))
        return (rtn_dict)

    r = finlib.Finlib().fibonocci(df, cri_percent=5, cri_hit=0.01)

    y_axis = np.array(df['close'])
    x_axis = np.array(df['date'])

    the_day = pd.to_datetime(x_axis[-1]).strftime("%Y%m%d")

    #if not r['hit']:
    #    print("code " + code + ", name " + name
    #          + ", hit " + str(r['hit']))

    #if r['hit'] or True:
    if True:
        #print("code " + code + ", name " + name
        #      + ", hit " + str(r['hit'])
        #      + ", price " + str(r['pri_cur'])
        #      + ", percent " + str(r['per_cur'])
        #      + ", history hit " + str(r['current_hit_cnt'])
        #      )

        suggestion = code + " " + name+" long at " + str( r['long_enter_price'])\
                      + ", tp " + str(r['long_take_profit_price'])\
                      + ", sl " + str(r['long_stop_lost_price'])\
                      + ", tpp " + str(r['long_take_profit_percent'])\
                      + ", slp " + str(r['long_stop_lost_percent'])

        logging.info(__file__+" "+"suggestion: " + suggestion)

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
            if r['hit']:
                fn = "/home/ryan/DATA/result/fib_plot/" + code + "_" + name + "_" + the_day + "_hitted.png"
            else:
                fn = "/home/ryan/DATA/result/fib_plot/" + code + "_" + name + "_" + the_day + ".png"

            fig.savefig(fn, bbox_inches='tight')
            print("figure saved to " + fn + "\n")

        if show_fig_f:
            plt.show()

        plt.close('all')
        plt.clf()

        if r['closest'] in ["NA", "00"]:
            #logging.ingo(__file__+" "+"ignore as it's NA or at the lowest 00 line")
            return (rtn_dict)

        rtn_dict['code'] = code
        rtn_dict['name'] = name
        rtn_dict['date'] = the_day
        rtn_dict['cur_price'] = r['pri_cur']
        rtn_dict['p_max'] = r['p_max']
        rtn_dict['p_min'] = r['p_min']
        rtn_dict['percent'] = r['per_cur']
        rtn_dict['long_take_profit_percent'] = r['long_take_profit_percent']
        rtn_dict['long_stop_lost_percent'] = r['long_stop_lost_percent']
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

    parser.add_option("-b", "--begin_date", type="string", action="store", dest="begin_date_f", default="2018-01-01", help="begin date to use Fibo")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=500, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")

    parser.add_option("--save_fig", action="store_true", dest="save_fig_f", default=False, help="save the matplot figure ")

    parser.add_option("--show_fig", action="store_true", dest="show_fig_f", default=False, help="display the matplot figure ")

    parser.add_option("--log_price", action="store_true", dest="log_price_f", default=False, help="log for y-axis price ")

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

    #df_rtn = pd.DataFrame()
    df_rtn = pd.DataFrame(columns=["code", "name"])

    (options, args) = parser.parse_args()
    debug_f = options.debug_f
    begin_date_f = options.begin_date_f
    show_fig_f = options.show_fig_f
    save_fig_f = options.save_fig_f
    min_sample_f = options.min_sample_f
    log_price_f = options.log_price_f
    selected = options.selected
    stock_global = options.stock_global

    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']  #stock_list has already been removed garbage.
    out_f = out_dir + "/" + stock_global.lower() + "_fib.csv"  #/home/ryan/DATA/result/selected/us_index_fib.csv

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    i = 0

    for index, row in stock_list.iterrows():
        i += 1
        print(str(i) + " of " + str(stock_list.__len__()) + " ", end="")
        name, code = row['name'], row['code']

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(csv_f)

        if not os.path.isfile(csv_f):
            logging.warning(__file__+" "+"file not exist. " + csv_f)
            continue

        df = finlib.Finlib().regular_read_csv_to_stdard_df(csv_f,add_market=False)

        if (df.__len__() < min_sample_f):
            continue

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
