# coding: utf-8

import os
import tushare as ts
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import finlib
from optparse import OptionParser

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import logging
logging.getLogger('matplotlib.font_manager').disabled = True
# ---- Read , test


def check_fibo(df,code_name_map):
    df = df[df['date']>= pd.Timestamp(datetime.date.fromisoformat('2018-01-01'))]
    code = df.iloc[0]['code']
    name = code_name_map[code_name_map['code']==code].iloc[0]['name']

    if df.__len__()<400:
        print("code "+code+", name "+ name+". no enough record. len "+str(df.__len__()))
        return

    r = finlib.Finlib().fibonocci(df,cri_percent=5)


    y_axis = np.array(df['close'])
    x_axis = np.array(df['date'])

    print("code "+code+", name "+ name
          +", hit "+str(r['hit'])
          + ", price " +str(r['pri_cur'])
          + ", percent " +str(r['per_cur'])
          + ", history hit "+str(r['current_hit_cnt'])
          )

    if r['hit']:
        #plt.rcParams['font.sans-serif'] = ['Source Han Sans TW', 'sans-serif']
        plt.rcParams["font.family"] = ["Noto Sans Mono CJK HK"]

        fig, ax = plt.subplots()
        ax.plot(x_axis, y_axis)
        plt.axhline(y=r['p00'])
        plt.axhline(y=r['p23'])
        plt.axhline(y=r['p38'])
        plt.axhline(y=r['p50'])
        plt.axhline(y=r['p61'])
        plt.axhline(y=r['p100'])

        plt.title(code+" "+name)
        #plt.show()


def main():
    ########################
    #
    #########################

    parser = OptionParser()


    parser.add_option( "-v", "--verify", action="store_true",
                      dest="verify_fibo_f", default=False,
                      help="verify if current price hit Fibo serie")


    parser.add_option( "-d", "--debug", action="store_true",
                      dest="debug_f", default=False,
                      help="debug ")

    (options, args) = parser.parse_args()
    verify_fibo_f = options.verify_fibo_f
    debug_f = options.debug_f

    stock_list = finlib.Finlib().get_A_stock_instrment() #603999
    stock_list = finlib.Finlib().add_market_to_code(stock_list, dot_f=False, tspro_format=False) #603999.SH
    df = finlib.Finlib().remove_garbage(stock_list, code_filed_name='code', code_format='C2D6')

    #debug_f = True
    verify_fibo_f = True

    if debug_f:
        df = df[df['code']=="SH600519"]

    for index, row in df.iterrows():
        name, code = row['name'], row['code']

        csv_f = "/home/ryan/DATA/DAY_Global/AG/"+code+".csv"

        if not os.path.exists(csv_f):
            print("csv_f not exist, "+csv_f)
            continue

        if verify_fibo_f:

            df = pd.read_csv(csv_f, skiprows=1, header=None, names=['code', 'date', 'open', 'high', 'low', 'close',
                                                                    'vol', 'amount', 'ratio'],
                             converters={'code': str})

            # date int to datetime
            df['date'] = df['date'].apply(lambda _d: datetime.datetime.strptime(str(_d), '%Y-%m-%d'))


            code_name_map = stock_list
            check_fibo(df, code_name_map)


    exit(0)


### MAIN ####
if __name__ == '__main__':
    main()
