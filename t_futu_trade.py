# coding: utf-8
#from futuquant import *
#import futuquant as ft
import time
import finlib
import finlib_indicator

from futu import *
# from futu.common.constant import *

import sys
import re
import pandas as pd
# import time
import tabulate
import os

import constant
import datetime
import pytz
import logging
import traceback
import math
import stockstats
import numpy as np

from optparse import OptionParser


from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# reduce webdriver session log for every request.
from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER
from selenium.webdriver.remote.remote_connection import logging as SELENIUM_logging

import yfinance as yf

SELENIUM_LOGGER.setLevel(SELENIUM_logging.ERROR)

logging.getLogger("FTConsoleLog").setLevel(logging.WARNING)  #
# logging.getLogger("FTFileLog").setLevel(logging.WARNING)  #
# logging.getLogger("Futu").setLevel(logging.WARNING)  #


import socket

def is_port_open(host,port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host,port))
    sock.close()

    if result == 0:
       logging.info("Port is open, host "+str(host)+" port "+str(port))
       return(True)
    else:
        logging.info("Port is not open, host " + str(host) + " port " + str(port))
        return(False)


def pprint(df):
    print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))


def buy_sell_stock_if_p_up_below_hourly_ma_minutely_check(
        code,
        name,
        k_renew_interval_second,
        simulator,
        trd_ctx_unlocked,
        ktype_short,
        ktype_long,
        dict_code,
        market,
        tri_bar_ma,
        tri_ma_ma,
        tri_abnormal_price,
        buy_only,
        sell_only,
    ):


    if simulator:
        trd_env = TrdEnv.SIMULATE
    else:
        trd_env = TrdEnv.REAL

    do_not_place_order = False
    do_not_place_order_reason = "None"

    _po = get_persition_and_order(trd_ctx=trd_ctx_unlocked, market=market, trd_env=trd_env)
    df_order_list = _po['order_list']
    df_position_list = _po['position_list']

    ###################
    # get order
    ###################

    last_buy_order_create_time = datetime.datetime.strptime('1979-01-04 01:01:01', "%Y-%m-%d %H:%M:%S")
    last_sell_order_create_time = datetime.datetime.strptime('1979-01-04 01:01:01', "%Y-%m-%d %H:%M:%S")


    orders = df_order_list[df_order_list['code']==code].reset_index().drop('index', axis=1)
    orders_buy = orders[orders['trd_side']=='BUY']
    orders_sell = orders[orders['trd_side']=='SELL']

    last_order = orders.sort_values(by="create_time", ascending=False).reset_index().drop('index', axis=1).head(1)
    last_sell_order = last_order
    last_buy_order = last_order

    if not orders_buy.empty:
        last_buy_order = orders_buy.sort_values(by="create_time", ascending=False).reset_index().drop('index', axis=1).head(1)
        last_buy_order_create_time = datetime.datetime.strptime(last_buy_order.create_time[0], "%Y-%m-%d %H:%M:%S")
        last_buy_order_string = finlib.Finlib().pprint(last_buy_order[['code', 'stock_name', 'trd_side', 'order_type','order_status','order_id' , 'qty' ,  'price' , 'create_time' ]])

    if not orders_sell.empty:
        last_sell_order = orders_sell.sort_values(by="create_time", ascending=False).reset_index().drop('index', axis=1).head(1)
        last_sell_order_create_time = datetime.datetime.strptime(last_sell_order.create_time[0], "%Y-%m-%d %H:%M:%S")
        last_sell_order_string = finlib.Finlib().pprint(last_sell_order[['code', 'stock_name', 'trd_side', 'order_type','order_status','order_id' , 'qty' ,  'price' , 'create_time' ]])

    if last_sell_order_create_time > last_buy_order_create_time and (last_sell_order is not None):
        last_order = last_sell_order
        last_order_string = last_sell_order_string
    elif last_buy_order_create_time > last_sell_order_create_time and (last_buy_order is not None):
        last_order = last_buy_order
        last_order_string = last_buy_order_string
    elif (not orders.empty) and (last_buy_order_create_time == last_sell_order_create_time):
        # raise Exception("the last buy and sell order creation time are equal.")
        logging.warning("the last buy and sell order creation time are equal.")


    # index 0 is the most recent order
    # orders = orders.sort_values(by="create_time", ascending=False).reset_index().drop('index', axis=1)

    # last_order = orders.iloc[0]

    # last_order.order_id #6226957295081580411
    # last_order.code #HK.09977
    # last_order.stock_name #凤祥股份
    # last_order.trd_side  # BUY
    # last_order.qty #1000.0
    # last_order.price #2.5
    # last_order.create_time #'2021-04-02 11:44:05'
    # last_order.order_status #SUBMITTED


    # US Market
    if code.startswith('US.'):
        last_buy_create_time_to_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) \
                                      - convert_dt_timezone(last_buy_order_create_time,
                                                            tz_in=pytz.timezone('America/New_York'),
                                                            tz_out=pytz.timezone('Asia/Shanghai'),
                                                    )


        last_sell_create_time_to_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) \
                                       - convert_dt_timezone(last_sell_order_create_time,
                                                             tz_in=pytz.timezone('America/New_York'),
                                                             tz_out=pytz.timezone('Asia/Shanghai'),
                                                    )


    # not a US market. HK Market
    else:
        last_buy_create_time_to_now = datetime.datetime.now() - last_buy_order_create_time
        last_sell_create_time_to_now = datetime.datetime.now() - last_sell_order_create_time

    if last_buy_create_time_to_now.seconds <= 60*60*4 or last_sell_create_time_to_now.seconds <= 60*60*4: # 4 hours
        if not simulator:
            logging.info(__file__ + " " + "code "+code+" "+name+ " placed an order in 4 hours, will not create more orders. Abort further processing")
            logging.info(__file__ + " lastest order" + last_order_string)
            do_not_place_order = True
            do_not_place_order_reason = "code " + code+" "+name + ", REAL env, placed order in 4 hours"
            return()

        elif simulator and (not last_order.empty) and (last_order.order_status[0] not in ('FILLED_ALL','FILLED_PART','CANCELLED_ALL')):
            logging.info(__file__ + " " + "code "+code+" "+name+" SIMULATOR but has no UNfilled order in 4 hours, will not create more orders. Abort further processing")
            logging.info(__file__ + " " + "latest order:\n"+last_order_string)
            do_not_place_order = True
            do_not_place_order_reason = "code " + code+" "+name + ", SIM env, UNfilled order in 4 hours"
            return()

        elif simulator:
            logging.info(__file__ + " " + "code "+code+" "+name+" SIMULATOR, ignore orders created in 4 hours. REAL will abort here.")


    ###################
    # get position
    ###################
    stock_lot_size = dict_code[code]['stock_lot_size']

    if not code in df_position_list['code'].to_list():
        # logging.info(__file__ + " " + "code " + code + " no position, will short on sell.")
        sell_slot_size_1_of_4_position = stock_lot_size

    else:

        position = df_position_list[df_position_list['code'] == code].reset_index().drop('index', axis=1)

        # cur_pos="current position:\n"+finlib.Finlib().pprint(position[['code','stock_name', 'qty','cost_price','can_sell_qty','position_side','unrealized_pl','realized_pl']])
        # logging.info(cur_pos)
        # position_qty = position.qty[0]
        position_can_sell_qty = position.can_sell_qty[0]
        position_cost_price = position.cost_price[0]  # cheng beng

        ###################
        # Buy/Sell position
        ###################
        stock_lot_size = dict_code[code]['stock_lot_size']
        sell_slot_size_1_of_4_position = int(round(position_can_sell_qty * 0.25 / stock_lot_size, 0) ) * stock_lot_size

        if sell_slot_size_1_of_4_position < stock_lot_size:
            sell_slot_size_1_of_4_position = stock_lot_size

        #trading one unit in REAL env.
        if (not simulator) and sell_slot_size_1_of_4_position > stock_lot_size:
            sell_slot_size_1_of_4_position = stock_lot_size

    ###################
    # evaluate p_ask with MA
    ###################
    p_current = dict_code[code]['p_last']
    time_current= dict_code[code]['update_time']

    p_last_bar_close = dict_code[code]['p_last_last']
    p_ask = dict_code[code]['p_ask']
    p_bid = dict_code[code]['p_bid']
    ma_short = dict_code[code]['short'][ktype_short]['ma']
    ma_long = dict_code[code]['long'][ktype_long]['ma']

    # ktype = dict_code[code]['ktype']
    ma_period_short = dict_code[code]['ma_period_short']
    ma_period_long = dict_code[code]['ma_period_long']
    last_bar_close = dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['last_bar'].iloc[0]['close']
    previous_ma_short = dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b0']
    previous_ma_short_time_key = dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b0_time_key']

    range = 0.00005

    if p_current < ma_short:
        sybmol_close_ma = "<"
    elif p_current ==  ma_short:
        sybmol_close_ma = "="
    else:
        sybmol_close_ma = ">"

    if last_bar_close < previous_ma_short:
        sybmol_close_ma_previous = "<"
    elif last_bar_close == previous_ma_short:
        sybmol_close_ma_previous = "="
    else:
        sybmol_close_ma_previous = ">"

    if ma_short*(1-range) < p_ask:
        symbol_Ask_ma = ">"
    elif ma_short*(1-range) == p_ask:
        symbol_Ask_ma = "="
    else:
        symbol_Ask_ma = "<" #To sell

    if ma_short*(1+range) < p_bid:
        symbol_Bid_ma = ">"
    elif ma_short*(1+range) == p_bid:
        symbol_Bid_ma = "="
    else:
        symbol_Bid_ma = "<" #To buy



    if last_bar_close > p_ask:
        symbol_Ask_lastClose = "<"
    elif last_bar_close == p_ask:
        symbol_Ask_lastClose = "<"
    else:
        symbol_Ask_lastClose = ">"

    if last_bar_close > p_bid:
        symbol_Bid_lastClose = "<"
    elif last_bar_close == p_ask:
        symbol_Bid_lastClose = "<"
    else:
        symbol_Bid_lastClose = ">"

    if ma_short > ma_long:
        symbol_ma_short_long = constant.TREND_UP
    elif ma_short == ma_long:
        symbol_ma_short_long = constant.NO_TREND
    else:
        symbol_ma_short_long = constant.TREND_DOWN

    # Will not run to this now. p_ask will be p_last if NA/0
    # if p_ask == 'N/A' or p_ask == 0:
    #     logging.info(__file__ + " " + "code " + code + ". ask price is "+ str(p_ask)+" . abort further processing.")
    #     return()
    p_delta = dict_code[code]['p_last'] - dict_code[code]['p_last_last']

    logging.info(__file__ +  " " + code+" "+name + " p_delta " +str(round(p_delta,2))+ " atr_14 " + str(round(dict_code[code]['short'][ktype_short]['atr_14'],2)))


    if dict_code[code]['p_last_last'] > 0 and dict_code[code]['p_last'] > 0 and dict_code[code]['short'][ktype_short]['atr_14'] > 0:

        if (not sell_only) and tri_abnormal_price and p_delta >0 and p_delta > dict_code[code]['short'][ktype_short]['atr_14']:
            logging.info(__file__ +  " " + code+" "+name + " "+ str(time_current)+" last_price "+ str(p_current)+ ". Abnormal Price SOAR !!  p_delta "+ str(p_delta)+" atr_14 "+ str(dict_code[code]['short'][ktype_short]['atr_14']))
            place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=p_bid, code=code, qty=stock_lot_size,trd_env=trd_env)

        if (not buy_only) and tri_abnormal_price and p_delta < 0 and abs(p_delta) > dict_code[code]['short'][ktype_short]['atr_14']:
            logging.info(__file__ +  " " + code+" "+name + " "+ str(time_current)+" last_price "+ str(p_current)+ ". Abnormal Price DROP !!  p_delta "+ str(p_delta)+" atr_14 "+ str(dict_code[code]['short'][ktype_short]['atr_14']))
            place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position,
                                   trd_env=trd_env)

    bma_short = dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']
    bma_long = dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']

    # SELL Condition: ASK cross down short MA.  a<><  a<=< . ask: minimal price seller willing to offer.
    if tri_bar_ma and (ma_short*(1-range) > p_ask > 0 ) and (bma_short['ma_b0'] > bma_short['close_b0'] >0) and (bma_short['close_b1'] > bma_short['ma_b1']):
        logging.info(__file__ +  " " + code+" "+name + " "+ str(time_current)+" last_price "+ str(p_current)+ ". ALERT! p_ask " + str(p_ask) + " across DOWN "+"MA_"+ktype_short+"_"+str(ma_period_short) + " "+str(ma_short)
                     + ". proceeding to SELL"
                     + ".  Previous "+str(bma_short['ma_b0_time_key']) +" close "+str(bma_short['close_b0']) + " ma "+str(bma_short['ma_b0'])
                     )
        if do_not_place_order:
            logging.info("will not place order. do_not_place_order "+str(do_not_place_order)+", reason "+str(do_not_place_order_reason))
        elif last_sell_create_time_to_now.seconds < k_renew_interval_second[ktype_short]:
            logging.info("will not place order. last sell order in 180 sec "+str(last_sell_create_time_to_now.seconds))
        elif buy_only:
            logging.info("not sell as specified buy_only.")
        else:
            # beep, last 1sec, repeat 5 times.
            os.system("beep -f 555 -l 100 -r 1")
            place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position,
                                   trd_env=trd_env)

    # SELL Condition: short MA across down long MA.
    if tri_ma_ma and (bma_short['ma_b1'] >= bma_long['ma_b1']  > 0) and ( bma_long['ma_b0'] > bma_short['ma_b0']):
        logging.info(__file__ +  " " + code+" "+name +" ALERT! Fast MA across DOWN Slow MA. proceeding to SELL. "
                     +" slow "+ktype_long+"_"+str(ma_period_long) +" "+ str(bma_long['ma_b0'])
                     +", fast "+ktype_short+"_"+str(ma_period_short) +" "+ str(bma_short['ma_b0'])
                     +". "+ str(time_current)+" last_price " + str(p_current)
                     )
        if do_not_place_order:
            logging.info("will not place order. do_not_place_order "+str(do_not_place_order)+", reason "+str(do_not_place_order_reason))
        elif last_sell_create_time_to_now.seconds < k_renew_interval_second[ktype_short]:
            logging.info("will not place order. last sell order in 180 sec "+str(last_sell_create_time_to_now.seconds))
        elif buy_only:
            logging.info("not sell as specified buy_only.")
        else:
            # beep, last 1sec, repeat 5 times.
            os.system("beep -f 555 -l 100 -r 1")
            place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position,
                                   trd_env=trd_env)

    # BUY Condition: short MA across up long MA.
    if tri_ma_ma and (bma_long['ma_b1'] >= bma_short['ma_b1'] > 0) and ( bma_short['ma_b0'] > bma_long['ma_b0']):
        logging.info(__file__ +  " " + code+" "+name +" ALERT! Fast MA across UP Slow MA. proceeding to BUY. "
                     +" slow "+ktype_long+"_"+str(ma_period_long) +" "+ str(bma_long['ma_b0'])
                     +", fast "+ktype_short+"_"+str(ma_period_short) +" "+ str(bma_short['ma_b0'])
                     +". "+ str(time_current)+" last_price " + str(p_current)
                     )
        if do_not_place_order:
            logging.info(__file__ + " " + "code " + code+" "+name +" will not place order. do_not_place_order "+str(do_not_place_order)+", reason "+str(do_not_place_order_reason))
        elif last_buy_create_time_to_now.seconds < k_renew_interval_second[ktype_short]:
            logging.info(__file__ + " " + "code " + code+" "+name +" will not place order. last buy order in 180 sec "+str(last_buy_create_time_to_now.seconds))
        elif sell_only:
            logging.info("not buy as specified sell_only.")
        else:
            # beep, last 1sec, repeat 5 times.
            os.system("beep -f 555 -l 100 -r 1")
            place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=p_bid, code=code, qty=stock_lot_size,trd_env=trd_env)


    # BUY Condition: BID cross up short MA. b><>  b>=>.  bid: max price buyer willing to pay
    if tri_bar_ma and (p_bid > ma_short*(1+range) > 0) and (bma_short['close_b0'] >= bma_short['ma_b0']  > 0) and ( bma_short['ma_b1'] > bma_short['close_b1']):

        logging.info(__file__ + " "
                     + code+" "+name +" "+ str(time_current)+" last_price "+ str(p_current)+ ". ALERT! p_bid " + str(p_bid) + " across UP "+"MA_"+ktype_short+"_"+str(ma_period_short) +" "+ str(ma_short)
                     + ". proceeding to BUY"
                     + ".  Previous "+str(bma_short['ma_b0_time_key']) +" close "+str(bma_short['ma_b0'] ) + " ma "+str(bma_short['ma_b0'])
                     )
        if do_not_place_order:
            logging.info(__file__ + " " + "code " + code+" "+name +" will not place order. do_not_place_order "+str(do_not_place_order)+", reason "+str(do_not_place_order_reason))
        elif last_buy_create_time_to_now.seconds < k_renew_interval_second[ktype_short]:
            logging.info(__file__ + " " + "code " + code+" "+name +" will not place order. last buy order in 180 sec "+str(last_buy_create_time_to_now.seconds))
        elif sell_only:
            logging.info("not buy as specified sell_only.")
        else:
            # beep, last 1sec, repeat 5 times.
            os.system("beep -f 555 -l 100 -r 1")
            place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=p_bid, code=code, qty=stock_lot_size,trd_env=trd_env)


    logging.info('*************************************')


    logging.info(
        __file__ + " this_ck_done. " + code+" "+name + " "+symbol_ma_short_long+" "
        +  str(time_current)+" last_price "+ str(p_current)+", MA_"+ktype_short+"_"+str(ma_period_short)+" " + str(ma_short)
        + ", MA_" + ktype_long + "_" + str(ma_period_long) + " " + str(ma_long)
        +".  bid "+ str(p_bid)+ ", ask "+ str(p_ask)
        + ". ask "+symbol_Ask_ma +  symbol_Ask_lastClose
        + ", bid "+symbol_Bid_ma +  symbol_Bid_lastClose
        + ", close_ma_prev_vs_current "+sybmol_close_ma_previous +  sybmol_close_ma
        + ".  Previous "+str(previous_ma_short_time_key) +" close "+str(last_bar_close) + " ma "+str(previous_ma_short)
    )

    return()


def place_sell_market_order(trd_ctx, code, qty, trd_env):
    trd_ctx = get_ctx_from_code(trd_ctx,code)

    logging.info(__file__ + " place_sell_market_order " + "code " + code + " , qty " + str(qty) + " , trd_env " + str(trd_env))
    ret, order_table = trd_ctx.place_order(price=999999, qty=qty, code=code, trd_side=TrdSide.SELL,trd_env=trd_env, order_type=OrderType.MARKET)

    if not ret == RET_OK:
        logging.info(__file__+" "+"code "+code+" place_sell_market_order failed, "+str(order_table))
    else:
        logging.info(__file__ + " " + "code " + code + " place_sell_market_order, "
                     +"order_id " + str(order_table.order_id[0])
                     +" , qty " + str(order_table.qty[0])
                     +" , price " + str(order_table.price[0])
                     +" , trd_side " + str(order_table.trd_side[0])
                     +" , order_type " + str(order_table.order_type[0])
                     +" , order_status " + str(order_table.order_status[0])
                     +" , create_time " + str(order_table.create_time[0])
                     +" , code " + str(order_table.code[0])
                     +" , stock_name " + str(order_table.stock_name[0])
                     + " , trd_env " + str(trd_env)
                     )
    return(order_table)


def place_sell_limit_order(trd_ctx, code, price, qty, trd_env ):
    trd_ctx = get_ctx_from_code(trd_ctx,code)

    logging.info(__file__+" place_sell_limit_order "+"code "+code+" , price "+str(price)+" , qty "+str(qty)+" , trd_env "+str(trd_env))
    ret, order_table = trd_ctx.place_order(price=price, qty=qty, code=code, trd_side=TrdSide.SELL,trd_env=trd_env, order_type=OrderType.NORMAL)

    if not ret == RET_OK:
        logging.info(__file__+" "+"code "+code+" place_sell_limit_order failed, "+str(order_table))
    else:
        logging.info(__file__ + " " + "code " + code + " place_sell_limit_order, "
                     +"order_id " + str(order_table.order_id[0])
                     +" , qty " + str(order_table.qty[0])
                     +" , price " + str(order_table.price[0])
                     +" , trd_side " + str(order_table.trd_side[0])
                     +" , order_type " + str(order_table.order_type[0])
                     +" , order_status " + str(order_table.order_status[0])
                     +" , create_time " + str(order_table.create_time[0])
                     +" , code " + str(order_table.code[0])
                     +" , stock_name " + str(order_table.stock_name[0])
                     +" , trd_env " + str(trd_env)
                     )
    return()

def get_ctx_from_code(trd_ctx, code):
    if code.startswith('HK.'):
        rtn = trd_ctx['trd_ctx_hk']
    elif code.startswith('US.'):
        rtn = trd_ctx['trd_ctx_us']
    elif code.startswith('SH.'):
        rtn = trd_ctx['trd_ctx_cn']
    elif code.startswith('SZ.'):
        rtn = trd_ctx['trd_ctx_cn']
    else:
        logging.fatal('unknown code '+str(code))
    return(rtn)

def place_buy_limit_order(trd_ctx, code, price, qty, trd_env):
    trd_ctx = get_ctx_from_code(trd_ctx,code)

    logging.info(__file__ + " place_buy_limit_order " + "code " + code + " , price " + str(price) + " , qty " + str(qty) + " , trd_env " + str(trd_env))
    ret, order_table = trd_ctx.place_order(price=price, qty=qty, code=code, trd_side=TrdSide.BUY,trd_env=trd_env, order_type=OrderType.NORMAL)
    # print(finlib.Finlib().pprint(order_table))
    if not ret == RET_OK:
        logging.info(__file__+" "+"code "+code+" place_buy_limit_order failed, "+str(order_table))
    else:
        logging.info(__file__ + " " + "code " + code + " place_buy_limit_order, "
                     + "order_id " + str(order_table.order_id[0])
                     + " , qty " + str(order_table.qty[0])
                     + " , price " + str(order_table.price[0])
                     + " , trd_side " + str(order_table.trd_side[0])
                     + " , order_type " + str(order_table.order_type[0])
                     + " , order_status " + str(order_table.order_status[0])
                     + " , create_time " + str(order_table.create_time[0])
                     + " , code " + str(order_table.code[0])
                     + " , stock_name " + str(order_table.stock_name[0])
                     + " , trd_env " + str(trd_env)
                     )
    return(order_table)


def buy_limit(quote_ctx, trd_ctx, df_stock_info, code, drop_threshold=0.19, pwd_unlock='123456', trd_env=TrdEnv.SIMULATE, time_sleep=4):
    trd_ctx = get_ctx_from_code(trd_ctx,code)


    ###
    #ret, data = quote_ctx.get_market_snapshot(code)
    #df = df_market_snapshot[df_market_snapshot['code']==code]

    #if ret == RET_OK:
    lot_size = df_stock_info.iloc[0]['lot_size']
    last_price = df_stock_info.iloc[0]['last_price']
    prev_close_price = df_stock_info.iloc[0]['prev_close_price']

    price_to_order = last_price * (1 - drop_threshold)
    price_to_order = round(price_to_order, 2)
    #else:
    #    lot_size = 0
    #    price_to_order = 0

    logging.info(__file__+" "+"Placing buying limit order, " + code + ", price: " + str(price_to_order) + ", lot: " + str(lot_size) + ", env: " + str(trd_env))

    ret, order_table = trd_ctx.place_order(price=price_to_order, qty=lot_size, code=code, trd_side=TrdSide.BUY, trd_env=trd_env, order_type=order_type)

    if ret == RET_OK:
        print(". Done")
    else:
        print(". Failed: " + order_table)
    time.sleep(time_sleep)


def get_stock_basicinfo(host, port, stock_list=None, market=Market.HK, securityType=SecurityType.STOCK):
    quote_ctx = OpenQuoteContext(host=host, port=port)

    if stock_list == None:
        ret, data = quote_ctx.get_stock_basicinfo(market, securityType)
    else:
        ret, data = quote_ctx.get_stock_basicinfo(market, securityType, stock_list)

    quote_ctx.close()  # After
    return(data)

def get_current_price( host, port, code_list=['HK.00700']):
    quote_ctx = OpenQuoteContext(host=host, port=port)
    mkt_state = quote_ctx.get_global_state()

    ret, df_market_snapshot = quote_ctx.get_market_snapshot(code_list)
    quote_ctx.close()

    if ret != RET_OK:
        # "行情权限不足"
        #  Failed to get_market_snapshot, 此协议请求太频繁，触发了频率限制，请稍后再试
        raise Exception('Failed to get_market_snapshot, '+df_market_snapshot)

        # if df_market_snapshot.find("行情权限不足") > -1:
        #     pass #ignore this error
        # else:
        #     "行情权限不足"
        #     #Failed to get_market_snapshot, 此协议请求太频繁，触发了频率限制，请稍后再试
        #     raise Exception('Failed to get_market_snapshot, '+df_market_snapshot)

    return(df_market_snapshot)

def get_history_bar(host,port,code,start, end, ktype,extended_time=False):
    # extended_time = False  # Futu App calculate MA without extended time. Compliance with Futu App.

    # Even extended_time==True, HK.00700 didn't return extended time at the morning 9:00-9:20, not sure BMP previlege caused?
    # 218  HK.00700  2021-04-29 15:57:00 634.00 633.50 634.50 633.50      0.00           0.00    72200   45776100.00        -0.16      634.50
    # 219  HK.00700  2021-04-29 16:00:00 634.00 631.50 634.50 631.50      0.00           0.00  1879000 1186806500.00        -0.32      633.50
    # 220  HK.00700  2021-04-30 09:33:00 627.50 623.00 627.50 622.50      0.00           0.00  1082000  677740767.00        -1.35      631.50

    quote_ctx = OpenQuoteContext(host=host, port=port)

    max_count = 1000

    ls = 'code ' + str(code) + " ktype " + str(ktype) + " start " + str(start) + " end " + str(
        end) + ' extended_time ' + str(extended_time) + " max_count " + str(max_count)
    logging.info("request_history_kline " + ls)

    #每 30 秒内最多请求 60 次历史 K 线接口
    time.sleep(30/60)
    ret, data, page_req_key = quote_ctx.request_history_kline(
        code, ktype=ktype,
        start=start,
        end=end,
        extended_time=extended_time,
        max_count=max_count)  #

    if ret != RET_OK:
        quote_ctx.close()
        logging.fatal(__file__ + " " + 'error:', data)
        raise Exception("Error on request_history_kline. " + ls)

    while page_req_key != None:  # 请求后面的所有结果
        logging.info("fetching "+str(page_req_key))
        ret, data_n, page_req_key = quote_ctx.request_history_kline(code,
                                                                    ktype=ktype,
                                                                    start=start,
                                                                    end=end,
                                                                    extended_time=extended_time,
                                                                    max_count=max_count,
                                                                    page_req_key=page_req_key,
                                                                    )  # 请求翻页后的数据

        if ret != RET_OK:
            quote_ctx.close()
            logging.fatal(__file__ + " " + 'error:', data)
            raise Exception("Error on request_history_kline. " + ls)
        else:
            # data = data.append(data_n)
            data = pd.concat([data, data_n])

    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

    return(data)


def get_rt_ticker(host,port,market, debug):
    class TickerTest(TickerHandlerBase):
        def on_recv_rsp(self, rsp_pb):
            ret_code, data = super(TickerTest, self).on_recv_rsp(rsp_pb)
            if ret_code != RET_OK:
                print("TickerTest: error, msg: %s" % data)
                return RET_ERROR, data
            print("TickerTest ", data)  # TickerTest 自己的处理逻辑
            return RET_OK, data

    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    handler = TickerTest()
    quote_ctx.set_handler(handler)  # 设置实时逐笔推送回调
    # quote_ctx.subscribe(['HK.00700'], [SubType.TICKER])  # 订阅逐笔类型，FutuOpenD 开始持续收到服务器的推送
    quote_ctx.subscribe(['SH.600519'], [SubType.TICKER])  # 订阅逐笔类型，FutuOpenD 开始持续收到服务器的推送
    # quote_ctx.subscribe(['US.AAPL'], [SubType.TICKER])  # 订阅逐笔类型，FutuOpenD 开始持续收到服务器的推送
    time.sleep(120)  # 设置脚本接收 FutuOpenD 的推送持续时间为15秒
    quote_ctx.close()  # 关闭当条连接，FutuOpenD 会在1分钟后自动取消相应股票相应类型的订阅



def get_rt_ticker_rt(host,port,market, debug):
    quote_ctx = OpenQuoteContext(host=host, port=port)

    ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.TICKER], subscribe_push=False)
    # 先订阅逐笔类型。订阅成功后 FutuOpenD 将持续收到服务器的推送，False 代表暂时不需要推送给脚本
    if ret_sub == RET_OK:  # 订阅成功
        ret, data = quote_ctx.get_rt_ticker('HK.00700', 2)  # 获取港股00700最近2个逐笔
        if ret == RET_OK:
            print(data)
            print(data['turnover'][0])  # 取第一条的成交金额
            print(data['turnover'].values.tolist())  # 转为 list
        else:
            print('error:', data)
    else:
        print('subscription failed', err_message)
    quote_ctx.close()  # 关闭当条连接，FutuOpenD 会在1分钟后自动取消相应股票相应类型的订阅


def get_current_ma(host, port, code,k_renew_interval_second, ktype, ma_period=5, history_bar_df=None,bar_fetch_period=None ):
    if bar_fetch_period is None:
        bar_fetch_period = ma_period

    end = datetime.datetime.today().strftime("%Y-%m-%d")

    # suppose one day have 4 trading hours, 5 tradings days in a week
    start = (datetime.datetime.today() - datetime.timedelta(
        days=math.ceil(k_renew_interval_second[ktype] * bar_fetch_period * 7/ 5 / 4 / 60 / 60) + 2)).strftime("%Y-%m-%d")

    # if datetime.datetime.today().isoweekday() == 1 : #Monday, suppose one day have 4 trading hours, 5 tradings days in a week
    #     start = (datetime.datetime.today() - datetime.timedelta(
    #         days=  math.ceil(k_renew_interval_second[ktype] * bar_fetch_period / 4 / 60 / 60) + 2)).strftime("%Y-%m-%d")
    # else:
    #     start = (datetime.datetime.today() - datetime.timedelta(
    #         days=math.ceil(k_renew_interval_second[ktype] * bar_fetch_period / 4 / 60 / 60))).strftime("%Y-%m-%d")


    # data = get_history_bar(host, port, code=code, start=start, end=end, ktype=ktype, extended_time=False)

    # data are Bars, e.g K_3M.
    # data don't contain current time bar. eg.K_3M, at 9.54~9.57, bar 9.57 is growing, data return bars end at 9.54.

    if history_bar_df is None: #the 1st time run
        data = get_history_bar(host, port, code=code, start=start, end=end, ktype=ktype, extended_time=True) #ryan debug
    else:
        data = history_bar_df

    if data.__len__() < ma_period:
        logging.info(finlib.Finlib().pprint(data))
        logging.error("request_history_kline, data length "+str(data.__len__())+" less than ma_period "+str(ma_period))
        return(
            {
                'code': code,
                'rtn_code': RET_ERROR,
            }
        )
        # raise Exception("request_history_kline, data length "+str(data.__len__())+" less than ma_period "+str(ma_period))

    ma_b0 = round(data[-ma_period:]['close'].mean(),2)  # current MA value
    ma_b0_time_key = data.iloc[-1]['time_key']
    close_b0 = data.iloc[-1]['close']

    ma_b1 = round(data[-ma_period-1:-1]['close'].mean(),2)  # previous-1 MA value
    ma_b1_time_key = data.iloc[-2]['time_key']
    close_b1 = data.iloc[-2]['close']

    ma_b2 = round(data[-ma_period-2:-2]['close'].mean(),2)  # previous-2 MA value
    ma_b2_time_key = data.iloc[-3]['time_key']
    close_b2 = data.iloc[-3]['close']

    # ma_nsub1_sum = round(data[-ma_period:-1]['close'].sum(),2)
    ma_nsub1_sum = round(data[-ma_period+1:]['close'].sum(),2)  # use to calculate right_now MA.

    # logging.info('*************************************')
    # logging.info(__file__+" "+"code "+code+", ktype "+ktype+", ma_nsub1_sum "+str(ma_nsub1_sum)+", ma_period "+str(ma_period)+" , ma_b0 "+str(ma_b1)+" at "+ma_b0_time_key
    #              +" , ma_b1 "+str(ma_b1)+" at "+ma_b1_time_key
    #              +" , ma_b2 "+str(ma_b2)+" at "+ma_b2_time_key
    #              )

    # logging.info(finlib.Finlib().pprint(data[['code','time_key','close','volume', 'turnover_rate','turnover','last_close']].tail(1).reset_index().drop('index',axis=1)))

    return({
        'code':code,
        'rtn_code':0,
        'ktype':ktype,
        'ma_period':ma_period,
        'ma_b0':ma_b0,
        'ma_b0_time_key': ma_b0_time_key,
        'close_b0': close_b0,
        'ma_b1':ma_b1,
        'ma_b1_time_key': ma_b1_time_key,
        'close_b1': close_b1,
        'ma_b2':ma_b2,
        'ma_b2_time_key': ma_b2_time_key,
        'close_b2': close_b2,
        'ma_nsub1_sum':ma_nsub1_sum,
        'time_key':ma_b0_time_key,
        # 'time_key':data.iloc[-2]['time_key'],
        'last_bar': data.tail(1).reset_index().drop('index',axis=1),
        'df_history_bars':data, #['code', 'time_key', 'open', 'close', 'high', 'low', 'pe_ratio', 'turnover_rate', 'volume', 'turnover', 'change_rate', 'last_close']
    })

def convert_dt_timezone(datetime_in, tz_in=pytz.timezone('America/New_York'), tz_out=pytz.timezone('Asia/Shanghai')):
    # dt_out = datetime_in.replace(tzinfo=tz_in).astimezone(tz=tz_out) # #incorrect convert
    dt_out = tz_in.localize(datetime_in).astimezone(tz_out)

    # ny = pytz.timezone('America/New_York')
    # sh = pytz.timezone('Asia/Shanghai')
    # din = datetime.datetime(2021, 1, 1, 21, 00, 00)
    # a = ny.localize(din).astimezone(sh) # Convert correctly, and Daylight saving aware.
    # b = din.replace(tzinfo=ny).astimezone(sh) #incorrect convert


    return(dt_out)


def test():
    quote_ctx = OpenQuoteContext(host, port)
    # ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.BROKER], subscribe_push=False)
    ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.BROKER], subscribe_push=False)
    # 先订阅经纪队列类型。订阅成功后FutuOpenD将持续收到服务器的推送，False代表暂时不需要推送给脚本
    if ret_sub == RET_OK:   # 订阅成功
        ret, bid_frame_table, ask_frame_table = quote_ctx.get_broker_queue('HK.00700')   # 获取一次经纪队列数据
        if ret == RET_OK:
            print(finlib.Finlib().pprint(bid_frame_table))
            print(finlib.Finlib().pprint(ask_frame_table))
        else:
            print('error:', bid_frame_table)
    else:
        print('subscription failed')
    quote_ctx.close()   # 关闭当条连接，FutuOpenD会在1分钟后自动取消相应股票相应类型的订阅


    exit(0)
    #################################

    class BrokerTest(BrokerHandlerBase):
        def on_recv_rsp(self, rsp_pb):
            ret_code, err_or_stock_code, data = super(BrokerTest, self).on_recv_rsp(rsp_pb)
            if ret_code != RET_OK:
                print("BrokerTest: error, msg: {}".format(err_or_stock_code))
                return RET_ERROR, data
            print("BrokerTest: stock: {} data: {} ".format(err_or_stock_code, data))  # BrokerTest自己的处理逻辑
            return RET_OK, data
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    handler = BrokerTest()
    quote_ctx.set_handler(handler)  # 设置实时经纪推送回调
    quote_ctx.subscribe(['HK.00700'], [SubType.BROKER]) # 订阅经纪类型，FutuOpenD开始持续收到服务器的推送
    time.sleep(1000)  # 设置脚本接收FutuOpenD的推送持续时间为15秒
    quote_ctx.close()   # 关闭当条连接，FutuOpenD会在1分钟后自动取消相应股票相应类型的订阅

    #################################

def _get_trd_ctx(host="127.0.0.1", port=111111, market=['US','HK','AG']):
    trd_ctx_us = None
    trd_ctx_hk = None
    trd_ctx_cn = None

    if 'US' in market or 'US_HOLD' in market:
        trd_ctx_us = OpenUSTradeContext(host=host, port=port)

    if 'HK' in market or 'HK_HOLD' in market:
        trd_ctx_hk = OpenHKTradeContext(host=host, port=port)

    if 'AG' in market or 'AG_HOLD' in market:
        trd_ctx_cn = OpenCNTradeContext(host=host, port=port)

    if market.__len__() > 0 and ('US' not in market) and ('HK' not in market) and ('AG' not in market) \
            and ('US_HOLD' not in market) and ('HK_HOLD' not in market) and ('AG_HOLD' not in market):
        logging.fatal(__file__ + " " + "unknown market, support (US,HK,AG, US_HOLD, HK_HOLD, AG_HOLD). get " + str(market))
        raise Exception("unknown market, support (US,HK,AG). get " + str(market))

    return(
        {
            "trd_ctx_us":trd_ctx_us,
            "trd_ctx_hk":trd_ctx_hk,
            "trd_ctx_cn":trd_ctx_cn,
        }
           )


def _unlock_trd_ctx(trd_ctx,pwd_unlock):
    # ret, data = trd_ctx.unlock_trade(pwd_unlock)
    if trd_ctx['trd_ctx_us'] is not None:
        ret, data = trd_ctx['trd_ctx_us'].unlock_trade(pwd_unlock)
        if ret != RET_OK:
            logging.fatal(__file__ + " " + 'Failed to unlock trade US. ' + data)
            raise Exception('Failed to unlock trade US. '+data)

    if trd_ctx['trd_ctx_hk'] is not None:
        ret, data =trd_ctx['trd_ctx_hk'].unlock_trade(pwd_unlock)
        if ret != RET_OK:
            logging.fatal(__file__ + " " + 'Failed to unlock trade HK. ' + data)
            raise Exception('Failed to unlock trade HK. '+data)

    if trd_ctx['trd_ctx_cn'] is not None:
        ret, data =trd_ctx['trd_ctx_cn'].unlock_trade(pwd_unlock)
        if ret != RET_OK:
            logging.fatal(__file__ + " " + 'Failed to unlock trade CN. ' + data)
            raise Exception('Failed to unlock trade CN. '+data)


    return(trd_ctx)


def get_persition_and_order(trd_ctx,market,trd_env):
    df_order_list = pd.DataFrame()
    df_position_list = pd.DataFrame()

    if 'HK' in market or 'HK_HOLD' in market:
        #checking orders(in queue) 查询今日订单
        ret, df_order_list_hk = trd_ctx['trd_ctx_hk'].order_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Cannot get HK order info, "+df_order_list_hk)
        else:
            df_order_list = df_order_list.append(df_order_list_hk).reset_index().drop('index', axis=1)


        #checking postion
        ret, df_position_list_hk = trd_ctx['trd_ctx_hk'].position_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Failed to get HK position. "+df_position_list_hk)
        else:
            df_position_list = df_position_list.append(df_position_list_hk).reset_index().drop('index', axis=1)

    if 'US' in market or 'US_HOLD' in market:
        #checking orders(in queue) 查询今日订单
        ret, df_order_list_us = trd_ctx['trd_ctx_us'].order_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Cannot get US order info, "+df_order_list_us)
        else:
            df_order_list = df_order_list.append(df_order_list_us).reset_index().drop('index', axis=1)


        #checking postion
        ret, df_position_list_us = trd_ctx['trd_ctx_us'].position_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Failed to get US position. "+df_position_list_us)
        else:
            df_position_list = df_position_list.append(df_position_list_us).reset_index().drop('index', axis=1)

    # if 'SH' in mkt or 'SZ' in mkt or 'AG' in mkt:
    if 'AG' in market or 'AG_HOLD' in market:
        #checking orders(in queue) 查询今日订单
        ret, df_order_list_cn = trd_ctx['trd_ctx_cn'].order_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Cannot get CN order info, "+df_order_list_cn)
        else:
            df_order_list = df_order_list.append(df_order_list_cn).reset_index().drop('index', axis=1)

        #checking postion
        ret, df_position_list_cn = trd_ctx['trd_ctx_cn'].position_list_query(trd_env=trd_env)
        if ret != RET_OK:
            raise Exception("Failed to get CN position. "+df_position_list_cn)
        else:
            df_position_list = df_position_list.append(df_position_list_cn).reset_index().drop('index', axis=1)


    return(
        {   'market':market,
            'position_list':df_position_list,
            'order_list':df_order_list,
        }
    )


def hourly_ma_minutely_check(
        code,
        ktype_short,
        ktype_long,
        ma_period_short,
        ma_period_long,
        dict_code,
    ):


    ###################
    # get live price
    ###################
    dict_code[code]['stock_lot_size'] = dict_code[code]['df_live_price'].iloc[0]['lot_size']
    dict_code[code]['stock_daily_snap'] = dict_code[code]['df_live_price'].iloc[0]

    dict_code[code]['ktype_short'] = ktype_short
    dict_code[code]['ktype_long'] = ktype_long
    dict_code[code]['ma_period_short'] = ma_period_short
    dict_code[code]['ma_period_long'] = ma_period_long
    dict_code[code]['p_last_last'] = dict_code[code]['df_live_price']['last_price'].values[0]

    dict_code[code]['p_ask_last'] = dict_code[code]['df_live_price']['ask_price'].values[0]
    dict_code[code]['p_bid_last'] = dict_code[code]['df_live_price']['bid_price'].values[0]
    dict_code[code]['update_time_last'] = dict_code[code]['df_live_price']['update_time'].values[0]

    dict_code[code]['short'][ktype_short]['ma'] = round((dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_nsub1_sum']+ dict_code[code]['df_live_price'].iloc[0]['last_price'] ) / ma_period_short, 2)
    dict_code[code]['long'][ktype_long]['ma']= round((dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_nsub1_sum']+ dict_code[code]['df_live_price'].iloc[0]['last_price'] ) / ma_period_long, 2)
    dict_code[code]['p_last'] = dict_code[code]['df_live_price'].iloc[0]['last_price']  # seller want to sell at this price.
    dict_code[code]['update_time'] = dict_code[code]['df_live_price'].iloc[0]['update_time'] #buyer want to buy at this price.


    if dict_code[code]['df_live_price'].iloc[0]['ask_price'] in ['N/A', 0]:
        logging.info(__file__ + " " + "code " + code + " invalid ask price "+str(dict_code[code]['df_live_price'].iloc[0]['ask_price'])+" , use last_price "+ str( dict_code[code]['df_live_price'].iloc[0]['last_price'])+" as ask_price")
        dict_code[code]['p_ask'] = dict_code[code]['df_live_price'].iloc[0]['last_price']
    else:
        dict_code[code]['p_ask'] = dict_code[code]['df_live_price'].iloc[0]['ask_price']  # seller want to sell at this price.


    if dict_code[code]['df_live_price'].iloc[0]['bid_price']  in ['N/A', 0]:
        logging.info(__file__ + " " + "code " + code + " invalid bid price "+str(dict_code[code]['df_live_price'].iloc[0]['bid_price'])+" , use last_price "+ str( dict_code[code]['df_live_price'].iloc[0]['last_price'])+" as bid_price")
        dict_code[code]['p_bid'] = dict_code[code]['df_live_price'].iloc[0]['last_price']
    else:
        dict_code[code]['p_bid'] = dict_code[code]['df_live_price'].iloc[0]['bid_price']  # buyer want to buy at this price.



    logging.info("\n"+__file__ + " " + "code " + code + " "
                 +"MA_"+ktype_short+"_"+str(ma_period_short) +": ma/p_last "+ str(dict_code[code]['short'][ktype_short]['ma'])+"/"+str(dict_code[code]['p_last'])+" at "+dict_code[code]['df_live_price']['update_time'].values[0]
                 + " , ma_b0/close_b0 " + str(dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b0'])+"/"
                 +str( dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['close_b0']) + " at "
                 + dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b0_time_key']
                 + " , ma_b1/close_b1 " + str( dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b1']) +"/"
                 +str( dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['close_b1'])+ " at "
                 + dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b1_time_key']
                 + " , ma_b2/close_b2 " + str(dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b2'])+"/"
                 + str(dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['close_b2']) + " at "
                 +dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_b2_time_key']

                 +" p_ask "+str(dict_code[code]['p_ask'])
                 +" p_bid "+str(dict_code[code]['p_bid'])

                 +" updated "+str(dict_code[code]['update_time'])
                 )




    logging.info("\n"+__file__ + " " + "code " + code + " "
                 +"MA_"+ktype_long+"_"+str(ma_period_long) +": ma/p_last "+ str(dict_code[code]['long'][ktype_long]['ma'])+"/"+str(dict_code[code]['p_last'])+" at "+dict_code[code]['df_live_price']['update_time'].values[0]
                 + " , ma_b0/close_b0 " + str(dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b0'])+"/"
                 +str( dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['close_b0']) + " at "
                 + dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b0_time_key']
                 + " , ma_b1/close_b1 " + str( dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b1']) +"/"
                 +str( dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['close_b1'])+ " at "
                 + dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b1_time_key']
                 + " , ma_b2/close_b2 " + str(dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b2'])+"/"
                 + str(dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['close_b2']) + " at "
                 +dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_b2_time_key']

                 +" p_ask "+str(dict_code[code]['p_ask'])
                 +" p_bid "+str(dict_code[code]['p_bid'])

                 +" updated "+str(dict_code[code]['update_time'])
                 )


    return(dict_code)

def tv_init():
    opts = Options()
    # opts.add_argument("start-maximized")
    opts.add_argument("--log-level=0")
    # opts.headless = True
    # opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    browser = Chrome(options=opts)

    ######################################
    # Login TV and go to screener page
    ######################################
    browser = finlib_indicator.Finlib_indicator().tv_login(browser=browser, target_uri='https://tradingview.com/screener/')

    WebDriverWait(browser, 10).until(EC.title_contains("Screener"))
    return(browser)


def tv_monitor_minutely(browser, column_filed,interval,market,filter):
    finlib_indicator.Finlib_indicator().tv_screener_start(
        browser=browser,
        column_filed=column_filed,
        interval=interval,
        market=market,
        filter=filter
    )

    ######################################
    # Parse result to a dataframe
    ######################################
    select_date_time = datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
    df_result = finlib_indicator.Finlib_indicator().tv_save_result_table(browser=browser, market=market, parse_ticker_only=True)
    # print(finlib.Finlib().pprint(df_result.head(2)))

    df_result_a = pd.DataFrame.from_dict({
        'datetime':[select_date_time],
        'filter':[filter],
        "code_list":[','.join(df_result['code'].to_list())],
    })

    csv_f = "/home/ryan/DATA/result/tv_filter_monitor.csv"
    if os.path.isfile(csv_f):
        df = pd.read_csv(csv_f)
        df = df.append(df_result_a)
    else:
        df = df_result_a

    df.to_csv(csv_f, encoding='UTF-8', index=False)
    logging.info("TV filter "+filter+" output appened to "+csv_f +" ,stock numbers in result "+str(df_result.__len__()))
    return(df_result)




def get_chk_code_list(market,debug):

    rtn_list=[]

    # hold = "HOLD" in market

    if market == 'NASDAQ100':
        rtn_list = get_nasdaq100_list()['code'].to_list()
    elif 'US_HOLD' in market:
        rtn_list += _get_chk_code_list(market='US', debug=debug, hold=True)
    elif 'HK_HOLD' in market:
        rtn_list += _get_chk_code_list(market='HK', debug=debug, hold=True)
    elif 'AG_HOLD' in market:
        rtn_list += _get_chk_code_list(market='SH', debug=debug, hold=True)
        rtn_list += _get_chk_code_list(market='SZ', debug=debug, hold=True)
    elif 'US' in market:
        rtn_list += _get_chk_code_list(market='US', debug=debug, hold=False)
    elif 'HK' in market:
        rtn_list += _get_chk_code_list(market='HK', debug=debug, hold=False)
    elif 'AG' in market:
        rtn_list += _get_chk_code_list(market='SH', debug=debug, hold=False)
        rtn_list += _get_chk_code_list(market='SZ', debug=debug, hold=False)
    elif 'FUTU_CN_ETF' in market:
        rtn_list += _get_chk_code_list(market='FUTU_CN_ETF', debug=debug, hold=False)

    rtn_list = list(set(rtn_list))
    return(rtn_list)


def _get_chk_code_list(market,debug,hold=False):
    if market == Market.HK:
        if hold:
            stock_list = finlib.Finlib().get_stock_configuration(selected=True, stock_global='HK_HOLD')['stock_list']
        else:
            stock_list = finlib.Finlib().get_stock_configuration(selected=True, stock_global='HK')['stock_list']
        get_price_code_list = stock_list['code'].apply(lambda _d: 'HK.'+_d).to_list()
        if debug:
            get_price_code_list = ['HK.00700']
    elif market == Market.US:
        if hold:
            stock_list = finlib.Finlib().get_stock_configuration(selected=True, stock_global='US_HOLD')['stock_list']
        else:
            stock_list = finlib.Finlib().get_stock_configuration(selected=True, stock_global='US')['stock_list']
        get_price_code_list = stock_list['code'].apply(lambda _d: 'US.' + _d).to_list()
        # get_price_code_list = ['US.FUTU', 'US.AAPL']
        # get_price_code_list = ['US.FUTU', 'US.AAPL','US.MSFT','US.FB','US.TSLA','US.NVDA','US.WMT','US.HD','US.DIS',
        #                        'US.ADBE','US.PYPL','US.NFLX','US.KO','US.AMZN','US.GOOG','US.TSM',
        #                        'US.BABA','US.NIO','US.MCD','US.IBM','US.PDD','US.MMM','US.UBER']

        if debug:
            get_price_code_list = ['US.FUTU']
            # get_price_code_list = ['US.MDU']
    elif market == Market.SH:
        if hold:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list'])
        else:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')['stock_list'])
        _ = finlib.Finlib().add_market_to_code(df=_, dot_f=True, tspro_format=False)
        _ = _[_['code'].str.contains('SH')]['code']
        get_price_code_list = _.to_list()
        if debug:
            get_price_code_list = ['SH.688111']
    elif market == Market.SZ:
        if hold:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG_HOLD')['stock_list'])
        else:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='AG')['stock_list'])
        _ = finlib.Finlib().add_market_to_code(df=_, dot_f=True, tspro_format=False)
        _ = _[_['code'].str.contains('SZ')]['code']
        get_price_code_list = _.to_list()
        if debug:
            get_price_code_list = ['SZ.000001']
    elif market == 'FUTU_CN_ETF':
        if hold:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='FUTU_CN_ETF')['stock_list'])
        else:
            _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='FUTU_CN_ETF')['stock_list'])
        _ = finlib.Finlib().add_market_to_code(df=_, dot_f=True, tspro_format=False)
        _ = _['code']
        get_price_code_list = _.to_list()
        if debug:
            get_price_code_list = ['SZ.000001']
    else:
        logging.fatal("Unknow market. "+str(market))

    return(get_price_code_list)


def get_quote_previlege(host='127.0.0.1',port=11111):
    ag_p = hk_p = us_p = False

    quote_ctx = OpenQuoteContext(host=host, port=port)
    ret_ag, data = quote_ctx.get_market_snapshot(['SH.600519'])
    ret_hk, data = quote_ctx.get_market_snapshot(['HK.00700'])
    ret_us, data = quote_ctx.get_market_snapshot(['US.AAPL'])
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

    if ret_ag == 0:  ag_p = True
    if ret_hk == 0:  hk_p = True
    if ret_us == 0:  us_p = True

    return({
        'ag_quote_previlege':ag_p,
        'hk_quote_previlege':hk_p,
        'us_quote_previlege':us_p,
    })


def get_market_state(host='127.0.0.1',port=11111):
    quote_ctx = OpenQuoteContext(host=host, port=port)
    ret, data = quote_ctx.get_global_state()
    if not ret == RET_OK:
        logging.error('error:', data)
    quote_ctx.close()

    return({
        'ag_state':data['market_sh'],
        'hk_state':data['market_hk'],
        'us_state':data['market_us'],
    })

def get_avilable_market(host,port,debug,market_str="US_HK_AG"):
    # logging.info("market before proceeding: "+market_str)
    market = market_str.split("-")
    # market.remove('HOLD')
    mkt_state = get_market_state(host=host, port=port)
    if (not debug) and ('AG' in market) and ( mkt_state['ag_state'] in ['CLOSED','REST',]):
        market.remove('AG')
        logging.info("remove AG, not in trading hours, "+str(mkt_state['ag_state']))
    if (not debug) and ('HK' in market) and (mkt_state['hk_state'] in ['CLOSED','REST',]):
        market.remove('HK')
        logging.info("remove HK, not in trading hours, " + str(mkt_state['hk_state']))
    if (not debug) and ('US' in market) and (mkt_state['us_state'] in ['CLOSED','REST','AFTER_HOURS_END']):
        market.remove('US')
        logging.info("remove US, not in trading hours, " + str(mkt_state['us_state']))

    quote_previlege = get_quote_previlege()
    if ('AG' in market) and (not quote_previlege['ag_quote_previlege']):
        market.remove('AG')
        logging.info("remove AG, FutuOpenD doesn't have quote previlege" )
    if ('HK' in market) and (not quote_previlege['hk_quote_previlege']):
        market.remove('HK')
        logging.info("remove HK, FutuOpenD doesn't have quote previlege" )
    if ('US' in market) and (not quote_previlege['us_quote_previlege']):
        market.remove('US')
        logging.info("remove US, FutuOpenD doesn't have quote previlege" )

    # logging.info("market after proceeding: " + str(market))
    if market == []:
        logging.warning("\nwarning: empty market (all markets are closed, or FutuOpenD has no quote previlege on open markets). Adding --debug may overwrite.\n")
        # exit()

    return(market)

def get_atr(code, df_tv_all):
    #code: US.FUTU -->  market: US, code:FUTU (TradingView format)
    [market,code]=code.split(".")
    if market =='HK':
        code = str(int(code)) # '00700' --> '700'
    elif market in ['SH','SZ']:
        code = market+code

    atr_14 = df_tv_all[df_tv_all.code == code]['atr_14'].values[0]
    return(atr_14)

def fetch_history_bar(host,port,market,debug,ktype=KLType.K_1M):
    if market == 'FUTU_CN_ETF':
        _ = finlib.Finlib().remove_market_from_tscode(finlib.Finlib().get_stock_configuration(selected=True, stock_global='FUTU_CN_ETF')['stock_list'])
        code_name_df = finlib.Finlib().add_market_to_code(df=_, dot_f=True, tspro_format=False)

    for code in get_chk_code_list(market=market, debug=debug):

        if market == 'FUTU_CN_ETF':
            dir = "/home/ryan/DATA/DAY_Global/FUTU_AG_OPTION"
        else:
            dir = "/home/ryan/DATA/DAY_Global/FUTU_" + code[0:2]

        csv_f = dir + "/" + code + "_"+ktype.lower().split("_")[1]+".csv"



        if not os.path.isdir(dir):
            os.mkdir(dir)

        if os.path.exists(csv_f):
            df_exist = pd.read_csv(csv_f, converters={'volume': float,'date':str, 'code': str, 'time_key': str})
            csv_min_date = datetime.datetime.strptime(df_exist.time_key.min(), "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
            csv_max_date = datetime.datetime.strptime(df_exist.time_key.max(), "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
            start = csv_max_date
            end = datetime.datetime.today().strftime("%Y-%m-%d")
        else:
            df_exist = pd.DataFrame()
            if debug:
                csv_min_date = start = (datetime.datetime.today() - datetime.timedelta(days=100)).strftime("%Y-%m-%d")
            else:
                csv_min_date = start = (datetime.datetime.today() - datetime.timedelta(days=1000)).strftime("%Y-%m-%d")
            csv_max_date = end = datetime.datetime.today().strftime("%Y-%m-%d")

        logging.info("fetching date " + start + " " + end + " " + code)
        df = get_history_bar(host=host, port=port, code=code, start=start, end=end, ktype=ktype,
                             extended_time=True)

        df['date'] = df['time_key'].apply(
            lambda _d: datetime.datetime.strptime(_d, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d"))

        if 'AG' in market: # AG or AG_HOLD
            df = df.rename(columns={"code": "code_ft"}, inplace=False)
            df['code']=finlib.Finlib().get_code_format(code)['C2D6']
            df = finlib.Finlib().adjust_column(df=df, col_name_list=['code', 'date'])

        df_rtn = pd.concat([df_exist,df]).drop_duplicates(subset=['time_key'], keep='last',
                                                     ignore_index=True).reset_index().drop('index', axis=1)

        if 'code_name_df' in locals():
            name = code_name_df[code_name_df['code']==code]['name'].iloc[0]
            df_rtn['name']=name
            df_rtn = finlib.Finlib().adjust_column(df=df_rtn, col_name_list=['code','name'])


        df_rtn.to_csv(csv_f, encoding='UTF-8', index=False)
        logging.info("fetched, saved to " + csv_f
                     + ". fetched len " + str(df.__len__())
                     + " total len " + str(df_rtn.__len__())
                     + ". start " + csv_min_date + " end " + csv_max_date
                     )

def get_nasdaq100_list():
    f = '/home/ryan/DATA/pickle/INDEX_US_HK/nasdqa100.csv'
    df_nas100 = pd.read_csv(f)
    df_nas100['code'] = "US." + df_nas100['code']
    return(df_nas100)


def check_high_volume(market,debug,ndays=3):
    df_rtn = pd.DataFrame()

    if market in ['AG','AG_HOLD','HK','HK_HOLD']:
        today=datetime.datetime.strptime(finlib.Finlib().get_last_trading_day(),"%Y%m%d")
    elif market in ['US', 'US_HOLD','NASDAQ100']:
        today=datetime.datetime.strptime(finlib.Finlib().get_last_trading_day_us(),"%Y-%m-%d")

    code_list = get_chk_code_list(market=market, debug=debug)

    df_rtn = pd.DataFrame()
    dir =  "/home/ryan/DATA/result/high_volumes"
    csv =  dir+"/"+market+".csv"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    last_n_days = []
    for i in range(ndays):
        last_n_days.append((today - datetime.timedelta(i)).strftime("%Y%m%d")
                           )
    for code in code_list:
        logging.info("checking "+code)
        csv_f = "/home/ryan/DATA/DAY_Global/FUTU_" + code[0:2] + "/" + code + "_1m.csv"

        if not os.path.exists(csv_f):
            logging.warning("No such file "+csv_f)
            continue

        df = pd.read_csv(csv_f, converters={'volume': float, 'date':str, 'code': str, 'time_key': str})

        if df.__len__()< 60*4*260 and (not debug):
            logging.info(code+" insufficient 1minute bars, expect more than 1 years, actual "+str(df.__len__()))
            continue


        ######################### Ryan debug start
        df = df[df['time_key'].str.contains(pat="2022-0[67]", regex=True)]
        if market in ['US','US_HOLD','NASDAQ100']:
            a = df[~df['time_key'].str.contains(pat="20:00:00", regex=True)] #end of an pan
            a = a[~a['time_key'].str.contains(pat="19:5.*:00", regex=True)] #end of an pan
            a = a[~a['time_key'].str.contains(pat="16:0.*:00", regex=True)] #end of day
            a = a[~a['time_key'].str.contains(pat="15:5.*:00", regex=True)] #end of day
            a = a[~a['time_key'].str.contains(pat="09:3.*:00", regex=True)] #start of day
            a = a[~a['time_key'].str.contains(pat="04:0.*:00", regex=True)] #start of an pan

        a = a[a['volume']>0] #a is 1min bars of a code, all time

        gs = 10 #group size, how many days to compare in a group
        a['date'] = a['time_key'].apply(lambda _d: _d.split()[0])
        t = a['date'].unique()
        len = int(t.__len__()/gs) * gs
        t = t[-len:] #ensure the latest day alway in the group after reshape
        b = np.reshape(t, (-1, gs))  # 3 days per group


        for r in b.__iter__():
            # print(r[0])
            ds = " ".join(r)
            b1 = a[a['time_key'].str.contains(r[0])]
            b2 = a[a['time_key'].str.contains(r[1])]
            b3 = a[a['time_key'].str.contains(r[2])]
            b = b1.append(b2).append(b3) # b is 1min bars of a code in 3 days window

            b = b[['code','date', 'time_key', 'close', 'volume','change_rate']]
            b['change_rate']=round(b['change_rate'],1)

            # b['v2'] = b['volume'] - b['volume'].mean()
            b['pv_power'] = round(b['volume'] / b['volume'].mean())
            b['pv_power'] = b['pv_power'].fillna(0)

            c = b.sort_values(by='pv_power').tail(1) #the highest 1min volume in 10 days
            logging.info(f"High vol of {ds}\n"+finlib.Finlib().pprint(c))

            df_rtn = df_rtn.append(c)

            print("go")

        # # aa = df[df['time_key'].str.contains(pat='2022-0[67]',regex=True)]
        # aa = df[df['time_key'].str.contains(pat='2022-06-2[89]',regex=True)]
        # aa1 = df[df['time_key'].str.contains(pat='2022-06-3[01]',regex=True)]
        # aa = aa.append(aa1)
        #
        # vol_avg_window = 15
        # a['vol_ratio_n'] = round(a['volume'] / a['volume'].rolling(window=vol_avg_window).mean().shift(1), 2)
        # a = a[vol_avg_window:]
        # a['vol_ratio_n'] = a['vol_ratio_n'].fillna(0)
        # c = a.sort_values(by='vol_ratio_n').tail(10)



        ######################### Ryan debug end
        logging.info(f"end of {code}")

    df_today_hit = df_rtn[df_rtn['date'].isin(last_n_days)]

    if df_today_hit.__len__() > 0 and 'AG' in market:
        df_today_hit = finlib.Finlib().add_stock_name_to_df(df=df_today_hit)

    if df_today_hit.__len__() > 0 and 'HK' in market:
        df_today_hit['code_ft'] = df_today_hit['code']
        df_today_hit['code'] = df_today_hit['code'].apply(lambda _d: _d.split('.')[1])
        df_today_hit = finlib.Finlib().add_stock_name_to_df_us_hk(df=df_today_hit, market='HK')

    if df_today_hit.__len__() > 0 and 'US' in market:
        df_today_hit['code_ft'] = df_today_hit['code']
        df_today_hit['code'] = df_today_hit['code'].apply(lambda _d: _d.split('.')[1])
        df_today_hit = finlib.Finlib().add_stock_name_to_df_us_hk(df=df_today_hit, market='US')

    if df_today_hit.__len__() > 0:
        df_today_hit = finlib.Finlib().df_format_column(df=df_today_hit, precision='%.1e')

        logging.info(" hit an abnormal high value in past " + str(ndays) + " days.")

    ##############
    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv, encoding='UTF-8', index=False)
    print(finlib.Finlib().pprint(df=df_rtn))
    logging.info("high volume stocks list save to "+csv+" len "+str(df_rtn.__len__()))
    return(df_rtn)

def check_high_volume_ori(market,debug,ndays=3):
    if market in ['AG','AG_HOLD','HK','HK_HOLD']:
        today=datetime.datetime.strptime(finlib.Finlib().get_last_trading_day(),"%Y%m%d")
    elif market in ['US', 'US_HOLD']:
        today=datetime.datetime.strptime( finlib.Finlib().get_last_trading_day_us(),"%Y-%m-%d")

    df_rtn = pd.DataFrame()
    dir =  "/home/ryan/DATA/result/high_volumes"
    csv =  dir+"/"+market+".csv"

    if not os.path.isdir(dir):
        os.mkdir(dir)

    last_n_days = []
    for i in range(ndays):
        last_n_days.append((today - datetime.timedelta(i)).strftime("%Y%m%d")
                           )
    for code in get_chk_code_list(market=market, debug=debug):
        logging.info("checking "+code)
        csv_f = "/home/ryan/DATA/DAY_Global/FUTU_" + code[0:2] + "/" + code + "_1m.csv"

        if not os.path.exists(csv_f):
            logging.warning("No such file "+csv_f)
            continue

        df = pd.read_csv(csv_f, converters={'volume': float, 'date':str, 'code': str, 'time_key': str})

        if df.__len__()< 60*4*260 and (not debug):
            logging.info(code+" insufficient 1minute bars, expect more than 1 years, actual "+str(df.__len__()))
            continue

        df['price_vol']=df['close']*df['volume']
        stock = stockstats.StockDataFrame.retype(df)

        # HK: 5.5 Hours/Day.  [9.30am, 10, 11, 12:00] 2.5Hour ---  [13:00, 14, 15,  15:59] 3 Hour
        sma_window = 60*4*3 # 10 days 1 minutes records
        col_name = 'price_vol_'+str(sma_window)+"_sma"
        stock[col_name]
        df[col_name] = df[col_name].apply(lambda _d: int(_d))

        #stockstat change df use 'date' as index, now restore it.
        df = df.reset_index()  # after retype, 'date' column was changed to index. reset 'date' to a column
        if 'index' in df.columns:
            df = df.drop('index', axis=1)

        df = df.shift(sma_window) # remove header records

        df['pv_power'] = round(df['price_vol'] / df[col_name],1)

        # df_v100 = df.sort_values(by='pv_power', ascending=False).head(30)

        # three years, 30 records.  10 records out of 260 in a year.
        df_v100 = df.sort_values(by='pv_power', ascending=False).head(30)
        logging.info("\n" + finlib.Finlib().pprint(df=df_v100.head(3)[['date','code','time_key','close','volume','price_vol',col_name,'pv_power']]))

        df_today_hit = df_v100[df_v100['date'].isin(last_n_days)]

        if df_today_hit.__len__() > 0 and 'AG' in market:
            df_today_hit = finlib.Finlib().add_stock_name_to_df(df=df_today_hit)

        if df_today_hit.__len__() > 0 and 'HK' in market:
            df_today_hit['code_ft'] = df_today_hit['code']
            df_today_hit['code'] = df_today_hit['code'].apply(lambda _d: _d.split('.')[1])
            df_today_hit = finlib.Finlib().add_stock_name_to_df_us_hk(df=df_today_hit, market='HK')

        if df_today_hit.__len__() > 0 and 'US' in market:
            df_today_hit['code_ft'] = df_today_hit['code']
            df_today_hit['code'] = df_today_hit['code'].apply(lambda _d: _d.split('.')[1])
            df_today_hit = finlib.Finlib().add_stock_name_to_df_us_hk(df=df_today_hit, market='US')

        if df_today_hit.__len__() > 0:
            df_today_hit = df_today_hit[['code','name','time_key','volume','close']]

            df_today_hit['MaxD1']=df_v100.iloc[0].time_key
            df_today_hit['MaxV1']=df_v100.iloc[0].volume
            df_today_hit['MaxD2']=df_v100.iloc[1].time_key
            df_today_hit['MaxV2']=df_v100.iloc[1].volume
            df_today_hit['MaxD3']=df_v100.iloc[2].time_key
            df_today_hit['MaxV3']=df_v100.iloc[2].volume

            df_today_hit = finlib.Finlib().df_format_column(df=df_today_hit, precision='%.1e')

            logging.info(code+" hit an abnormal high value in past "+str(ndays) +" days." )
            df_rtn = df_rtn.append(df_today_hit)

    df_rtn = df_rtn.reset_index().drop('index', axis=1)
    df_rtn.to_csv(csv, encoding='UTF-8', index=False)
    print(finlib.Finlib().pprint(df=df_rtn))
    logging.info("high volume stocks list save to "+csv+" len "+str(df_rtn.__len__()))
    return(df_rtn)


def close_all_positions(host, port,trd_ctx, market,trd_env, debug):
    _po = get_persition_and_order(trd_ctx,market,trd_env)['position_list']
    code_list = _po['code'].to_list()

    _price = get_current_price(host=host, port=port, code_list=code_list)

    for code in code_list:
        qty = _po[_po['code'] == code]['can_sell_qty'].values[0]
        price = _price[_price['code'] == code]['low_price'].values[0]
        logging.info("close the postion "+code)
        place_sell_limit_order(trd_ctx=trd_ctx, code=code,price=price,qty=qty, trd_env=trd_env)
        #此协议请求太频繁，触发了频率限制，请稍后再试
        #每 30 秒内最多请求 15 次下单接口，且连续两次请求的间隔不可小于 0.02 秒
        time.sleep(30/15)
    return()



def init_dict_code(dict_code,code,ktype_short, ktype_long,ma_period_short,ma_period_long, df_tv_all):
    atr_14 = get_atr(code,df_tv_all)

    dict_code[code] = {'short':{
        ktype_short:{'history_bars_and_ma':{'bars_and_ma':{'ma_nsub1_sum':0}},
                     'atr_14': atr_14,
                     't_last_k_renew': datetime.datetime.now(),
                     't_last_k_time_key': datetime.datetime.now(),
                     },},
        'long':{

        ktype_long: {'history_bars_and_ma': {'bars_and_ma': {'ma_nsub1_sum': 0}},
                      'atr_14': atr_14,
                     't_last_k_renew': datetime.datetime.now(),
                     't_last_k_time_key': datetime.datetime.now(),
                      },},

    }
    #
    # dict_code[code] = {
    #     'ma_nsub1_sum': 0,
    #     'p_less_ma_cnt_in_a_row': 0,
    #     'p_great_ma_cnt_in_a_row': 0,
    #     'p_last': 0,
    #     'p_ask': 0,
    #     'p_bid': 0,
    #     'ma': 0,
    #     'update_time': 0,
    #     't_last_k_renew': datetime.datetime.now(),
    #     't_last_k_time_key': datetime.datetime.now(),
    #     'atr_14':atr_14
    # }
    return(dict_code)


def clear_price_reminder(quote_ctx,market,host='127.0.0.1',port=11111):
    ret, data = quote_ctx.get_price_reminder(code=None, market=market)
    if ret == RET_OK:
        for c in data['code']:
            logging.info("removing reminder on "+str(c))
            ret_c, data_c = quote_ctx.set_price_reminder(code=c, op=SetPriceReminderOp.DEL_ALL)
            time.sleep(0.6)


def get_price_reminder(host='127.0.0.1',port=11111):
    quote_ctx = OpenQuoteContext(host=host, port=port)

    ret, data = quote_ctx.get_price_reminder(code='HK.00700')
    if ret == RET_OK:
        print(data)
        print(data['key'].values.tolist())  # 转为 list
    else:
        print('error:', data)
    print('******************************************')
    ret, data = quote_ctx.get_price_reminder(code=None, market=Market.HK)
    if ret == RET_OK:
        print(data)
        if data.shape[0] > 0:  # 如果到价提醒列表不为空
            print(data['code'][0])  # 取第一条的股票代码
            print(data['code'].values.tolist())  # 转为 list
    else:
        print('error:', data)
    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽

def _set_common_ag_price_reminder(df,quote_ctx, type='stock'):
    #################################################################
    # reminder that not related to cost_price(cheng ben). so no duplicate rule for one same stock in diff accounts.
    #################################################################
    for code in df['code'].unique():

        if type=='stock' and (not re.match("^SH", code)) and (not re.match("^SZ", code)):
            logging.info("unrecognized stock, skip. " + str(code))
            continue

        sell = False
        sell_reason_cn = ''

        p_support = 0
        p_pressure = 0

        c_df = df[df['code']==code]

        if 'support' in df.columns:
            p_support = c_df['support'].mean()
            d_support = c_df['s_date'].iloc[0]

        if 'pressure' in df.columns:
            p_pressure = c_df['pressure'].mean()
            d_pressure = c_df['p_date'].iloc[0]


        if type=='stock':
            f_p = "/home/ryan/DATA/DAY_Global/AG_qfq" + "/" + code.replace(".", "") + ".csv"
            df_p = finlib.Finlib().regular_read_csv_to_stdard_df(f_p)
            df_p = finlib.Finlib().add_stock_name_to_df(df_p)


        if type=='etf':
            df_p = finlib.Finlib().get_etf_price(etf_code=code)
            code="SH."+code


        df_p = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_p, short=4, middle=27, long=60)

        name = df_p['name'].iloc[0]
        logging.info(f"setting reminder common {str(code)} {name} ")

        #pressure/Support
        if p_pressure != 0:
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=p_pressure,
                               reason_cn="突破压力;"+str(round(p_pressure,2))+f" at {d_pressure}",
                               reminder_type=PriceReminderType.PRICE_UP)
        if p_support != 0:
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=p_support,
                               reason_cn="跌破支撑;"+str(round(p_support,2))+f" at {d_support}",
                               reminder_type=PriceReminderType.PRICE_DOWN)


        # Cond #2
        p = round(0.5 * df_p[-60:]['close'].max(), 2)
        set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="60日最高一半;"+str(round(df_p[-60:]['close'].max(),2)),
                           reminder_type=PriceReminderType.PRICE_DOWN)

        p = round(df_p.iloc[-1]['close'] * 1.07, 2)
        set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="涨7;"+str(round(df_p.iloc[-1]['close'],2)),
                           reminder_type=PriceReminderType.PRICE_UP)

        p = round(df_p.iloc[-1]['close'] * 0.93, 2)
        set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="跌7;"+str(round(df_p.iloc[-1]['close'],2)),
                           reminder_type=PriceReminderType.PRICE_DOWN)

        # Cond #2
        if 100 * (df_p.iloc[-1]['close'] - df_p.iloc[-2]['close']) / df_p.iloc[-2]['close'] < -8:
            sell = True
            sell_reason_cn += "日内跌幅;"  # "daily drop gt 8%;"

        # Cond #2
        if 100 * (df_p[-5:]['close'].max() - df_p.iloc[-1]['close']) / df_p[-5:]['close'].max() > 8:
            sell = True
            sell_reason_cn += "5日跌幅;"  # "drop gt 8 in 5 trading days;"

        # Cond #2
        if df_p.iloc[-1]['close_4_sma'] < df_p.iloc[-1]['close_27_sma']:
            if df_p.iloc[-2]['close_4_sma'] < df_p.iloc[-2]['close_27_sma']:
                if df_p.iloc[-3]['close_4_sma'] < df_p.iloc[-3]['close_27_sma']:
                    sell = True
                    sell_reason_cn += '连3日均4低27;'  # "sma4 lt sma27 3 days in a roll;"

        # Cond #2
        if df_p.iloc[-1]['close'] < df_p.iloc[-1]['close_4_sma']:
            if df_p.iloc[-2]['close'] < df_p.iloc[-2]['close_4_sma']:
                if df_p.iloc[-3]['close'] < df_p.iloc[-3]['close_4_sma']:
                    sell = True
                    sell_reason_cn += '连3日低均4;'  # "under SMA4 3 days in a roll;"

        # sell logic end
        if sell:
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=df_p.iloc[-1]['close'], reason_cn=sell_reason_cn,
                               reminder_type=PriceReminderType.PRICE_DOWN)


def _set_act_related_ag_price_reminder(df,quote_ctx,type='stock'):
    #################################################################
    # Reminder related to account (cost_price , position_profit_ratio etc)
    #################################################################
    for index, row in df.iterrows():
        code, name = row['code'], row['name']

        if type == 'stock' and (not re.match("^SH", code)) and (not re.match("^SZ", code)):
            logging.info("unrecognized stock, skip. " + str(code) + " " + name)
            continue

        if type=='etf':
            df_p = finlib.Finlib().get_etf_price(etf_code=code)
            code = "SH." + code

        if type=='stock':
            f_p = "/home/ryan/DATA/DAY_Global/AG_qfq" + "/" + code.replace(".", "") + ".csv"
            df_p = finlib.Finlib().regular_read_csv_to_stdard_df(f_p)
            df_p = finlib_indicator.Finlib_indicator().add_ma_ema(df=df_p, short=4, middle=27, long=60)



        hold_state = "[本" + str(round(row['cost_price'], 1)) + "仓" + str(round(int(row['number_can_sale']),1)) \
                     + "盈" + str(round(row['position_profit_ratio'],1))\
                     + "户" + str(row['account'])[:4]\
                     + "]"

        logging.info("setting reminder base on profit " + str(code) + " " + name)
        p = round(row['cost_price'] * 0.98, 2)
        set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="2帕损;" + hold_state,
                           reminder_type=PriceReminderType.PRICE_DOWN)

        if row['position_profit_ratio'] < 5:
            p = round(row['cost_price'] * 1.05, 2)
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="5帕盈;" + hold_state,
                               reminder_type=PriceReminderType.PRICE_UP)

        if row['position_profit_ratio'] < 10:
            p = round(row['cost_price'] * 1.1, 2)
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="10帕盈;" + hold_state,
                               reminder_type=PriceReminderType.PRICE_UP)

        if row['position_profit_ratio'] < 15:
            p = round(row['cost_price'] * 1.15, 2)
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=p, reason_cn="15帕盈;" + hold_state,
                               reminder_type=PriceReminderType.PRICE_UP)

        if row['position_profit_ratio'] > 0:
            p = row['current_price'] * (1 + row['position_profit_ratio'] / 100 - 0.05)
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=round(p, 2),
                               reason_cn="5帕盈利回撤;" + hold_state, reminder_type=PriceReminderType.PRICE_DOWN)

        logging.info("checking sell condition, " + str(code) + " " + name)


        sell = False
        sell_reason_cn = ''

        # sell logic start
        # Cond #1
        if row['position_profit_ratio'] < -8:
            sell = True
            sell_reason_cn += "损8;"  # 'position lost gt 8%;'

        # sell logic end
        if sell:
            set_price_reminder(quote_ctx=quote_ctx, code=code, price=df_p.iloc[-1]['close'], reason_cn=sell_reason_cn,
                               reminder_type=PriceReminderType.PRICE_DOWN)


def set_ag_price_reminder(quote_ctx, clear_all, host="127.0.0.1",port=11111, debug=False):
    if clear_all:
    # if False: #ryan debug
        clear_price_reminder(quote_ctx,market=Market.SH)
        clear_price_reminder(quote_ctx,market=Market.SZ)


    # How to get .csv
    # export from ZhaoShangZhengquan --> ZiJinGuFeng.
    # Copy xls files from Windows to HAHA_BRAIN
    # libreoffice open xls encoding GB18030, save to CSV (933.csv, 059.csv, 653.csv)


    df1 = pd.read_csv('/home/ryan/933.csv', converters={'证券代码': str,}, encoding="GB18030")
    df2 = pd.read_csv('/home/ryan/059.csv', converters={'证券代码': str,}, encoding="GB18030")
    df3 = pd.read_csv('/home/ryan/653.csv', converters={'证券代码': str,}, encoding="GB18030")

    df = df1.append(df2)
    df = df.append(df3)

    df = df[['证券代码', '证券名称', '证券数量', '可卖数量', '当前价', '成本价',
             '今日盈亏', '今日盈亏比例(%)', '持仓盈亏', '持仓盈亏比例(%)',
             '最新市值', '成本金额','股东代码']]

    df = df.rename(columns={
        "证券代码": "code",
        "证券名称": "name",
        "证券数量": "number_securities",
        "可卖数量": "number_can_sale",
        "当前价": "current_price",
        "成本价": "cost_price",
        "今日盈亏": "today_profit",
        "今日盈亏比例(%)": "today_profit_ratio",
        "持仓盈亏比例(%)": "position_profit_ratio",
        "持仓盈亏": "position_profit",
        "最新市值": "latest_market_value",
        "成本金额": "cost_amount",
        "股东代码": "account",
    })

    # if debug:
    #     df = df[df['code'].str.startswith('600519')]

    #FUTU code in format SH.600519, SZ.000001, HK.0700
    df_cb = df[df['code'].str.startswith('1')]
    df_etf = df[df['code'].str.startswith('5')]
    df_stock = df[~(df['code'].str.startswith('5') | df['code'].str.startswith('1'))]
    df_stock = finlib.Finlib().add_market_to_code(df=df_stock,dot_f=True)
    df_stock_ps = pd.read_csv("/home/ryan/DATA/result/pressure_support_now.csv")
    df_stock = pd.merge(left=df_stock, right=df_stock_ps, on='name', how="inner",suffixes=["",'_x'])

    # bond : 债券，可转债．　　etf: 基金　
    # _set_common_ag_price_reminder(df_cb, quote_ctx, type='bond')
    # _set_act_related_ag_price_reminder(df_cb, quote_ctx, type='bond')

    _set_common_ag_price_reminder(df_etf, quote_ctx, type='etf')
    _set_act_related_ag_price_reminder(df_etf, quote_ctx, type='etf')

    _set_common_ag_price_reminder(df_stock, quote_ctx, type='stock')
    _set_act_related_ag_price_reminder(df_stock, quote_ctx, type='stock')

    logging.info("reminder set done")


def set_price_reminder(quote_ctx, code,price, reason_cn, reminder_type=PriceReminderType.PRICE_DOWN, host='127.0.0.1',port=11111):
    reason_cn = reason_cn[:34]
    # ret, data = quote_ctx.set_price_reminder(code=code,op=SetPriceReminderOp.DEL_ALL)
    ret, data = quote_ctx.set_price_reminder(code=code,op=SetPriceReminderOp.ADD,
                                             key=None, reminder_type=reminder_type,
                                             reminder_freq=PriceReminderFreq.ONCE_A_DAY, value=price, note=reason_cn)

    time.sleep(0.6) #('此协议请求太频繁，触发了频率限制，请稍后再试',)
    if ret == RET_OK:
        logging.info("Added reminder " + code + " , " + reason_cn + " ," + str(price))
    else:
        logging.info('error:', data)



def main():
    logging.basicConfig(filename='/home/ryan/del.log', filemode='a', format='%(asctime)s %(message)s',  datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

    logging.info(__file__+" "+"\n")
    logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

    parser = OptionParser()
    # parser.add_option("--debug", action="store_true", default=False, dest="debug", help="debug, only check 1st 10 stocks in the list")
    parser.add_option("--real_account", action="store_true", default=False, dest="real", help="real environment")
    parser.add_option("--tv_source", action="store_true", default=False, dest="tv_source", help="open tradingview")
    parser.add_option("--fetch_history_bar", action="store_true", default=False, dest="fetch_history_bar", help="fetch history bar, --market = [AG|HK|US|AG_HOLD|HK_HOLD|US_HOLD]")
    parser.add_option("--fetch_history_bar_ag_option", action="store_true", default=False, dest="fetch_history_bar_ag_option", help="fetch history bar of AG Option, --market == FUTU_CN_ETF]")
    parser.add_option("--check_high_volume", action="store_true", default=False, dest="check_high_volume", help="check high volume based on 1minute bar, --market = [AG|HK|US|AG_HOLD|HK_HOLD|US_HOLD]")
    parser.add_option("--get_rt_ticker", action="store_true", default=False, dest="get_rt_ticker", help="get real time ticker 获取实时逐笔")
    parser.add_option("-m", "--market", default="HK", dest="market",type="str", help="market name. [US-HK-AG|US_HOLD-HK_HOLD-AG_HOLD]")
    parser.add_option("--host", default="127.0.0.1", dest="host",type="str", help="futuOpenD host")
    parser.add_option("--port", default="11111", dest="port",type=int, help="futuOpenD port")
    parser.add_option("--ma_period_short", default="21", dest="ma_period_short",type=int, help="MA Period short")
    parser.add_option("--ma_period_long", default="55", dest="ma_period_long",type=int, help="MA Period long")
    parser.add_option("--ktype_short", default="K_3M", dest="ktype_short",type="str", help="Kline type short. [K_1M (1,3,5,15,30,60), K_DAY, K_WEEK, K_MON,K_QUARTER,K_YEAR ")
    parser.add_option("--ktype_long", default="K_60M", dest="ktype_long",type="str", help="Kline type long. [K_1M (1,3,5,15,30,60), K_DAY, K_WEEK, K_MON,K_QUARTER,K_YEAR ")
    parser.add_option("-d", "--debug", action="store_true", dest="debug", default=False, help="debug ")
    parser.add_option("--tri_bar_ma", action="store_true", default=False, dest="tri_bar_ma", help="trigger buy/sell based on Bar close Across MA_short")
    parser.add_option("--tri_ma_ma", action="store_true", default=True, dest="tri_ma_ma", help="trigger buy/sell based on MA_short Across MA_long")
    parser.add_option("--tri_abnormal_price", action="store_true", default=True, dest="tri_abnormal_price", help="trigger buy/sell when price change in two check > ATR14 D")
    parser.add_option("--buy_only", action="store_true", default=False, dest="buy_only", help="only buy")
    parser.add_option("--sell_only", action="store_true", default=False, dest="sell_only", help="only sell")
    parser.add_option("--close_all_positions", action="store_true", default=False, dest="close_all_positions", help="close_all_positions")
    parser.add_option("--set_ag_reminder", action="store_true", default=False, dest="set_ag_reminder", help="set_ag_reminder")


    (options, args) = parser.parse_args()

    host = options.host
    port = options.port
    pwd_unlock = '731024'

    ############# ! IMPORTANT ! ######################
    simulator = True

    if options.real:
        simulator = False


    ############# ! IMPORTANT ! ######################

    # market = Market.HK
    # market = Market.US
    # market = Market.SH
    # market = Market.SZ
    # market = options.market
    ktype_short =options.ktype_short
    ktype_long =options.ktype_long
    ma_period_short = options.ma_period_short
    ma_period_long = options.ma_period_long
    tri_bar_ma = options.tri_bar_ma
    tri_ma_ma = options.tri_ma_ma
    tri_abnormal_price = options.tri_abnormal_price
    tv_source = options.tv_source

    if not is_port_open(host=host,port=port):
        exit()

    if ma_period_short > ma_period_long:
        logging.fatal("ma_period_short > ma_period_long, quit. ma_period_short "+str(ma_period_short) + " ma_period_long "+str(ma_period_long))
        exit(1)

    ##########################start


    trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host='127.0.0.1', port=11111,
                                  security_firm=SecurityFirm.FUTUSECURITIES)
    ret, data = trd_ctx.history_order_list_query()
    if ret != RET_OK:
        print('history_order_list_query error: ', data)
        exit(0)

    for key, value in data.iterrows():
        # print(value['order_status'])

        if value['order_status'] in ('FAILED','CANCELLED_ALL','WAITING_SUBMIT'):
            continue

        if value['trd_side'] != 'BUY':
            continue

        date_str = value['create_time'].split()[0]
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        today = datetime.datetime.today()
        days_diff = (today - date_obj).days

        code = value['code'].replace("US.", "")


        stock = yf.Ticker(code)
        data = stock.history(period='1d')  # 获取当天数据
        if not data.empty:
            closing_prices= round(data['Close'].iloc[-1],2)  # 取收盘价
        else:
            closing_prices = None  # 可能当天无交易数据

        # print(code,closing_prices)

        price_delta = (closing_prices - value['price'])/value['price']*100 - 0.05*days_diff
        price_delta = round(price_delta,2)


        print(value['code'], date_str, value['price'], value['trd_side'],value['stock_name'],days_diff,closing_prices,price_delta)

    # print(data)
    trd_ctx.close()
    exit()

    #### set reminder
    if options.set_ag_reminder and is_port_open(host=host,port=port):
        quote_ctx = OpenQuoteContext(host=host, port=port)
        set_ag_price_reminder(quote_ctx=quote_ctx, clear_all=True, debug=options.debug)
        quote_ctx.close()
        exit()

    #### fetch history bar of AG option
    if options.fetch_history_bar_ag_option and is_port_open(host=host,port=port):
        fetch_history_bar(host=host,port=port,market='FUTU_CN_ETF', debug=options.debug, ktype=options.ktype_short)
        fetch_history_bar(host=host,port=port,market='FUTU_CN_ETF', debug=options.debug, ktype=options.ktype_long)
        exit()

    #### fetch history bar
    if options.fetch_history_bar and is_port_open(host=host,port=port):
        fetch_history_bar(host=host,port=port,market=options.market, debug=options.debug)
        exit()

    if options.check_high_volume:
        check_high_volume(market=options.market, debug=options.debug,ndays=5)
        exit()

    if options.get_rt_ticker and  is_port_open(host=host,port=port):
        get_rt_ticker(host=host,port=port,market=options.market, debug=options.debug)
        exit()

    market = get_avilable_market(host=host,port=port,debug=options.debug,market_str=options.market)
    get_price_code_list = get_chk_code_list(market=market,debug=options.debug)

    if simulator:
        # trd_env = TrdEnv.SIMULATE
        check_interval_sec = 15
    else:
        logging.info("WILL RUN IN REAL ACCOUNT, type REAL_ACCOUNT to continue: ")
        confirm = input()

        if confirm != "REAL_ACCOUNT":
            exit(0)

        # trd_env = TrdEnv.REAL
        check_interval_sec = 60


    #load tv df
    df_tv_us = finlib.Finlib().load_tv_fund(market='US', period='1D')
    df_tv_hk = finlib.Finlib().load_tv_fund(market='HK', period='1D')
    df_tv_cn = finlib.Finlib().load_tv_fund(market='AG', period='1D')
    df_tv_all = df_tv_us.append(df_tv_hk).append(df_tv_cn).reset_index().drop('index', axis=1)



    #General get lot
    # df_stock_basicinfo = get_stock_basicinfo(host=host, port=port, stock_list=get_price_code_list, market=market, securityType=SecurityType.STOCK)

    trd_ctx_unlocked = _unlock_trd_ctx(trd_ctx=_get_trd_ctx(host=host, port=port,market=market), pwd_unlock=pwd_unlock)

    # Close all open positions, for Simulator Env only
    if options.close_all_positions:
        if options.real:
            logging.fatal("Not allowed to close ALL positions on REAL account.")
        else:
            close_all_positions(host=host, port=port, trd_ctx=trd_ctx_unlocked,
                                market=options.market, trd_env=TrdEnv.SIMULATE, debug=options.debug)
        exit()

    #populate code specification dictionary
    dict_code = {}
    for code in get_price_code_list:
        dict_code = init_dict_code(dict_code, code,ktype_short, ktype_long,ma_period_short,ma_period_long,df_tv_all)

    k_renew_interval_second = {
        'K_1M':1*60,
        'K_3M':3*60,
        'K_5M':5*60,
        'K_15M':15*60,
        'K_30M':30*60,
        'K_60M':60*60,
        'K_DAY':24*60*60,
        'K_WEEK':24*60*60*7,
        'K_MON':24*60*60*7*30,
        'K_QUARTER':24*60*60*7*90,
        'K_YEAR':24*60*60*7*365,
    }


    ############## TV
    if tv_source:
        browser = tv_init()


   ############# Minutely Check ###############

    while True:
        if tv_source and False: #market is a list now. no longer a string.
            # df_sma_20_across_up_50 = tv_monitor_minutely(browser, 'column_short', '1h', market, 'sma_20_across_up_50')
            # df_sma_20_across_down_50 = tv_monitor_minutely(browser, 'column_short', '1h', market,'sma_20_across_down_50')

            df_p_across_up_20 = tv_monitor_minutely(browser, 'column_short', '1h', market, 'p_across_up_sma20')
            logging.info("Head of df_p_across_up_20:\n"+finlib.Finlib().pprint(df_p_across_up_20.head(2)))


            df_p_across_down_sma20 = tv_monitor_minutely(browser, 'column_short', '1h', market,'p_across_down_sma20')
            logging.info("Head of df_p_across_down_sma20:\n" + finlib.Finlib().pprint(df_p_across_down_sma20.head(2)))

        market = get_avilable_market(host=host, port=port, debug=options.debug, market_str=options.market)
        if market == []:
            logging.info("Market is empty, sleep and continue. market "+str(market))
            time.sleep(5)
            continue


        get_price_code_list = get_chk_code_list(market=market, debug=options.debug)
        df_name = finlib.Finlib().add_name_to_futu_code_list(get_price_code_list)

        df_live_price = get_current_price(host=host, port=port, code_list=get_price_code_list)


        for code in get_price_code_list:
            name = df_name[df_name['code_ft']==code]['name'].values[0]

            if code not in dict_code.keys():
                dict_code = init_dict_code(dict_code, code,ktype_short, ktype_long,ma_period_short,ma_period_long,df_tv_all)
                logging.info("initialized code "+code+ " to dict_code")

            # update ma at the 1st minute of a new hour
            now = datetime.datetime.now()

            if code.startswith('US.'):
                last_bar_time_to_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) \
                                              - convert_dt_timezone(dict_code[code]['short'][ktype_short]['t_last_k_time_key'],
                                                                    tz_in=pytz.timezone('America/New_York'),
                                                                    tz_out=pytz.timezone('Asia/Shanghai'),
                                                                    )
            else:
                last_bar_time_to_now = datetime.datetime.now() - dict_code[code]['short'][ktype_short]['t_last_k_time_key']


             #handling ktype_short
            if ma_period_long >= ma_period_short:
                ma_period_large = ma_period_long
            else:
                ma_period_large = ma_period_short

            if (dict_code[code]['short'][ktype_short]['history_bars_and_ma']['bars_and_ma']['ma_nsub1_sum'] == 0) or (last_bar_time_to_now.seconds > k_renew_interval_second[ktype_short]):
                rtn_current_ma = get_current_ma(host=host, port=port, code=code, k_renew_interval_second=k_renew_interval_second, ktype=ktype_short, ma_period=ma_period_short, bar_fetch_period=ma_period_large)
                if rtn_current_ma['rtn_code'] == RET_ERROR:
                    continue
                # accessing : dict_code[code]['K_3M']['history_bars_and_ma']
                dict_code[code]['short'][ktype_short]['history_bars_and_ma'] ={"bars_and_ma":rtn_current_ma}
                dict_code[code]['short'][ktype_short]['t_last_k_renew'] =now
                dict_code[code]['short'][ktype_short]['t_last_k_time_key'] =datetime.datetime.strptime(rtn_current_ma['time_key'], "%Y-%m-%d %H:%M:%S")


            # handling ktype_long
            if (dict_code[code]['long'][ktype_long]['history_bars_and_ma']['bars_and_ma']['ma_nsub1_sum'] == 0) or (last_bar_time_to_now.seconds > k_renew_interval_second[ktype_long]):
                if ktype_long == ktype_short:
                    _ = get_current_ma(host=host, port=port, code=code, k_renew_interval_second=k_renew_interval_second,ktype=ktype_long, ma_period=ma_period_long,history_bar_df=rtn_current_ma['df_history_bars'])
                else:
                    _ = get_current_ma(host=host, port=port, code=code, k_renew_interval_second=k_renew_interval_second,ktype=ktype_long, ma_period=ma_period_long)


                if _['rtn_code'] == RET_ERROR:
                    continue

                dict_code[code]['long'][ktype_long]['history_bars_and_ma'] = {"bars_and_ma": _}
                dict_code[code]['long'][ktype_long]['t_last_k_renew'] = now
                dict_code[code]['long'][ktype_long]['t_last_k_time_key'] = datetime.datetime.strptime(_['time_key'], "%Y-%m-%d %H:%M:%S")

            #handling df_live_price
            dict_code[code]['df_live_price'] = df_live_price[df_live_price['code'] == code]


                # logging.info(__file__ + " code "+code+" renewed "+dict_code[code]['ktype'] +"_ma_nsub1_sum " + str(dict_code[code]['ma_nsub1_sum']))

            dict_code = hourly_ma_minutely_check(code=code,
                                     ktype_short = ktype_short,
                                     ktype_long = ktype_long,
                                     ma_period_short=ma_period_short,
                                     ma_period_long=ma_period_long,
                                     dict_code = dict_code,
                                )

            #check for each code
            try:
                buy_sell_stock_if_p_up_below_hourly_ma_minutely_check(
                    code=code,
                    name=name,
                    k_renew_interval_second=k_renew_interval_second,
                    simulator=simulator,
                    trd_ctx_unlocked=trd_ctx_unlocked,
                    ktype_short=ktype_short,
                    ktype_long=ktype_long,
                    dict_code = dict_code,
                    market= market,
                    tri_bar_ma = tri_bar_ma,
                    tri_ma_ma = tri_ma_ma,
                    tri_abnormal_price = tri_abnormal_price,
                    buy_only=options.buy_only,
                    sell_only=options.sell_only,
                )
            except Exception:
                for k in trd_ctx_unlocked.keys():
                    trd_ctx_unlocked[k].close()
                # trd_ctx_unlocked.close()
                logging.info("caught exception, terminate trd_ctx_unlocked session")

        logging.info(__file__ + " " + "sleep " + str(check_interval_sec) + " sec before next check.\n\n")
        time.sleep(check_interval_sec)

    print("program completed, exiting.")


if __name__ == '__main__':
    exception_cnt = 0
    while True:
        try:
            main()
        except Exception:
            exception_cnt += 1
            logging.info(traceback.format_exc())
            if exception_cnt >= 10:
                logging.fatal("exception more than 10 times, exit")
                # sys.exit(-1) #program hung on exit
                os._exit(0)
                # exit(0) #program hung on exit
            else:
                logging.info("caught exception, restart main(). exception_cnt "+str(exception_cnt))
                time.sleep(1)
    exit(0)




def _extreme_low_price_JianLou():

    #prepareprint(
    quote_ctx = OpenQuoteContext(host=ip, port=port)
    (rc1, df1) = quote_ctx.get_market_snapshot(code_list=code_list)
    pprint(df1)

    (rc1, df1) = quote_ctx.get_market_snapshot(['SH.600000', 'HK.00700'])
    pprint(df1)

    (rc2, df2) = quote_ctx.get_multiple_history_kline(['HK.00700'], '2017-06-20', '2017-06-25', KLType.K_DAY, AuType.QFQ)
    (rc3, df3) = quote_ctx.get_multiple_history_kline(codelist=code_list, start=None, end=None, ktype=KLType.K_DAY, autype=AuType.QFQ)
    quote_ctx.close()

    trd_ctx_hk = OpenHKTradeContext(host=ip, port=port)
    trd_ctx_us = OpenUSTradeContext(host=ip, port=port)

    f_dow = '/home/ryan/DATA/pickle/INDEX_US_HK/dow.csv'
    f_hkhs = '/home/ryan/DATA/pickle/INDEX_US_HK/hkhs.csv'
    f_nasdqa100 = '/home/ryan/DATA/pickle/INDEX_US_HK/nasdqa100.csv'

    df_dow = pd.read_csv(f_dow, converters={'code': str})
    df_hkhs = pd.read_csv(f_hkhs, converters={'code': str})
    df_nasdqa100 = pd.read_csv(f_nasdqa100, converters={'code': str})

    df_input = pd.DataFrame(columns=['code', 'name'])

    for index, row in df_dow.iterrows():
        code = 'US.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    for index, row in df_nasdqa100.iterrows():
        code = 'US.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    for index, row in df_hkhs.iterrows():
        code = 'HK.' + row['code']
        name = row['name']
        new_df = pd.DataFrame([[code, name]], columns=['code', 'name'])
        df_input = df_input.append(new_df, ignore_index=True)

    ### Buy according to df_input
    ret, df_market_snapshot = quote_ctx.get_market_snapshot(df_input['code'].tolist())
    if ret != RET_OK:
        quote_ctx.close()
        raise Exception('Failed to get_market_snapshot.'+df_market_snapshot)

    ret, data = trd_ctx_hk.unlock_trade(pwd_unlock)
    trd_ctx_hk.close()
    if ret != RET_OK:
        raise Exception('Failed to unlock trade, HK')

    ret, data = trd_ctx_us.unlock_trade(pwd_unlock)
    trd_ctx_us.close()
    if ret != RET_OK:
        raise Exception('Failed to unlock trade, US')

    #checking account
    ret, df_accinfo_hk = trd_ctx_hk.accinfo_query(trd_env=trd_env)
    trd_ctx_hk.close()
    if ret != RET_OK:
        raise Exception("Cannot get account info, HK")

    ret, df_accinfo_us = trd_ctx_us.accinfo_query(trd_env=trd_env)
    trd_ctx_us.close()
    if ret != RET_OK:
        raise Exception("Cannot get account info, US")
    '''
    power_us =df_accinfo_hk.iloc[0]['power'] #99804.33
    total_assets=data.iloc[0]['total_assets'] #100000.00
    cash=data.iloc[0]['cash'] #100000.00
    market_val=data.iloc[0]['market_val'] # 0.00
    frozen_cash=data.iloc[0]['frozen_cash'] #195.67
    avl_withdrawal_cash=data.iloc[0]['avl_withdrawal_cash'] # 0.00



    power = data.iloc[0]['power']  # 99804.33
    total_assets = data.iloc[0]['total_assets']  # 100000.00
    cash = data.iloc[0]['cash']  # 100000.00
    market_val = data.iloc[0]['market_val']  # 0.00
    frozen_cash = data.iloc[0]['frozen_cash']  # 195.67
    avl_withdrawal_cash = data.iloc[0]['avl_withdrawal_cash']  # 0.00
    '''

    #checking orders(in queue)
    ret, df_order_list_hk = trd_ctx_hk.order_list_query(trd_env=trd_env)
    if ret != RET_OK:
        trd_ctx_hk.close()
        raise Exception("Cannot get order info, HK")

    ret, df_order_list_us = trd_ctx_us.order_list_query(trd_env=trd_env)
    if ret != RET_OK:
        trd_ctx_us.close()
        raise Exception("Cannot get order info, US")

    #checking postion
    ret, df_position_list_hk = trd_ctx_hk.position_list_query(trd_env=trd_env)
    if ret != RET_OK:
        trd_ctx_hk.close()
        raise Exception("Failed to get position, HK")

    ret, df_position_list_us = trd_ctx_us.position_list_query(trd_env=trd_env)
    if ret != RET_OK:
        trd_ctx_us.close()
        raise Exception("Failed to get position, US")

    i_cnt = 1
    length = df_input.__len__()

    for index, row in df_input.iterrows():
        sys.stdout.write(str(i_cnt) + " of " + str(length) + ". ")
        i_cnt = i_cnt + 1
        code = row['code']
        df_stock_info = df_market_snapshot[df_market_snapshot['code'] == code]

        if re.match('.*HK.*', code):
            trd_ctx = trd_ctx_hk
            df_position = df_position_list_hk
            df_order_list = df_order_list_hk

        if re.match('.*US.*', code):
            trd_ctx = trd_ctx_us
            df_position = df_position_list_us
            df_order_list = df_order_list_us

        #order check
        df = df_order_list[df_order_list['code'] == code].reset_index().drop('index', axis=1)

        if df.__len__() > 0:
            print("already have the open order for " + code + " . Not making new order")
            for i in range(df.__len__()):
                stock_name = df.iloc[i]['stock_name']  # 安东油田服务
                trd_side = df.iloc[i]['trd_side']  # BUY
                order_status = df.iloc[i]['order_status']  # SUBMITTED
                price = df.iloc[i]['price']  # 1.11
                qty = df.iloc[i]['qty']  # 2000
                create_time = df.iloc[i]['create_time']  # 2018-09-16 06:53:10
                updated_time = df.iloc[i]['updated_time']  # 2018-09-16 06:53:10
                dealt_qty = df.iloc[i]['dealt_qty']  # 0
                dealt_avg_price = df.iloc[i]['dealt_avg_price']  # 0
                print("\t" + stock_name + " " + str(qty) + " " + str(trd_side) + " " + str(price))
            continue

        #postion check
        df = df_position[df_position['code'] == code].reset_index().drop('index', axis=1)

        if df.__len__() > 0:
            print("already have the open position for " + code + " . Not making new order")
            for i in range(df.__len__()):
                stock_name = df.iloc[i]['stock_name']  # 腾讯控股
                qty = df.iloc[i]['qty']  # 100
                can_sell_qty = df.iloc[i]['can_sell_qty']  # 100
                cost_price = df.iloc[i]['cost_price']  # 331.4
                nominal_price = df.iloc[i]['nominal_price']  # 330
                position_side = df.iloc[i]['position_side']  # LONG
                pl_ratio = df.iloc[i]['pl_ratio']  # -0.42245
                print("\t" + stock_name + " " + str(qty) + " " + str(position_side) + " " + str(pl_ratio))
            continue

        #place order
        buy_limit(quote_ctx=quote_ctx, trd_ctx=trd_ctx, df_stock_info=df_stock_info, code=code, drop_threshold=drop_threshold, pwd_unlock=pwd_unlock, trd_env=trd_env, time_sleep=4)

    #### clean up
    quote_ctx.close()
    trd_ctx_hk.close()
    trd_ctx_us.close()
