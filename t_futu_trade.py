# coding: utf-8
#from futuquant import *
#import futuquant as ft

from futu import *
import sys
import re
import pandas as pd
# import time
import tabulate
import finlib
import datetime

import logging
logging.basicConfig(filename='/home/ryan/del.log', filemode='a', format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)


#Depends on futu demon
#Step1: /home/ryan/FutuOpenD_1.03_Ubuntu16.04/FutuOpenD &
#Step2:  python3a $0
# /usr/bin/python3a: symbolic link to /hdd2/anaconda3/bin/python3


def pprint(df):
    print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))


def place_sell_market_order(trd_ctx, code, qty, trd_env ):
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


def place_buy_limit_order(trd_ctx, code, price, qty, trd_env ):
    ret, order_table = trd_ctx.place_order(price=price, qty=qty, code=code, trd_side=TrdSide.BUY,trd_env=trd_env, order_type=OrderType.NORMAL)
    print(finlib.Finlib().pprint(order_table))
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



    print(1)



def buy_limit(quote_ctx, trd_ctx, df_stock_info, code, drop_threshold=0.19, pwd_unlock='123456', trd_env=TrdEnv.SIMULATE, time_sleep=4):
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


def get_current_price(code_list=['HK.00700']):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret, df_market_snapshot = quote_ctx.get_market_snapshot(code_list)
    if ret != RET_OK:
        raise Exception('Failed to get_market_snapshot')

    quote_ctx.close()
    return(df_market_snapshot)



def get_current_ma(code='HK.00700', ktype=KLType.K_60M, ma_period=5, ):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret, data, page_req_key = quote_ctx.request_history_kline(
        code, ktype=ktype,
        start=(datetime.datetime.today() - datetime.timedelta(days=3)).strftime("%Y-%m-%d"),
        end=datetime.datetime.today().strftime("%Y-%m-%d"),
        max_count=100)  #

    if ret != RET_OK:
        logging.error(__file__+" "+'error:', data)
        return()

    ma_value_b1 = data[-ma_period:]['close'].mean()
    ma_value_nsub1_sum = data[-ma_period+1:]['close'].sum()
    logging.info(__file__+" "+"code "+code+", ktype "+ktype+", ma_value_nsub1_sum "+str(ma_value_nsub1_sum)+", ma_period "+str(ma_period)+" , ma_value_b1 "+str(ma_value_b1)+" at "+data.iloc[-1]['time_key'])

    return({
        'code':code,
        'ktype':ktype,
        'ma_period':ma_period,
        'ma_value_b1':ma_value_b1,
        'ma_value_nsub1_sum':ma_value_nsub1_sum,
        'time_key':data.iloc[-1]['time_key'],
    })


    quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽



def test():
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
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

def _get_trd_ctx(host="127.0.0.1", port=111111, market='HK'):
    if market == 'US':
        trd_ctx = OpenUSTradeContext(host=host, port=port)
    elif market == 'HK':
        trd_ctx = OpenHKTradeContext(host=host, port=port)
    else:
        logging.fatal(__file__ + " " + "unknown market, support (US,HK). get " + str(market))
        raise Exception("unknown market, support (US,HK). get " + str(market))

    return(trd_ctx)


def _unlock_trd_ctx(trd_ctx,pwd_unlock):
    ret, data = trd_ctx.unlock_trade(pwd_unlock)
    if ret != RET_OK:
        logging.fatal(__file__ + " " + 'Failed to unlock trade')
        raise Exception('Failed to unlock trade')
    return(trd_ctx)


def get_persition_and_order(trd_ctx,market,trd_env):

    #checking orders(in queue)
    ret, df_order_list = trd_ctx.order_list_query(trd_env=trd_env)
    if ret != RET_OK:
        raise Exception("Cannot get order info ")

    #checking postion
    ret, df_position_list = trd_ctx.position_list_query(trd_env=trd_env)
    if ret != RET_OK:
        raise Exception("Failed to get position")

    return(
        {   'market':market,
            'position_list':df_position_list,
            'order_list':df_order_list,
        }
    )


def main():
    simulator = True
    market = 'US'
    code = 'HK.09977'
    code = 'US.FUTU'
    host = "127.0.0.1"
    port = 11111

    ktype =KLType.K_60M
    ma_period =5
    ma_period =21

    if simulator:
        trd_env = TrdEnv.SIMULATE
    else:
        trd_env = TrdEnv.REAL

    pwd_unlock = '731024'

    trd_ctx_unlocked = _unlock_trd_ctx(trd_ctx=_get_trd_ctx(host=host, port=port,market=market), pwd_unlock=pwd_unlock)

    rtn = get_persition_and_order(trd_ctx=trd_ctx_unlocked, market=market, trd_env=trd_env)

    df_order_list = rtn['order_list']
    df_position_list = rtn['position_list']

    orders = df_order_list[df_order_list['code']==code].reset_index().drop('index', axis=1)
    print(finlib.Finlib().pprint(orders))

    position = df_position_list[df_position_list['code']==code].reset_index().drop('index', axis=1)
    position_qty = position.qty[0]
    position_can_sell_qty = position.can_sell_qty[0]
    print(finlib.Finlib().pprint(position))

    if not code in df_position_list['code'].to_list():
        logging.info(__file__+" "+"code "+code +" not has position")

    #init h1_ma5
    h1_ma5_dict = get_current_ma(code=code, ktype=ktype, ma_period=ma_period)
    h1_ma5_nsub1_sum = h1_ma5_dict['ma_value_nsub1_sum']

    #check every minute, get realtime data
    p_less_ma5_cnt_in_a_row = 0

    while True:

        if simulator:
            ts = 6
        else:
            ts = 60

        logging.info(__file__ + " " + "sleep "+str(ts)+" sec before next check .")

        try:
            time.sleep(ts)
        except:
            print("catch exception")
            trd_ctx_unlocked.close()


        #update h1_ma5 at the 1st minute of a new hour
        now = datetime.datetime.now()
        if now.minute <= 3:
            h1_ma5_dict = get_current_ma(code=code, ktype=ktype, ma_period=ma_period)
            h1_ma5_nsub1_sum = h1_ma5_dict['ma_value_nsub1_sum']
            logging.info(__file__ + " renewed h1_ma5 at the begining of new hour. h1_ma5_nsub1_sum "+str(h1_ma5_nsub1_sum))


        prices = get_current_price(['US.FUTU','HK.09977'])
        stock = prices[prices['code'] == code]
        stock_lot_size = stock.iloc[0]['lot_size']

        sell_slot_size_1_of_4_position = int(position_can_sell_qty*0.25/stock_lot_size)*stock_lot_size
        # print(finlib.Finlib().pprint(stock))

        p_ask = stock.ask_price[0] #seller want to sell at this price.
        # p_bid = stock.bid_price[0] #buyer want to buy at this price.
        h1_ma5 = (h1_ma5_nsub1_sum+p_ask)/ma_period
        logging.info(__file__+" "+"code "+code+", h1_ma5 "+str(h1_ma5)+ " , ask price "+str(p_ask)+" at "+stock['update_time'][0])

        if simulator:
            rtn = place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=stock_lot_size, trd_env=trd_env)
            # rtn = place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=stock_lot_size, trd_env=trd_env)


        if p_ask < h1_ma5:
            p_less_ma5_cnt_in_a_row += 1

            logging.info(__file__+" "+"code "+code+" alert! p_ask " +str(p_ask)+" < h1_ma5 " +str(h1_ma5) +
                         " , p_less_ma5_cnt_in_a_row "+str(p_less_ma5_cnt_in_a_row))

            if p_less_ma5_cnt_in_a_row >= 3:
                logging.info(__file__+" "+"code "+code+" proceeding to sell, p_less_ma5_cnt_in_a_row "+str(p_less_ma5_cnt_in_a_row))
                # place_sell_market_order(trd_ctx=trd_ctx_unlocked, code=code, qty=stock_lot_size,trd_env=trd_env)
                place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position,trd_env=trd_env)
                continue
            else:
                logging.info(__file__+" "+"code "+code + " hold to sell, p_less_ma5_cnt_in_a_row has not reach 3 yet" + str(p_less_ma5_cnt_in_a_row))

            if p_ask <= h1_ma5 * 0.95:
                logging.info(__file__+" "+"code "+code+" proceeding to sell, p_ask " +str(p_ask)+" <= h1_ma5*.95 " +str(h1_ma5*.095) +
                         " , p_less_ma5_cnt_in_a_row "+str(p_less_ma5_cnt_in_a_row))
                # place_sell_market_order(trd_ctx=trd_ctx_unlocked, code=code, qty=stock_lot_size, trd_env=trd_env)
                place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position, trd_env=trd_env)
                continue
            else:
                logging.info(__file__+" "+"code "+code+" hold to sell,  p_ask " +str(p_ask)+" has not <= h1_ma5*.095 " +str(h1_ma5*.095) +
                         " , p_less_ma5_cnt_in_a_row "+str(p_less_ma5_cnt_in_a_row))


        if p_ask >= h1_ma5 and p_less_ma5_cnt_in_a_row > 0:
            p_less_ma5_cnt_in_a_row = 0
            logging.info(__file__+" "+"code "+code+ " reset p_less_ma5_cnt_in_a_row tp 0. ")

        logging.info(__file__ + " "+"code "+code + " this minute check completed. h1_ma5 "+str(h1_ma5)+ " , ask price "+str(p_ask))


    trd_ctx_unlocked.close()
    print("program completed, exiting.")


if __name__ == '__main__':
    main()
    exit(0)

    #init
    #ip = '127.0.0.1'
    ip = "haha_data_source"
    ip = "192.168.199.242"
    port = 11111
    pwd_unlock = '731024'
    code = "US.AAPL"
    code_list = ["US.AAPL", "HK.00700"]
    code_list = ["SH.600519", "HK.00700"]
    #code = "HK.00700"
    #code = "HK.03337"
    drop_threshold = 0.19  #buy at 19% drop
    order_type = OrderType.NORMAL
    trd_env = TrdEnv.SIMULATE


    main()

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
        raise Exception('Failed to get_market_snapshot')

    ret, data = trd_ctx_hk.unlock_trade(pwd_unlock)
    if ret != RET_OK:
        raise Exception('Failed to unlock trade, HK')

    ret, data = trd_ctx_us.unlock_trade(pwd_unlock)
    if ret != RET_OK:
        raise Exception('Failed to unlock trade, US')

    #checking account
    ret, df_accinfo_hk = trd_ctx_hk.accinfo_query(trd_env=trd_env)
    if ret != RET_OK:
        raise Exception("Cannot get account info, HK")

    ret, df_accinfo_us = trd_ctx_us.accinfo_query(trd_env=trd_env)
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
        raise Exception("Cannot get order info, HK")

    ret, df_order_list_us = trd_ctx_us.order_list_query(trd_env=trd_env)
    if ret != RET_OK:
        raise Exception("Cannot get order info, US")

    #checking postion
    ret, df_position_list_hk = trd_ctx_hk.position_list_query(trd_env=trd_env)
    if ret != RET_OK:
        raise Exception("Failed to get position, HK")

    ret, df_position_list_us = trd_ctx_us.position_list_query(trd_env=trd_env)
    if ret != RET_OK:
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
