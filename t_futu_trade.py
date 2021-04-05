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
import finlib_indicator
import datetime
import pytz
import logging
logging.basicConfig(filename='/home/ryan/del.log', filemode='a', format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)



from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# reduce webdriver session log for every request.
from selenium.webdriver.remote.remote_connection import LOGGER as SELENIUM_LOGGER
from selenium.webdriver.remote.remote_connection import logging as SELENIUM_logging
SELENIUM_LOGGER.setLevel(SELENIUM_logging.ERROR)


#Depends on futu demon
#Step1: /home/ryan/FutuOpenD_1.03_Ubuntu16.04/FutuOpenD &
#Step2:  python3a $0
# /usr/bin/python3a: symbolic link to /hdd2/anaconda3/bin/python3


def pprint(df):
    print(tabulate.tabulate(df, headers='keys', tablefmt='psql'))


def place_sell_market_order(trd_ctx, code, qty, trd_env ):
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


def place_buy_limit_order(trd_ctx, code, price, qty, trd_env ):
    logging.info(__file__ + " place_buy_limit_order " + "code " + code + " , price " + str(price) + " , qty " + str(
        qty) + " , trd_env " + str(trd_env))
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


def get_stock_basicinfo(host, port, stock_list=None, market=Market.HK, securityType=SecurityType.STOCK):
    quote_ctx = OpenQuoteContext(host=host, port=port)

    if stock_list == None:
        ret, data = quote_ctx.get_stock_basicinfo(market, securityType)
    else:
        ret, data = quote_ctx.get_stock_basicinfo(market, securityType, stock_list)

    quote_ctx.close()  # After
    return(data)

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
        extended_time=True,
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

def _get_trd_ctx(host="127.0.0.1", port=111111, market=Market.HK):
    if market == Market.US:
        trd_ctx = OpenUSTradeContext(host=host, port=port)
    elif market == Market.HK:
        trd_ctx = OpenHKTradeContext(host=host, port=port)
    else:
        logging.fatal(__file__ + " " + "unknown market, support (Market.HK, Market.US). get " + str(market))
        raise Exception("unknown market, support (Market.HK, Market.US). get " + str(market))

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


def buy_sell_stock_if_p_up_below_hourly_ma_minutely_check(
        code,
        simulator,
        trd_ctx_unlocked,
        dict_code,
        df_position_list,
        df_order_list,
    ):

    if simulator:
        trd_env = TrdEnv.SIMULATE
    else:
        trd_env = TrdEnv.REAL



    ###################
    # get order
    ###################
    orders = df_order_list[df_order_list['code']==code].reset_index().drop('index', axis=1)

    if orders.empty:
        logging.info("no orders there on code "+code)
    else:
        # index 0 is the most recent order
        orders = orders.sort_values(by="create_time", ascending=False).reset_index().drop('index', axis=1)


        this_order_string = finlib.Finlib().pprint(orders.head(1))

        last_order = orders.iloc[0]

        # last_order.order_id #6226957295081580411
        # last_order.code #HK.09977
        # last_order.stock_name #凤祥股份
        # last_order.trd_side  # BUY
        # last_order.qty #1000.0
        # last_order.price #2.5
        # last_order.create_time #'2021-04-02 11:44:05'
        # last_order.order_status #SUBMITTED

        lastorder_create_time = datetime.datetime.strptime(last_order.create_time, "%Y-%m-%d %H:%M:%S")

        # US Market
        if code.startswith('US.'):
            lastorder_create_time = convert_dt_timezone(lastorder_create_time,
                                                        tz_in=pytz.timezone('America/New_York'),
                                                        tz_out=pytz.timezone('Asia/Shanghai'),
                                                        )

            create_time_to_now = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai')) - lastorder_create_time

        # not a US market. HK Market
        else:
            create_time_to_now = datetime.datetime.now() - lastorder_create_time

        if create_time_to_now.seconds <= 60*60*4: # 4 hours
            if not simulator:
                logging.info(__file__ + " " + "code "+code+" placed an order in 4 hours, will not create more orders. Abort further processing")
                logging.info(__file__ + " " + this_order_string)
                return()

            elif simulator and (last_order.order_status not in ('FILLED_ALL','FILLED_PART')):
                logging.info(__file__ + " " + "code "+code+" SIMULATOR but has no UNfilled order in 4 hours, will not create more orders. Abort further processing")
                logging.info(__file__ + " " + "latest order:\n"+this_order_string)
                return()



    ###################
    # get position
    ###################
    if not code in df_position_list['code'].to_list():
        if simulator:
            logging.info(__file__ + " " + "code " + code + " SIMULATOR, no position, create new order for simulator.")
            place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=dict_code[code]['p_ask'], code=code, qty=dict_code[code]['stock_lot_size'],
                                        trd_env=trd_env)
            return()
        else:
            logging.info(__file__ + " " + "code " + code + " not has position. Abort further processing.")
            return()

    position = df_position_list[df_position_list['code'] == code].reset_index().drop('index', axis=1)

    cur_pos="current position:\n"+finlib.Finlib().pprint(position)
    # print(cur_pos)
    logging.info(cur_pos)
    position_qty = position.qty[0]
    position_can_sell_qty = position.can_sell_qty[0]

    ###################
    # sell position
    ###################
    stock_lot_size = dict_code[code]['stock_lot_size']
    sell_slot_size_1_of_4_position = int(position_can_sell_qty * 0.25 / stock_lot_size) * stock_lot_size

    #trading one unit in REAL env.
    if (not simulator) and sell_slot_size_1_of_4_position > stock_lot_size:
        sell_slot_size_1_of_4_position = stock_lot_size

    ###################
    # evaluate p_ask with MA
    ###################
    p_ask = dict_code[code]['p_ask']
    p_bid = dict_code[code]['p_bid']
    h1_ma = dict_code[code]['h1_ma']

    if p_ask == 'N/A' or p_ask == 0:
        logging.info(__file__ + " " + "code " + code + ". ask price is "+ str(p_ask)+" . abort further processing.")
        return()

    logging.info(__file__ + " " + "code " + code + ", h1_ma " + str(h1_ma) + " , ask price " + str(p_ask))

    if (p_ask < h1_ma) and (dict_code[code]['p_ask_last']  > dict_code[code]['h1_ma_last'] ):
        logging.info(__file__ + " " + "code " + code + " ALERT! p_ask " + str(p_ask) + " DOWN across h1_ma " + str(h1_ma)+ ". proceeding to SELL")

        place_sell_limit_order(trd_ctx=trd_ctx_unlocked, price=p_ask, code=code, qty=sell_slot_size_1_of_4_position,
                               trd_env=trd_env)

    if (p_bid > h1_ma) and (dict_code[code]['p_bid_last'] < dict_code[code]['h1_ma_last'] ):
        logging.info(__file__ + " " + "code " + code + " ALERT! p_ask " + str(p_ask) + " UP across h1_ma " + str(h1_ma)+ ". proceeding to BUY")

        place_buy_limit_order(trd_ctx=trd_ctx_unlocked, price=p_bid, code=code, qty=stock_lot_size,trd_env=trd_env)


    logging.info(
        __file__ + " " + "code " + code + " this minute check completed. h1_ma " + str(h1_ma) + " , ask price " + str(
            p_ask))

    return()


def hourly_ma_minutely_check(
        code,
        ma_period,
        dict_code,
        df_live_price,
    ):


    ###################
    # get live price
    ###################
    stock = df_live_price[df_live_price['code'] == code]

    dict_code[code]['p_ask_last'] = dict_code[code]['p_ask']
    dict_code[code]['p_bid_last'] = dict_code[code]['p_bid']
    dict_code[code]['h1_ma_last'] = dict_code[code]['h1_ma']
    dict_code[code]['update_time_last'] = dict_code[code]['update_time']

    dict_code[code]['p_ask'] = stock.iloc[0]['ask_price']  # seller want to sell at this price.
    dict_code[code]['p_last'] = stock.iloc[0]['last_price']  # seller want to sell at this price.
    dict_code[code]['p_bid'] = stock.iloc[0]['bid_price'] #buyer want to buy at this price.
    dict_code[code]['update_time'] = stock.iloc[0]['update_time'] #buyer want to buy at this price.

    dict_code[code]['h1_ma'] = round((dict_code[code]['h1_ma_nsub1_sum'] + stock.iloc[0]['ask_price'] ) / ma_period, 2)

    dict_code[code]['stock_lot_size'] = stock.iloc[0]['lot_size']

    if dict_code[code]['p_ask']  < dict_code[code]['h1_ma']:
        dict_code[code]['p_less_ma_cnt_in_a_row'] += 1
        dict_code[code]['p_great_ma_cnt_in_a_row'] = 0

    elif dict_code[code]['p_ask'] > dict_code[code]['h1_ma']:
        dict_code[code]['p_great_ma_cnt_in_a_row'] += 1
        dict_code[code]['p_less_ma_cnt_in_a_row'] = 0

    logging.info("\n"+__file__ + " " + "code " + code + " "
                 +" p_last "+str(dict_code[code]['p_last'])
                 +" h1_ma " + str(dict_code[code]['h1_ma'])

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
    browser = finlib_indicator.Finlib_indicator().tv_login(browser=browser)

    browser.get('https://tradingview.com/screener/')
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


def main():
    ############# ! IMPORTANT ! ######################
    simulator = True
    ############# ! IMPORTANT ! ######################

    market = Market.HK
    market = Market.US
    ktype =KLType.K_60M
    # ma_period =5
    ma_period =21
    tv_source = True

    if  market == Market.HK:
        get_price_code_list = ['HK.00700', 'HK.09977']
    elif market == Market.US:
        get_price_code_list = ['US.FUTU', 'US.AAPL']

    if simulator:
        trd_env = TrdEnv.SIMULATE
        check_interval_sec = 15
    else:
        trd_env = TrdEnv.REAL
        check_interval_sec = 60

    host = "127.0.0.1"
    port = 11111
    pwd_unlock = '731024'

    #General get lot
    df_stock_basicinfo = get_stock_basicinfo(host=host, port=port, stock_list=get_price_code_list, market=market, securityType=SecurityType.STOCK)

    trd_ctx_unlocked = _unlock_trd_ctx(trd_ctx=_get_trd_ctx(host=host, port=port,market=market), pwd_unlock=pwd_unlock)

    _po = get_persition_and_order(trd_ctx=trd_ctx_unlocked, market=market, trd_env=trd_env)

    #populate code specification dictionary
    dict_code = {}
    for code in get_price_code_list:
        dict_code[code] = {
            'h1_ma_nsub1_sum':0,
            'p_less_ma_cnt_in_a_row':0,
            'p_great_ma_cnt_in_a_row':0,
            'p_ask':0,
            'p_bid':0,
            'h1_ma':0,
            'update_time':0,
        }


    ############## TV
    if tv_source:
        browser = tv_init()


   ############# Minutely Check ###############
    while True:
        if tv_source:
            # df_sma_20_across_up_50 = tv_monitor_minutely(browser, 'column_short', '1h', market, 'sma_20_across_up_50')
            # df_sma_20_across_down_50 = tv_monitor_minutely(browser, 'column_short', '1h', market,'sma_20_across_down_50')

            df_p_across_up_20 = tv_monitor_minutely(browser, 'column_short', '1h', market, 'p_across_up_sma20')
            logging.info("Head of df_p_across_up_20"+finlib.Finlib().pprint(df_p_across_up_20.head(2)))


            df_p_across_down_sma20 = tv_monitor_minutely(browser, 'column_short', '1h', market,'p_across_down_sma20')
            logging.info("Head of df_sma_20_across_down_50" + finlib.Finlib().pprint(df_p_across_down_sma20.head(2)))


        df_live_price = get_current_price(get_price_code_list)

        for code in get_price_code_list:
            # update h1_ma5 at the 1st minute of a new hour
            now = datetime.datetime.now()

            if dict_code[code]['h1_ma_nsub1_sum'] == 0 or now.minute <= 3:
                dict_code[code]['h1_ma_nsub1_sum'] = get_current_ma(code=code, ktype=ktype, ma_period=ma_period)['ma_value_nsub1_sum']
                logging.info(__file__ + " code "+code+" renewed h1_ma_nsub1_sum " + str(dict_code[code]['h1_ma_nsub1_sum']))

            dict_code = hourly_ma_minutely_check(code=code,
                                     ma_period=ma_period,
                                     dict_code = dict_code,
                                     df_live_price = df_live_price,
                                )

            #check for each code
            try:
                buy_sell_stock_if_p_up_below_hourly_ma_minutely_check(
                    code=code,
                    simulator=simulator,
                    trd_ctx_unlocked=trd_ctx_unlocked,
                    dict_code = dict_code,
                    df_position_list= _po['position_list'],
                    df_order_list= _po['order_list'],
                )

            except KeyboardInterrupt:
                trd_ctx_unlocked.close()
                logging.info("caught exception, terminate trd_ctx_unlocked session")

        logging.info(__file__ + " " + "sleep " + str(check_interval_sec) + " sec before next check.\n\n")
        time.sleep(check_interval_sec)

    print("program completed, exiting.")


if __name__ == '__main__':
    main()
    exit(0)



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
