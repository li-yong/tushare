import v20
import sys
import time
import calendar
import json
import pandas as pd
import ast
import re
#from datetime import datetime, timedelta


sys.path.append("/home/ryan/tushare_ryan/Oanda_v2/lib/v20-python-samples/src")

import argparse
import datetime
import common.config #v20
from order.args import OrderArguments, add_replace_order_id_argument
from order.view import print_order_create_response_transactions
from account.account import Account


class v20C:
    def __init__(self, active_account='101-011-7847380-001', token='43b32f61afca7adc3a85ebd5d8fcfb8a-015be2ef2131fa8db6d68a675d2c1a64'):
        #self.hostname="stream-fxpractice.oanda.com"
        self.hostname="api-fxpractice.oanda.com"
        self.port=443
        self.ssl=True
        self.datetime_format = "RFC3339"

        self.active_account=active_account
        self.token = token


        self.api = v20.Context(
            hostname=self.hostname,
            port=self.port,
            ssl=self.ssl,
            application="sample_code",
            token=self.token,
            datetime_format=self.datetime_format,
            poll_timeout=6,
        )



        #print('set self.api to 6')

    def get_instruments(self):

        response = self.api.account.instruments(self.active_account)

        code_map = ast.literal_eval(response.raw_body)

        dict_rtn = {}
        dict_code_to_instrument = {}

        if 'errorMessage' in code_map.keys():
            print('Error when getting accound instruments: '+code_map['errorMessage'])

        if 'instruments' in code_map.keys():
            for i in range(code_map['instruments'].__len__()):
                code = code_map['instruments'][i]['name']
                code_dict = code_map['instruments'][i]
                dict_rtn[code]=code_dict

            for k in dict_rtn.keys():
                code_no_underscore = re.sub("_", '', k) #EUR_USD --> EURUSD
                dict_rtn[k]['code']=code_no_underscore

                dict_code_to_instrument[code_no_underscore]=k

                if False:
                    print("\n")
                    print("instruments "+k)
                    print("code "+ str(dict_rtn[k]['code']))
                    print("displayName "+ str(dict_rtn[k]['displayName']))
                    print("displayPrecision "+ str(dict_rtn[k]['displayPrecision']))
                    print("marginRate "+ str(dict_rtn[k]['marginRate']))
                    print("maximumOrderUnits "+ str(dict_rtn[k]['maximumOrderUnits']))
                    print("maximumPositionSize "+ str(dict_rtn[k]['maximumPositionSize']))
                    print("maximumTrailingStopDistance "+ str(dict_rtn[k]['maximumTrailingStopDistance']))
                    print("minimumTradeSize "+ str(dict_rtn[k]['minimumTradeSize']))
                    print("minimumTrailingStopDistance "+ str(dict_rtn[k]['minimumTrailingStopDistance']))
                    print("name "+ str(dict_rtn[k]['name']))
                    print("pipLocation "+ str(dict_rtn[k]['pipLocation']))
                    print("tradeUnitsPrecision "+ str(dict_rtn[k]['tradeUnitsPrecision']))
                    print("type "+ str(dict_rtn[k]['type']))
        else:
            print('failed to get instruments spec, instruments not in the code_map response keys')

        return (dict_rtn,dict_code_to_instrument)

    def dump_account(self):

        #parser = argparse.ArgumentParser()
        #common.config.add_argument(parser)
        #args = parser.parse_args()
        #account_id = args.config.active_account
        api = self.api

        #response = api.account.get(account_id)
        response = api.account.get(self.active_account)

        account = Account(
            response.get("account", "200")
        )

        #account.details.balance

        #account.dump()

        return account

    def get_open_trades(self):
        api = self.api
        response = api.trade.list_open(self.active_account)
        return response

    def get_orders(self):
        api = self.api
        response_list = api.order.list(self.active_account)
        response_listpending = api.order.list_pending(self.active_account)

        #print(response_list.raw_body)

        '''
        In[1]: response_list.raw_body
        Out[2]:
        u'{"orders":[{"id":"53592","createTime":"2018-03-15T12:10:06.695098130Z","type":"TRAILING_STOP_LOSS","tradeID":"53589","distance":"998.0","timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING","trailingStopValue":"6234.9"},{"id":"53591","createTime":"2018-03-15T12:10:06.695098130Z","type":"STOP_LOSS","tradeID":"53589","price":"5758.0","guaranteed":false,"timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING"},{"id":"53590","createTime":"2018-03-15T12:10:06.695098130Z","type":"TAKE_PROFIT","tradeID":"53589","price":"4760.0","timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING"}],"lastTransactionID":"53593"}'



        In[2]: response_listpending.raw_body
        Out[3]:
        u'{"orders":[{"id":"53592","createTime":"2018-03-15T12:10:06.695098130Z","type":"TRAILING_STOP_LOSS","tradeID":"53589","distance":"998.0","timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING","trailingStopValue":"6234.9"},{"id":"53591","createTime":"2018-03-15T12:10:06.695098130Z","type":"STOP_LOSS","tradeID":"53589","price":"5758.0","guaranteed":false,"timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING"},{"id":"53590","createTime":"2018-03-15T12:10:06.695098130Z","type":"TAKE_PROFIT","tradeID":"53589","price":"4760.0","timeInForce":"GTC","triggerCondition":"DEFAULT","state":"PENDING"}],"lastTransactionID":"53593"}'

        '''
        pass

        #return response



    def limit_order(self):
        parser = argparse.ArgumentParser()
        common.config.add_argument(parser)
        add_replace_order_id_argument(parser)

        orderArgs = OrderArguments(parser)
        orderArgs.add_instrument()
        orderArgs.add_units()
        orderArgs.add_price()
        orderArgs.add_time_in_force()
        orderArgs.add_position_fill()
        orderArgs.add_take_profit_on_fill()
        orderArgs.add_stop_loss_on_fill()
        orderArgs.add_trailing_stop_loss_on_fill()
        orderArgs.add_client_order_extensions()
        orderArgs.add_client_trade_extensions()

        #args = parser.parse_args()
        #args = parser.parse_args(args = _sys.argv[1:])



        orderArgs.set_datetime_formatter(lambda dt: api.datetime_to_str(dt))

        #
        # Extract the Limit Order parameters from the parsed arguments
        #
        args = parser.parse_args(args=['XAU_USD', '-10', '1000'])
        orderArgs.parse_arguments(args)


        api = self.api

        if args.replace_order_id is not None:
            #
            # Submit the request to cancel and replace a Limit Order
            #
            response = api.order.limit_replace(
                args.config.active_account,
                args.replace_order_id,
                **orderArgs.parsed_args
            )
        else:
            #
            # Submit the request to create a Limit Order
            #
            response = api.order.limit(
                args.config.active_account,
                **orderArgs.parsed_args  #orderArgs.parsed_args :::  {'instrument': 'XAU_USD', 'price': '1000', 'units': '-10'}
            )

        print("Response: {} ({})".format(response.status, response.reason))
        print("")

        print_order_create_response_transactions(response)


    def simple_limit(self):
        api=self.api

        #http://developer.oanda.com/rest-live-v20/order-df/
        limit_order_def = {'instrument': 'XAU_USD', 'price': '1100', 'units': '10'}
        try:
            response = api.order.limit(self.active_account, **limit_order_def)

        except Exception as e:
            print("Error: {}".format(e))
        else:
            print("Response: {} ({})".format(response.status, response.reason))
            print("")

            print_order_create_response_transactions(response)
            return response

        pass

    def simple_market(self, instrument, unit, take_profit_price, stop_loss_price, trailing_stop_loss_distance=None, local_oid = '000'):
        epoch_time = calendar.timegm(time.gmtime()) + 120  #hold 120 seconds

        market_order_def = {'instrument': instrument, 'units': unit, 'timeInForce': "FOK",
                          'takeProfitOnFill': {'price': take_profit_price},
                          'stopLossOnFill': {'price': stop_loss_price},

                          # trailing stop init is to 1390 if price is 1410 unit 10 (BUY) and distance is 20
                          # instance have to > 0, otherwise Invalid request will be rejected.
                          'trailingStopLossOnFill': {'distance': trailing_stop_loss_distance},
                          'clientExtensions': {'id': local_oid, 'clientTag': str(epoch_time),
                                               'comment': "close_epoch:" + str(epoch_time)},
                          'tradeClientExtensions': {'id': local_oid, 'clientTag': str(epoch_time),
                                                    'comment': "close_epoch:" + str(epoch_time)}
                          }
        response = self.api.order.market(self.active_account, **market_order_def)

        pass


    def simple_stop(self, instrument, price, unit, take_profit_price, stop_loss_price, trailing_stop_loss_distance=None, local_oid = '000'):
        api=self.api


        print("Making a STOP order, Instument: "+ instrument + ", Price "+str(price) + \
              " unit "+str(unit) + \
             " take_profit_price "+str(take_profit_price) + \
              " stop_loss_price " + str(stop_loss_price) + \
              " trailing_stop_loss_distance " + str(trailing_stop_loss_distance)
        )



        #take_profit_price = round(take_profit_price,4)
        #stop_loss_price = round(stop_loss_price,4)

        #gtdTime=datetime.datetime.utcnow() - datetime.timedelta(hours=5)- datetime.timedelta(minutes=3) +datetime.timedelta(seconds=120)
        #gtdTime=datetime.datetime.now() + datetime.timedelta(seconds=120)
        #gtdTime=str(gtdTime.isoformat('T'))
        #print(gtdTime)

        #Set Expire date On GMT+8:        2018-02-28T00:03:33.748039
        #Get Expire date On OANDA WEB:    2/28/2018, 12:03:33 AM


        epoch_time = calendar.timegm(time.gmtime()) + 60*2  #hold 2minutes # lost -> close
        epoch_time = calendar.timegm(time.gmtime()) + 60*120  #hold 2 hours  #lost -> win -> lost -> close
        epoch_time = calendar.timegm(time.gmtime()) + 60*15   #hold 15 minutes

        stop_order_def = {'instrument': instrument, 'price': price, 'units': unit,

                          #Ryan Debug
                           'takeProfitOnFill':{'price':take_profit_price},
                           'stopLossOnFill':{'price':stop_loss_price},

                          # trailing stop init is to 1390 if price is 1410 unit 10 (BUY) and distance is 20
                          # instance have to > 0, otherwise Invalid request will be rejected.

                          #Ryan Debug
                          #'trailingStopLossOnFill':{'distance':trailing_stop_loss_distance},
                          'timeInForce':"GTD",
                          'gtdTime':str(epoch_time),
                          'clientExtensions':{'id':local_oid, 'clientTag':str(epoch_time), 'comment': "close_epoch:"+str(epoch_time)},
                          'tradeClientExtensions':{'id':local_oid, 'clientTag':str(epoch_time), 'comment': "close_epoch:"+str(epoch_time)}
                          }
        response = api.order.stop(self.active_account, **stop_order_def)

        #print("Response: {} ({})".format(response.status, response.reason))
        #print("")
        #print_order_create_response_transactions(response)\

        if ('errorMessage' in response.body.keys()):
        #if response.body.has_key('errorMessage'):
            print("Response error message: "+ response.body['errorMessage'])
        else:
            print("No error in oanda respons, response keys "+str(response.body.keys()))

        return response

    def simple_cancel_orders(self):
        account = self.dump_account()

        for oid in account.__dict__['orders']:
            order = account.__dict__['orders'][oid]

            #account_id = args.config.active_account
            api = self.api

            response = api.order.get(self.active_account,order.__dict__['id'])
            a = response.__dict__['raw_body']
            b=json.loads(a)
            state = b.get('order').get('state')
            id = b.get('order').get('id')
            createTime = b.get('order').get('createTime')
            instrument = b.get('order').get('instrument')
            price = b.get('order').get('price')
            type = b.get('order').get('type')
            units = b.get('order').get('units')
            timeInForce = b.get('order').get('timeInForce')
            clientExtensions = b.get('order').get('clientExtensions') #check the planned close time here?

            print("Print order "+ id +", "+str(state) +" "+str(type)+" "+str(instrument)+" "+str(price)+" "+str(units)+" "+str(createTime))


            response = api.order.cancel(self.active_account,order.__dict__['id'])

            print("Cancel order "+ order.__dict__['id'] + ", reason "+response.__dict__['reason']+", status "+str(response.__dict__['status']))

            pass

        pass

    def simple_close_a_trade(self, account, tid,close_reason):
        #if not tid in account.__dict__['trades'].keys():
        if not tid in account.__dict__['trades'].keys():
        #if not account.__dict__['trades'].has_key(tid):
            print("no such tid "+str(tid))
            return

        trade = account.__dict__['trades'][tid]
        response = self.api.trade.get(self.active_account, trade.__dict__['id'])
        trade_obj = response.body.get('trade')

        a = response.__dict__['raw_body']

        b = json.loads(a)
        state = b.get('trade').get('state')
        id = b.get('trade').get('id')
        openTime = b.get('trade').get('openTime')
        instrument = b.get('trade').get('instrument')
        price = b.get('trade').get('price')
        unrealizedPL = b.get('trade').get('unrealizedPL')
        realizedPL = b.get('trade').get('realizedPL')
        marginUsed = b.get('trade').get('marginUsed')
        initialUnits = b.get('trade').get('initialUnits')

        print(
        "Closing Trade id "+str(id)+ ", reason "+close_reason+", " + str(state) + " " + str(instrument) + " " + str(price) + " initialUnits:" + str(
            initialUnits)
        + " realizedPL:" + str(realizedPL) + " unrealizedPL:" + str(unrealizedPL) + " "
        + " marginUsed:" + str(marginUsed) + " openTime:"
        + str(openTime)
        )

        response = self.api.trade.close(self.active_account, int(id))

        print("Closed Trade " + str(id) + ", reason " + response.__dict__['reason'] + ", status " + str(response.__dict__['status']))
        #    response.__dict__['status']))

    def simple_close_all_trades(self, close_reason):
        account = self.dump_account()

        for tid in account.__dict__['trades']:
            self.simple_close_a_trade(account,tid,close_reason)
            pass

        pass

    def simple_get_candles(self,instrument,granularity,count):
        api=self.api

        get_candle_def = {'instrument': instrument,
                          'price': 'M',
                          'granularity':granularity,
                          'count':count,
                          }
        response = api.instrument.candles(**get_candle_def)

        if 'errorMessage' in response.__dict__['raw_body']:
            print('get candle response error: '+response.__dict__['raw_body'])
            return

        df = pd.DataFrame(columns=['code', 'date', 'o', 'h', 'l', 'c', 'vol'])
        for candle in response.body['candles']:
            d = candle.__dict__

            d_d = {
                "code":str(response.body['instrument']).replace('_',''),
                "date": d['time'],
                "o": d['mid'].__dict__['o'],
                "h": d['mid'].__dict__['h'],
                "l": d['mid'].__dict__['l'],
                "c": d['mid'].__dict__['c'],
                "vol": d['volume'],
            }

            df = df.append(d_d, ignore_index=True)
            #print df.head(2)



        return df


 #have to be limited to 5000 records since start_date to to_date
    def get_history_candles(self,instrument,granularity,start_date, to_date):
        api=self.api

        get_candle_def = {'instrument': instrument,
                          'price': 'M',
                          'granularity':granularity,
                          'fromTime':start_date,
                          'toTime':to_date,
                          }
        response = api.instrument.candles(**get_candle_def)

        if 'errorMessage' in response.__dict__['raw_body']:
            print('FATAL: get candle response error: '+response.__dict__['raw_body'])
            #return


        df = pd.DataFrame(columns=['code', 'date', 'o', 'h', 'l', 'c', 'vol'])
        for candle in response.body['candles']:
            d = candle.__dict__

            d_d = {
                "code":str(response.body['instrument']).replace('_',''),
                "date": d['time'],
                "o": d['mid'].__dict__['o'],
                "h": d['mid'].__dict__['h'],
                "l": d['mid'].__dict__['l'],
                "c": d['mid'].__dict__['c'],
                "vol": d['volume'],
            }

            df = df.append(d_d, ignore_index=True)
            #print df.head(2)



        return df

    def get_history_forex_data(self, year, month, day, next_n_days):
        (inst_dict, code_to_instrument_dict) = self.get_instruments()
        #inst_dict.keys():  dict_keys(['EUR_HKD', 'EUR_PLN', 'HK33_HKD', 'EUR_CAD', 'EUR_NZD', 'BCO_USD', 'USD_SAR', 'GBP_CAD', 'USD_CAD])
        #code_to_instrument_dict: {'USDDKK': 'USD_DKK'}
        for ins in ['USD_CAD']:
        #for ins in inst_dict.keys():
            instrument = ins
            instrument_file = ins + ".csv"

            d_start = datetime.datetime(year=year, month=month, day=day)
            delta_1d = datetime.timedelta(days=1)
            delta_neg_1s = datetime.timedelta(seconds=-1)

            DATETIME_FMT = "%FT%T.000000000Z"

            for i in range(next_n_days):  # 365 for a year
                d_end = d_start + delta_1d + delta_neg_1s

                print("loading " + instrument + " " + d_start.strftime(DATETIME_FMT))

                try:
                    df = self.get_history_candles(instrument, 'M1', d_start.strftime(DATETIME_FMT),
                                                             d_end.strftime(DATETIME_FMT))


                except:
                    print(instrument + " catch except on day start " + d_start.strftime(DATETIME_FMT))
                    d_start += delta_1d
                    continue

                code2 = instrument
                code2 = re.sub("_", '', code2)  # change EUR_USD_abc to EURUSDabc.

                print("loaded " + instrument + d_start.strftime(DATETIME_FMT))

                fh = open(instrument_file, "a")
                for i in range(df.__len__()):
                    t = str(df.iloc[i, df.columns.get_loc('date')])  # '2018-02-01T00:00:00.000000Z'
                    t = re.sub('\.000000000Z$', '', t)  # '2018-02-01T00:00:00'
                    t = re.sub('T', ' ', t)  # '2018-02-01 00:00:00'

                    o = str(df.iloc[i, df.columns.get_loc('o')])
                    h = str(df.iloc[i, df.columns.get_loc('h')])
                    l = str(df.iloc[i, df.columns.get_loc('l')])
                    c = str(df.iloc[i, df.columns.get_loc('c')])
                    vol = str(df.iloc[i, df.columns.get_loc('vol')])

                    line = code2 + "," + t + "," + o + "," + h + "," + l + "," + c + "," + vol + "\n"


                    fh.write(line)
                fh.close()
                print("save to file "+ instrument_file)


                d_start += delta_1d

