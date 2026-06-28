# coding: utf-8
import logging
import traceback
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import datetime
import json
import pprint
import sys
import time

from optparse import OptionParser


sys.path.append('/home/ryan/tushare_ryan/Oanda_v2/logic')
import t_ph_lib

parser = OptionParser()
parser.add_option("-r", "--revert", action="store_true",
                  dest="revert", default=False,
                  help="revert B/S action on comparation account")

(options, args) = parser.parse_args()
revert=options.revert

print("revert: "+str(revert))

if revert:
    #practice account sunraise2008,  740@qq, fav
    active_account = '101-011-8038242-001'
    token = '1aff7320463828a486046f155ecfaad6-5f8ad04cdaaf330fd17ab07d4dbcea12'
else:
    #practice account sunraise2007
    active_account = '101-011-7847380-001'
    token = '43b32f61afca7adc3a85ebd5d8fcfb8a-015be2ef2131fa8db6d68a675d2c1a64'


class t_ph_strategy(object):
    print("entering my strategy!")

    def __init__(self):
        self.trading_enabled = False
        self._action = None
        self.mysql_host = '127.0.0.1'
        #self.mysql_host = 'td'
        self.v20C = t_ph_lib.v20C(active_account,token)
        #get instrument spec
        (self.inst_dict,self.code_to_instrument_dict) = self.v20C.get_instruments()


    #if __name__ == '__main__':

    def update_from_oanda_to_db(self):
        response = self.v20C.get_open_trades()

        print(response.raw_body)
        if response.body['trades'].__len__() == 0:
            print("no open trade in oanda")
            time.sleep(5)



        for i in range(response.body['trades'].__len__()):
            trd = response.body['trades'][i]
            if 'tradeClientExtensions' in trd.dict().keys():
                tradeClientExtensions = trd.dict()['tradeClientExtensions']
                db_oid = tradeClientExtensions['id']  # '1'
                comment = tradeClientExtensions['comment']  # 'close_epoch:1521188571'

            if 'clientExtensions' in trd.dict().keys():
                clientExtensions = trd.dict()['clientExtensions'] #{'comment': u'close_epoch:1522921323', 'id': u'1728'}
                db_oid = clientExtensions['id']  #u'1728'

                comment = clientExtensions['comment']  # u'close_epoch:1522921323'


            if db_oid:
                #ch_rsn="read trade status from oanda and sync back to local;"
                ch_rsn=""  #save db field space
                self.update_order_tracking_forex_tbl(order_number=db_oid,oanda_number=trd.id, status=trd.state,order_change_reason=ch_rsn, revert = revert )




    def update_order_tracking_forex_tbl(self,order_number='', oanda_number='', order_change_reason='',status='', revert=False ):
        #revert: on the comparation forex account, B/S revert logic.

        #if revert:
        #    print("not update DB on revert operation. revert = "+str(revert))
        #    return

        engine = create_engine('mysql://root:admin888.@_@@'+self.mysql_host+'/ryan_stock_db?charset=utf8')

        # used by python mysql.connector write to db
        cnx = mysql.connector.connect(host=self.mysql_host, user='root', password='admin888.@_@', database="ryan_stock_db")
        cursor = cnx.cursor()

        sql = "SELECT * FROM `order_tracking_forex` WHERE `order_number` = \""+str(order_number)+"\""
        df_db_record = pd.read_sql_query(sql, engine, index_col='order_number')

        if df_db_record.__len__() == 0:
            print("empty tbl order_tracking_forex, order_number " +str(order_number))
            return

        a = df_db_record.to_dict()



        if revert:
            db_ord_chg_hist = a['order_change_hist_revert'][int(order_number)]
            db_status = a['status_revert'][int(order_number)]
            db_oanda_number = a['oanda_number_revert'][int(order_number)]
            db_field_oan = 'oanda_number_revert'
            db_field_sta = 'status_revert'
            db_field_lrt = 'last_review_time_revert'
            db_field_och = 'order_change_hist_revert'
        else:
            db_ord_chg_hist = a['order_change_hist'][int(order_number)]
            db_status = a['status'][int(order_number)]
            db_oanda_number = a['oanda_number'][int(order_number)]
            db_field_oan = 'oanda_number'
            db_field_sta = 'status'
            db_field_lrt = 'last_review_time'
            db_field_och = 'order_change_hist'

        if status=='':
            status=db_status

        if oanda_number=='':
            oanda_number=db_oanda_number

        if order_change_reason=='':
            order_change_hist=db_ord_chg_hist
        else:
            order_change_hist= str(db_ord_chg_hist) + str(order_change_reason)



        #if (db_oanda_number is None ) or (db_oanda_number == ''):
        if True:
            # update the order status(open,close) on db.
            add_order_tracking_forex = ("UPDATE order_tracking_forex "
                                  "SET "+db_field_oan+" = %(oanda_number)s,  "+db_field_sta+" = %(status)s, "+db_field_lrt+" = %(last_review_time)s, "+db_field_och+" = %(order_change_hist)s "
                                  "WHERE order_number=%(order_number)s"
                                  )



            data_order_tracking_forex = {
                'oanda_number': int(oanda_number),
                'status': status,
                'last_review_time': datetime.datetime.now(),
                'order_change_hist': order_change_hist,
                'order_number': int(order_number),
            }

            cursor.execute(add_order_tracking_forex, data_order_tracking_forex)

            cnx.commit()
            print("Update db record, revert: "+str(revert)+", order_number: " + str(order_number) + " oanda_number: " + str(oanda_number) + \
                  " status " + str(status) + " order_change_reason " + str(order_change_reason))

            cursor.close()
            cnx.close


    def Update(self):
        #read the oanda open trade, sync it's trade id and status to local order_tracking_forex tbl.
        self.update_from_oanda_to_db()






        #print("print orders")
        #t = self.v20C.get_orders()

        inst_dict = self.inst_dict
        code_to_instrument_dict = self.code_to_instrument_dict


        #engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
        engine = create_engine('mysql://root:admin888.@_@@'+self.mysql_host+'/ryan_stock_db?charset=utf8')

        # used by python mysql.connector write to db
        #cnx = mysql.connector.connect(host=mysql_host, user='root', password='admin888.@_@', database="ryan_stock_db")
        #cursor = cnx.cursor()

        # ONLY IN NEW, FILLED ?
        if revert:
            sql = "SELECT * FROM `order_tracking_forex` WHERE `status_revert` NOT IN ( \"CLOSED\",\"REJECTED\", \"CANCELLED\") "
        else:
            sql = "SELECT * FROM `order_tracking_forex` WHERE `status` NOT IN ( \"CLOSED\",\"REJECTED\", \"CANCELLED\") "

        df_db_record = pd.read_sql_query(sql, engine, index_col='order_number')
        #df_db_record = df_db_record.reindex()


        for order_number in df_db_record.index.tolist(): #index by order number
            print("\nchecking in market order,  order_number in local  "+str(order_number))

            # update the order last review time
            self.update_order_tracking_forex_tbl(order_number=order_number, oanda_number='', status='',
                                           order_change_reason='', revert=revert)

            row=df_db_record.loc[order_number]

            #If new order close_epoch_plan expired, then close the order.
            if revert:
                status_col = 'status_revert'
                oanda_col = 'oanda_number_revert'
            else:
                status_col = 'status'
                oanda_col = 'oanda_number'

            # mark the order to 'closed' in DB if the order_status != new and oand_number = 0
            if (str(row[oanda_col]) == '0' and row[status_col] != 'NEW'):
                close_reason = str(datetime.datetime.now()) + " close local order as it was not found on oanda open orders. (local);"
                print('close local order as it was not found on openning oanda orders, order_number ' + str(order_number) + ", reason " + close_reason)

                self.update_order_tracking_forex_tbl(order_number=order_number, oanda_number='', status='CLOSED',
                                               order_change_reason=close_reason, revert=revert)
                continue

            # status = 'NEW' AND 'CLOSE TIME MEET'
            if ((row[status_col] in ('NEW')) and (int(datetime.datetime.now().strftime('%s')) > int(row['close_epoch_plan']))):
                close_reason = str(datetime.datetime.now()) + " order expire time meet (local);"
                print('Close a new trade in DB, trade closing is not sent to Oanda, order_number ' + str(order_number) + ", reason " + close_reason)

                self.update_order_tracking_forex_tbl(order_number=order_number, oanda_number='', status='CLOSED',
                                               order_change_reason=close_reason, revert=revert)
                continue


            if (row[status_col] in ('FILLED','PLACED','OPEN')) : #check the planned exit time, close the order in oanda if time exceeded, then update db to 'CLOSED'
                #epoch_now = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
                epoch_now = int(datetime.datetime.now().strftime('%s'))
                if epoch_now > int(row['close_epoch_plan']):
                    tid = str(int(row[oanda_col]))

                    #close the order
                    close_reason=str(datetime.datetime.now())+" order expire time meet;"
                    account =self.v20C.dump_account() #the order has clientExtensions set to 'close_epoch :12345678' at t_ph_lib.py when open, check it probably could be another approach.
                    ch_rsn = 'Close a trade, tid '+tid+", reason "+close_reason
                    print(ch_rsn)
                    self.v20C.simple_close_a_trade(account,tid, close_reason)


                    self.update_order_tracking_forex_tbl(order_number=order_number, oanda_number='', status='CLOSED',
                                                   order_change_reason=ch_rsn, revert=revert)




            # debug start
            response = self.v20C.get_open_trades()
            MAX_OPEN_TRADE = 3
            if 'trades' in response.body.keys():
                resp_len = response.body['trades'].__len__()
                if resp_len > MAX_OPEN_TRADE:
                    print("not process (creating new order) , more than "+str(MAX_OPEN_TRADE)+" open transaction on oanda, open count is " + str(resp_len))
                    continue
            # debug end


            #instrument_oanda = row['code'][0:3] + '_' + row['code'][3:6] #EURUSD ==> EUR_USD
            instrument_oanda = code_to_instrument_dict[row['code']] #EURUSD ==> EUR_USD

            #general setting

            '''
   JP225_USD': {'code': 'JP225USD',
  'displayName': 'Japan 225',
  'displayPrecision': 1,
  'marginRate': '0.02',
  'maximumOrderUnits': '1000',
  'maximumPositionSize': '0',
  'maximumTrailingStopDistance': '10000.0',
  'minimumTradeSize': '1',
  'minimumTrailingStopDistance': '5.0',
  'name': 'JP225_USD',
  'pipLocation': 0,
  'tradeUnitsPrecision': 0,
  'type': 'CFD'},


 'EUR_USD': {'code': 'EURUSD',
  'displayName': 'EUR/USD',
  'displayPrecision': 5,
  'marginRate': '0.02',
  'maximumOrderUnits': '100000000',
  'maximumPositionSize': '0',
  'maximumTrailingStopDistance': '1.00000',
  'minimumTradeSize': '1',
  'minimumTrailingStopDistance': '0.00050',
  'name': 'EUR_USD',
  'pipLocation': -4,
  'tradeUnitsPrecision': 0,
  'type': 'CURRENCY'},

'EUR_ZAR': {'code': 'EURZAR',
  'displayName': 'EUR/ZAR',
  'displayPrecision': 5,
  'marginRate': '0.05',
  'maximumOrderUnits': '100000000',
  'maximumPositionSize': '0',
  'maximumTrailingStopDistance': '1.00000',
  'minimumTradeSize': '1',
  'minimumTrailingStopDistance': '0.00050',
  'name': 'EUR_ZAR',
  'pipLocation': -4,
  'tradeUnitsPrecision': 0,

  'type': 'CURRENCY'},



            '''

            precision = abs(inst_dict[instrument_oanda]['pipLocation']) #-4 for EUR_USD
            float_point = float(inst_dict[instrument_oanda]['minimumTrailingStopDistance'])*1
            trailing_distance = float(inst_dict[instrument_oanda]['minimumTrailingStopDistance'])*1


            #if row['code'] == 'EURUSD':
                #instrument_oanda = row['code'][0:3]+'_'+row['code'][3:6] #==> EUR_USD
                #precision = 4
                #float_point = 0.0103
                #trailing_distance = 0.0005
            #elif row['code'] == 'XAUUSD':
                #precision = 2
                #float_point = 5
                #trailing_distance = 6

            if row[status_col] == 'NEW': #make the order at Oanda then change db status to 'FILLED'
                #make the order at oanda ----------------------
                #accountID, token = exampleAuth()
                #api = API(access_token=token)
                instrument = row['code']
                price = round(row['open_price'],precision)

                account = self.v20C.dump_account()
                balance = account.details.balance
                unit = round(balance * 0.02 / price)  # RYAN: Adjust point.
                unit = round(balance * 0.2 / price)

                maximumOrderUnits= float(inst_dict[instrument_oanda]['maximumOrderUnits'])
                minimumTradeSize= float(inst_dict[instrument_oanda]['minimumTradeSize'])

                if unit > maximumOrderUnits:
                    unit = maximumOrderUnits
                    print("Unit "+str(unit) +
                          " great than maximumOrderUnits "+str(maximumOrderUnits) +
                          ", reduce unit to max")

                if unit < minimumTradeSize:
                    unit = minimumTradeSize
                    print("Unit "+str(unit) +
                          " less than minimumTradeSize "+str(minimumTradeSize) +
                          ", consider raise balance percent is trading, default is 2%")
                    print("Unit increased to minimumTradeSize")


                #trailing_stop_loss_distance = round(row['his_avg'], 5)+0.0001


                price_high = price * (1 + row['his_avg']) + float_point  # RYAN: Adjust point.
                price_high = round(price_high, precision)

                price_low = price * (1 - row['his_avg']) - float_point   # RYAN: Adjust point.
                price_low = round(price_low, precision)



                bs = None


                if revert:
                    if row['BS'] == 'S':
                        bs = 'B'
                    if row['BS'] == 'B':
                        bs = 'S'
                else:
                    if row['BS'] == 'B':
                        bs = 'B'
                    if row['BS'] == 'S':
                        bs = 'S'


                if bs == 'B':
                    unit = unit
                    take_profit_price = price_high * 1.2   # RYAN: Adjust point.
                    stop_loss_price =  round(price_low * 0.9, precision)  ## RYAN: Adjust point.
                    trailing_stop_loss_distance = (price - price_low)*1.1  #protect profit.  # RYAN: Adjust point.


                if bs == 'S':
                    #price = price * 1.5  #debug, so every Short order will be directly placed.
                    unit = -1 * unit
                    take_profit_price = price_low * 2
                    stop_loss_price =   round(price_high * 0.8, precision)
                    trailing_stop_loss_distance = (price_high - price)*1.1  #protect profit


                if trailing_stop_loss_distance < trailing_distance:
                    trailing_stop_loss_distance = trailing_distance  #minimal point is 5 point (at least for EUR_USD)

                trailing_stop_loss_distance = round(trailing_stop_loss_distance, precision)

                place_market_order = True ;  place_stoplost_order = False #RYAN DEBUG, PLACE MARKET ORDER
                #place_market_order = False;  place_stoplost_order = True  #RYAN DEBUG, PLACE STOP LOST ORDER

                #not in ('CURRENCY'): #THIS MOVED TO t_forex_live_check.py
                #if inst_dict[instrument_oanda]['type'] in ('CFD', 'METAL'):
                #    print("ignore the instrument type other than CURRENCY: "+ inst_dict[instrument_oanda]['type'])
                #    continue



                if place_stoplost_order:
                    response = self.v20C.simple_stop(instrument = instrument_oanda,
                                                           price = str(price),
                                                           unit = unit,
                                                           take_profit_price = str(take_profit_price),
                                                           stop_loss_price = str(stop_loss_price),
                                                           trailing_stop_loss_distance = str(trailing_stop_loss_distance),
                                                           local_oid = str(order_number)
                                                           )
                if place_market_order:
                    response = self.v20C.simple_market(instrument = instrument_oanda,
                                                           unit = unit,
                                                           take_profit_price = str(take_profit_price),
                                                           stop_loss_price = str(stop_loss_price),
                                                           trailing_stop_loss_distance = str(trailing_stop_loss_distance),
                                                           local_oid = str(order_number)
                                                           )



                last_transaction_id=response.body['lastTransactionID']
                oanda_transaction_id = last_transaction_id
                #oanda_order_status = 'UNKNOWN'

                if 'orderCreateTransaction' in response.body.keys():
                    if 'tradeClientExtensions' in response.body['orderCreateTransaction'].dict().keys():
                        tradeClientExtensions = response.body['orderCreateTransaction'].dict()['tradeClientExtensions']
                        db_oid=tradeClientExtensions['id'] #'1'
                        comment=tradeClientExtensions['comment'] #'close_epoch:1521188571'


                    if 'clientExtensions' in response.body['orderCreateTransaction'].dict().keys():
                        clientExtensions = response.body['orderCreateTransaction'].dict()['clientExtensions'] #{'comment': u'close_epoch:1521188571', 'id': u'1'}
                        db_oid = clientExtensions['id']  # '1'
                        comment = clientExtensions['comment']  # 'close_epoch:1521188571'



                    #if response.body['orderCreateTransaction']:
                    if last_transaction_id == response.body['orderCreateTransaction'].dict()['id']:
                        oanda_transaction_id = last_transaction_id
                        oanda_order_status = "PLACED" #pending to meet stop lost start creditial
                        order_change_reason = str(datetime.datetime.now())+" "+"placed the order by oanda;"



                            #the order is created on Oanda.

                if 'orderRejectTransaction' in response.body.keys():
                    error_code = response.body['errorCode']
                    error_message = response.body['errorMessage']
                    if last_transaction_id == response.body['orderRejectTransaction'].dict()['id']:
                        oanda_transaction_id = last_transaction_id #displayed on WEB GUI on Orders and Trades tab.
                        oanda_order_status= "REJECTED"
                        order_change_reason = "reject the order by oanda as "+error_code+","+error_message+";"



                if 'orderCancelTransaction' in response.body.keys():
                    #cancel reason
                    error_message = response.body['orderCancelTransaction'].dict()['reason']
                    if last_transaction_id == response.body['orderCancelTransaction'].dict()['id']:
                        oanda_transaction_id = last_transaction_id #displayed on WEB GUI on Orders and Trades tab.

                        oanda_order_status = "CANCELLED"

                        order_change_reason = "cancel the order by oanda as "+error_message+";"


                if 'orderFillTransaction' in response.body.keys():
                    oanda_transaction_id = response.body['orderFillTransaction'].dict()['id']
                    oanda_order_id = response.body['orderFillTransaction'].dict()['orderID']
                    oanda_order_status = "FILLED"
                    order_change_reason = "filled the order by t_ph_strategy;"


                result_s = ''
                #print("response raw body:")
                result_s += "response raw body=>"
                #pprint.pprint(response.raw_body)
                result_s += response.raw_body

                for k in response.body:
                    #print k;
                    result_s += "\n"+k+"=>"
                    if '__dict__' in dir(response.body[k]):
                        #pprint.pprint(vars(response.body[k]));
                        result_s +=pprint.pformat(vars(response.body[k]), indent=4)
                    else:
                        #pprint.pprint(response.body[k])
                        result_s +=pprint.pformat(response.body[k], indent=4)
                print("Debug: Oanda Simple Stop order reponse:")
                print(result_s)

                # update the order status(open,close) on db.
                #if not revert:
                if True:
                    self.update_order_tracking_forex_tbl(order_number=order_number, oanda_number=oanda_transaction_id, status=oanda_order_status,
                                                   order_change_reason=order_change_reason, revert=revert)

                    print("Update db record, order_number "+str(order_number)+" oanda_transaction_id "+str(oanda_transaction_id) + \
                          " status "+str(oanda_order_status)+" order_change_reason "+str(order_change_reason)
                          )

if __name__ == "__main__":
    strategy = t_ph_strategy()

    while True:
        try:
            #print("updating order ...")
            strategy.Update()
            time.sleep(2)

            pass
        except:
            traceback.print_exc(file=sys.stdout)
            print("catched exception.")

        #except Exception as e:
            #exc_info = sys.exc_info()
            #traceback.print_exception(*exc_info)

            #print("Error: {}".format(e))

            #break
            #print("Exit as encounter exception")
            #exit(0)


