import oandapy
from settings import *
from util.mail import Email
from util.ui import CursedUI
from exchange.oanda import Oanda
from exchange.oanda import OandaExceptionCode
from logic.strategy import Strategy
import traceback
import logging
import sys
import pandas as pd
import numpy as np
import re

from settings import *
from datetime import datetime, timedelta



oa_tmp = Oanda(ACCESS_TOKEN,
           ACCOUNT_ID,
           'XAU_USD',
           ACCOUNT_CURRENCY,
           HOME_BASE_CURRENCY_PAIR,
           HOME_BASE_CURRENCY_PAIR_DEFAULT_EXCHANGE_RATE,
           ENVIRONMENT)

response = oa_tmp._oanda.get_instruments(ACCOUNT_ID)

for ins in response['instruments']:
    instrument =  ins['instrument']
    instrument_file =  ins['instrument']+".csv"

    oa = Oanda(ACCESS_TOKEN,
               ACCOUNT_ID,
               instrument,
               ACCOUNT_CURRENCY,
               HOME_BASE_CURRENCY_PAIR,
               HOME_BASE_CURRENCY_PAIR_DEFAULT_EXCHANGE_RATE,
               ENVIRONMENT)

    DATETIME_FMT = "%FT%TZ"
    d_start = datetime(year=2017, month=1, day=1)
    delta_1d = timedelta(days=1)
    delta_neg_1s = timedelta(seconds=-1)

    for i in range(30): #Jan has 31 days, start from 1st, so adding 31 - 1 = 30
    #for i in range(450):
        d_end = d_start + delta_1d+ delta_neg_1s
        print("loading "+ instrument + d_start.strftime(DATETIME_FMT) )

        try:
            response = oa._oanda.get_history(instrument=instrument,
                                  granularity='M1',
                                  start=d_start.strftime(DATETIME_FMT), end=d_end.strftime(DATETIME_FMT),
                                  candleFormat="midpoint"
                                  )

        except:
            print(instrument+" catch except on day start "+d_start.strftime(DATETIME_FMT))
            d_start += delta_1d
            continue

        code2 = response['instrument']
        print("loaded " + instrument + d_start.strftime(DATETIME_FMT))

        for candle in response['candles']:
            t = str(candle['time'])  # '2018-02-01T00:00:00.000000Z'
            t = re.sub('\.000000Z$','', t)  #'2018-02-01T00:00:00'
            t = re.sub('T',' ', t)  #'2018-02-01 00:00:00'


            o = str(candle['openMid'])
            h = str(candle['highMid'])
            l = str(candle['lowMid'])
            c = str(candle['closeMid'])
            vol = str(candle['volume'])


            line = code2+","+t+","+o+","+h+","+l+","+c+","+vol+"\n"

            fh = open(instrument_file, "a")
            fh.write(line)
            fh.close()



        d_start += delta_1d



exit(1)