# coding: utf-8

import finsymbols
import os
import pickle
import pandas as pd

import pprint
import sys
from bs4 import BeautifulSoup
from finsymbols.symbol_helper import *

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

logging.info(__file__+" "+"\n")
logging.info(__file__+" "+"SCRIPT STARTING " + " ".join(sys.argv))

HKHS_Prepare = False
HKHS_Prepare = True

if HKHS_Prepare:
    #manually made csv from https://zh.wikipedia.org/zh-cn/%E6%81%92%E7%94%9F%E6%8C%87%E6%95%B8
    #this is to covert the short code to full code, eg 1 --> 00001
    csv = "/home/ryan/Downloads/hkhs_index.csv"
    df = pd.read_csv(csv, dtype=str)

    #appending a new column
    new_value_df = pd.DataFrame([0] * df.__len__(), columns=['code_full'])  #
    df = new_value_df.join(df)

    for i in range(df.__len__()):
        code = df.iloc[i]['code']
        code_len = len(code)
        #hk code is 5 char

        full_code = '0' * (5 - code_len) + code
        logging.info(full_code)

        df.iloc[i, df.columns.get_loc('code_full')] = full_code

    ###### HK Heng SHeng ################
    csv = "/home/ryan/DATA/pickle/INDEX_US_HK/hkhs.csv"
    df = df.drop('code', axis=1)
    df.rename(columns={"code_full": "code"}, inplace=True)
    df = df[['code', 'name', 'flow_perc', 'weight']]
    df.to_csv(csv, encoding='UTF-8', index=False)
    logging.info(__file__+" "+"HKHS saved to " + csv)

pass

########## SP 500 ##########
csv = "/home/ryan/DATA/pickle/INDEX_US_HK/sp500.csv"
logging.info(__file__+" "+"getting SP500")
sp500 = finsymbols.get_sp500_symbols()

#conver list of dict to df
sp500_df = pd.DataFrame(sp500)
sp500_df.rename(columns={"symbol": "code"}, inplace=True)
cols = ['code', 'company', 'headquarters', 'industry', 'sector']
sp500_df = sp500_df[cols]
sp500_df.to_csv(csv, encoding='UTF-8', index=False)
logging.info(__file__+" "+"SP500 saved to " + csv)

logging.info('script completed')
os._exit(0)

# http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download

logging.info(__file__+" "+"getting nasdaq")
nasdaq = finsymbols.get_nasdaq_symbols()
logging.info(__file__+" "+"dumpping nasdaq")
pickle.dump(nasdaq, open("/home/ryan/DATA/pickle/INDEX_US/nasdaq.pickle", "wb"))

logging.info(__file__+" "+"getting amex")
amex = finsymbols.get_amex_symbols()
logging.info(__file__+" "+"dumpping amex")
pickle.dump(amex, open("/home/ryan/DATA/pickle/INDEX_US/amex.pickle", "wb"))

logging.info(__file__+" "+"getting nyse")
nyse = finsymbols.get_nyse_symbols()
logging.info(__file__+" "+"dumpping nyse")
pickle.dump(nyse, open("/home/ryan/DATA/pickle/INDEX_US/nyse.pickle", "wb"))
