# coding: utf-8
import finlib
import os.path
import pandas as pd

import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m_%d %H:%M:%S', level=logging.DEBUG)

import os
global debug_global
global force_run_global
global myToken

import requests
import pathlib
from optparse import OptionParser

#This script download [US, US_INDEX] in selected yaml from stooq.com. Run daily.

#url='https://www.nasdaq.com/market-activity/quotes/nasdaq-ndx-index?render=download'
#req = requests.get(url)
#url_content = req.content
#csv_file = open("csv.del", 'wb')
#csv_file.write(url_content)
#csv_file.close()
#logging.info(__file__+" "+"fetched to "+"csv.del")


def stooq_download(code, mkt, days=1, force_fetch=False):
    code = code.lower()
    mkt = mkt.lower()

    dir_o = "/home/ryan/DATA/DAY_Global/stooq" + "/" + mkt.upper()
    csv_o = dir_o + "/" + code.upper() + ".csv"
    if not os.path.isdir(dir_o):
        pathlib.Path(dir_o).mkdir(parents=True, exist_ok=True)

    if finlib.Finlib().is_cached(csv_o, day=days) and (not force_fetch):
        logging.info(__file__+" "+"file is updated in " + str(days) + " days. not fetch again. " + csv_o)
        return

    url_head = 'https://stooq.com/q/d/l/?s='
    url_tail = '&i=d'
    if code == "dow" and mkt == "us_index":
        url = url_head + "^dji" + url_tail
    elif code == "sp500" and mkt == "us_index":
        url = url_head + "^spx" + url_tail
    elif code == "nasdaq100" and mkt == "us_index":
        url = url_head + "^ndx" + url_tail
    else:
        url = url_head + code + "." + mkt + url_tail
    req = requests.get(url)
    url_content = req.content
    csv_file = open(csv_o, 'wb')
    csv_file.write(url_content)
    csv_file.close()
    logging.info(__file__+" "+"fetched to " + csv_o)

    df = pd.read_csv(csv_o, skiprows=1, names=['date', 'open', 'high', 'low', 'close', 'volume'], encoding="utf-8")
    df = pd.DataFrame([code] * df.__len__(), columns=['code']).join(df)

    df.to_csv(csv_o, encoding='UTF-8', index=False)
    logging.info(__file__+" "+"formatted, csv length " + str(df.__len__()))
    finlib.Finlib().pprint(df.iloc[-1:])
    pass


parser = OptionParser()
parser.add_option("--force_fetch", action="store_true", dest="force_fetch", default=False, help="force fetch data")

(options, args) = parser.parse_args()
force_fetch = options.force_fetch

rst = finlib.Finlib().load_select()

for mkt in ['US_INDEX', 'US']:
    #for mkt in ['HK']: ## stooq doesn't have HK data
    i = 0
    stock_list = rst[mkt]
    for index, row in stock_list.iterrows():
        i += 1
        name, code = row['name'], row['code']
        logging.info(str(i) + " of " + str(stock_list.__len__()) + " fetching stooq " + code + " " + name)
        stooq_download(code=code, mkt=mkt, force_fetch=force_fetch)
        pass

logging.info(__file__+" "+"script completed. ")

#https://quant.stackexchange.com/questions/26078/how-can-one-query-the-google-finance-api-for-dow-jones-and-sp-500-values
#mkdir /home/ryan/DATA/DAY_Global/US_INDEX
#cd /home/ryan/DATA/DAY_Global/US_INDEX
#curl -k -o dow.csv 'https://stooq.com/q/d/l/?s=^dji&i=d'
#curl -k -o sp500.csv 'https://stooq.com/q/d/l/?s=^spx&i=d'
