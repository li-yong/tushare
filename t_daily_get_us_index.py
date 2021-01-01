# coding: utf-8

# # List of all symbol / tickers in US's major indices and exchanges
#
# In this notebook you can get the list of all constituents / components and tickers in S&P500, S&P400, Dow Jones and NASDAQ100
# Retrieved from wikipedia and Nasdaq websites
#
# You'll get a dictionary with dataframes and csv files

# In[1]:

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
# import urllib.request
import logging
import finlib

output_csv_base = '~/DATA/pickle/INDEX_US_HK'

# ## Get components / constituents of indices

# In[2]:

#this code is stable as long as the following addresses are not being changed and thier structure not change
urls = {
    'nasdqa100': 'https://en.wikipedia.org/wiki/NASDAQ-100',
    'SNP500': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
    'sp400': 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies',
    'dow': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
}
indices = {}
for url in urls:
    logging.info("\ngetting " + url + " from " + urls[url])

    # Remove False if need download from Nasdaq site directly (manually). e.g WikiPedia has out-of-date data.
    if url == 'nasdqa100' and False:
        #the file is prepared by dowloading from https://www.nasdaq.com/market-activity/quotes/nasdaq-ndx-index
        #using 'Save Page WE' plugin on Chrome.
        #Do this every 2 months manually.  (NASDAQ Adjusted in month (4.20,6.22,7,20, 8.24,10.19,12.21))
        f = '/home/ryan/Downloads/nas100.html'
        bs = BeautifulSoup(open(f, "r").read(), 'lxml')
        table = str(bs.find('table', {'class': 'nasdaq-ndx-index__table'}))
        df = pd.read_html(table)[0]

    else:
        result = requests.get(urls[url], verify=False, timeout=10)
        c = result.content
        bs = BeautifulSoup(c, 'lxml')

        if url == 'nasdqa100':
            table = str(bs.find('table', {'id':"constituents"}))
        else:
            table = str(bs.find('table', {'class': 'wikitable sortable'}))

        df = pd.read_html(table)[0]

        if url == 'nasdqa100':
            df = df[['Ticker','Company']]
            df.columns = ['code', 'name']
        elif url == 'dow':
            df = df[['Symbol', 'Company']]
            df.columns = ['code', 'name']
        elif url == 'SNP500':
            #df = df[['Ticker symbol', 'Security', 'CIK']]
            df = df[['Symbol', 'Security', 'CIK', 'Founded']]
            df.columns = ['code', 'name', 'cik', 'founded']
        else:  #SP400
            df = df[['Ticker symbol', 'Security']]
            df.columns = ['code', 'name']

    df = df.rename(columns={'Symbol': 'code', 'Name': 'name'})
    df.to_csv(output_csv_base + "/" + url + '.csv', index=False, encoding='utf-8')
    logging.info("saved " + output_csv_base + "/" + url + '.csv')
    indices[url] = df.reset_index(drop=True)

logging.info('Done, script completed.')
exit()

urls = {'NYSE': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download', 'AMEX': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download', 'NASDAQ': 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download'}

exchanges = {}
for i in urls:
    logging.info("\ngetting exchange from " + i)
    exchanges[i] = pd.read_csv(urls[i], usecols=[0, 1], header=0)

merged_exchanges = pd.DataFrame(columns=['code'])
for i in exchanges:
    exchanges[i].columns = ['code', 'name']
    exchanges[i].to_csv(output_csv_base + "/" + i + '.csv', index=False, encoding='utf-8')
    logging.info("saved " + output_csv_base + "/" + i + '.csv')

    merged_exchanges = merged_exchanges.merge(exchanges[i], 'outer', ['code'], sort=True).fillna('')

merged_exchanges.to_csv(output_csv_base + "/" + 'merged_exchanges.csv', index=False, encoding='utf-8')
logging.info("saved " + output_csv_base + "/" + 'merged_exchanges.csv')
