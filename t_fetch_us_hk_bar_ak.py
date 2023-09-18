import pandas as pd
import finlib
import finlib_indicator
import os
import logging
from optparse import OptionParser
import tabulate
import constant
import akshare as ak
import datetime
import traceback
import sys



def fetch_base(stock_global, csv_dir, stock_list):
    i = 0
    for index, row in stock_list.iterrows():
        i += 1
        name, code = row['name'], row['code']

        if stock_global not in ['HK_AK','US_AK']:
            logging.fatal("please use HK_AK|US_AK only.")
            exit(0)

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(str(i) + " of " + str(stock_list.__len__())+" "+code+" "+name+" "+csv_f)

        if finlib.Finlib().is_cached(csv_f, day=1):
            logging.info("skip file updated in 1 days "+csv_f)
            continue
        try:
            exc_info = sys.exc_info()
            if stock_global == 'HK_AK':
                df = ak.stock_hk_daily(symbol=code, adjust="qfq")
            elif stock_global == 'US_AK':
                df = ak.stock_us_daily(symbol=code, adjust="qfq")

            df = df.reset_index().rename(columns={"index": "date"})
            df['date'] = df['date'].apply(lambda _d: _d.strftime('%Y%m%d'))
            df['name'] = name
            df['code'] = code

            df = finlib.Finlib().adjust_column(df,['code','name','date'])
            df.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info("saved "+csv_f)
        except:
            logging.info(__file__+" "+"\tcaught exception when getting data")
        finally:
            if exc_info == (None, None, None):
                pass  # no exception
            else:
                traceback.print_exception(*exc_info)
            del exc_info

def fetch_daily_spot(stock_global,force_run=False):
    allow_delay_min = 600
    force_fetch = True

    b_dir = "/home/ryan/DATA/pickle/daily_update_source"
    todayS = datetime.datetime.today().strftime("%Y%m%d")

    if stock_global == 'HK_AK':
        b_dir = b_dir+"/HK_AK"
        f_sl = b_dir+"/us_ak_daily_latest.csv"



        if not os.path.isdir(b_dir):
            os.mkdir(b_dir)

        if finlib.Finlib().is_market_open_hk() and (not force_run):
            logging.info("HK market is open now, cannot run daily spot at this time to update base stocks daily.")
            return

        csv_f = b_dir + "/hk_ak_daily_"+todayS+".csv"
        in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='HK', allow_delay_min=allow_delay_min,
                                                            force_fetch=force_fetch)

        in_day_price_df.to_csv(csv_f, encoding='UTF-8', index=False)
        logging.info("today HK mkt close from source akshare saved to " + csv_f)

        if os.path.exists(f_sl):
            os.unlink(f_sl)

        os.symlink(csv_f, f_sl)
        logging.info("HK_AK, symbol link created. "+f_sl+" --> "+csv_f)

    elif stock_global == 'US_AK':
        b_dir = b_dir + "/US_AK"
        f_sl = b_dir+"/us_ak_daily_latest.csv"

        if not os.path.isdir(b_dir):
            os.mkdir(b_dir)

        if finlib.Finlib().is_market_open_us() and (not force_run):
            logging.info("US market is open now, cannot run daily spot at this time to update base stocks daily.")
            return

        csv_f = b_dir + "/us_ak_daily_" + todayS + ".csv"
        in_day_price_df = finlib.Finlib().get_ak_live_price(stock_market='US', allow_delay_min=allow_delay_min,
                                                            force_fetch=force_fetch)
        in_day_price_df.to_csv(csv_f, encoding='UTF-8', index=False)
        logging.info("today US mkt close from source akshare saved to " + csv_f)

        if os.path.exists(f_sl):
            os.unlink(f_sl)

        os.symlink(csv_f, f_sl)
        logging.info("US_AK, symbol link created. "+f_sl+" --> "+csv_f)


def update_base(stock_global, csv_dir, stock_list): #append_daily_spot_to_base
    i = 0
    for index, row in stock_list.iterrows():
        i += 1
        name, code = row['name'], row['code']

        if stock_global not in ['HK_AK','US_AK']:
            logging.fatal("please use HK_AK|US_AK only.")
            exit(0)

        if stock_global == 'HK_AK':
            daily_source_dir = '/home/ryan/DATA/pickle/daily_update_source/HK_AK/hk_ak_daily_'
        elif stock_global == 'US_AK':
            daily_source_dir = '/home/ryan/DATA/pickle/daily_update_source/US_AK/hk_ak_daily_'
        else:
            logging.fatal()
            exit()

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(str(i) + " of " + str(stock_list.__len__())+" update "+code+" "+name+" "+csv_f)

        if finlib.Finlib().is_cached(csv_f, day=0.1):
            logging.info("skip file updated in 1 days "+csv_f)
            continue

        if not os.path.exists(csv_f):
            logging.warning("no such file "+csv_f)
            continue

        df_target = pd.read_csv(csv_f,converters={"code":str, "date":str})
        last_date = datetime.datetime.strptime(df_target.iloc[-1]['date'], '%Y%m%d')
        delta = (datetime.datetime.today() - last_date).days

        for i2 in range(delta):
            added_date = (last_date + datetime.timedelta(i2+1)).strftime('%Y%m%d')
            source_daily_csv_f = daily_source_dir + added_date+".csv"

            if not os.path.exists(source_daily_csv_f):
                logging.warning("no such file "+source_daily_csv_f)
                continue

            source_df = pd.read_csv(source_daily_csv_f, converters={"code":str, "date":str})
            a_stock_today_df = source_df[source_df['code']==code]
            df_target = df_target.append(pd.DataFrame.from_dict({
                'code':[code],
                'name':[name],
                'date':[added_date],
                'open':[a_stock_today_df.iloc[0]['open']],
                'high':[a_stock_today_df.iloc[0]['high']],
                'low':[a_stock_today_df.iloc[0]['low']],
                'close':[a_stock_today_df.iloc[0]['close']],
                'volume':[a_stock_today_df.iloc[0]['volume']],
            })).reset_index().drop('index', axis=1)

            df_target.to_csv(csv_f, encoding='UTF-8', index=False)
            logging.info("appended date "+added_date+" to "+csv_f)
            print(finlib.Finlib().pprint(df_target.tail(2)))


def list(stock_list,csv_dir):
    i = 0
    for index, row in stock_list.iterrows():
        i += 1
        name, code = row['name'], row['code']

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(str(i) + " of " + str(stock_list.__len__())+" "+code+" "+name+" "+csv_f)


def main():

    parser = OptionParser()

    parser.add_option("-s", "--show_result",   action="store_true", dest="show_result_f", default=False, help="show previous calculated result")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=200, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")
    parser.add_option("--force_run", action="store_true", dest="force_run", default=False, help="force_run")

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[US(US)|AG(AG)|dev(debug)|AG_HOLD|HK_HOLD|US_HOLD], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")
    #
    parser.add_option("--fetch_base", action="store_true", dest="fetch_base", default=False, help="fetch base (daily) on US HK stocks one by one. long run.")
    parser.add_option("--fetch_daily_spot", action="store_true", dest="fetch_daily_spot", default=False, help="fetch US HK stock spot. run daily after close.")
    parser.add_option("--update_base", action="store_true", dest="update_base", default=False, help="update spot to base.")
    parser.add_option("--list", action="store_true", dest="list", default=False, help="update spot to base.")

    # parser.add_option("--check_my_ma_allow_delay_min", type="int", action="store", dest="check_my_ma_allow_delay_min", default=30, help="minimal minutes to reuse cached market spot csv.")
    # parser.add_option("--check_my_ma_force_fetch", action="store_true", dest="check_my_ma_force_fetch", default=False,help="force download current market spot via akshare.")
    #

    #df_rtn = pd.DataFrame()
    df_rtn = pd.DataFrame(columns=["code", "name"])

    (options, args) = parser.parse_args()
    debug_f = options.debug_f
    force_run = options.force_run
    min_sample_f = options.min_sample_f
    selected = options.selected
    stock_global = options.stock_global    #



    if not os.path.isdir("/home/ryan/DATA/DAY_Global/akshare"):
        os.mkdir("/home/ryan/DATA/DAY_Global/akshare")

    if not os.path.isdir("/home/ryan/DATA/DAY_Global/akshare/HK"):
        os.mkdir("/home/ryan/DATA/DAY_Global/akshare/HK")

    if not os.path.isdir("/home/ryan/DATA/DAY_Global/akshare/US"):
        os.mkdir("/home/ryan/DATA/DAY_Global/akshare/US")

    if options.fetch_daily_spot:
        fetch_daily_spot(stock_global=stock_global,force_run=force_run)
        exit()


    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    if options.fetch_base:
        fetch_base(stock_global=stock_global, csv_dir=csv_dir, stock_list=stock_list)
        exit()
    elif options.update_base:
        if options.selected:
            logging.fatal("remove --selected when --update_base")
            exit()

        update_base(stock_global=stock_global, csv_dir=csv_dir, stock_list=stock_list)
        exit()
    elif options.list:
        list(stock_list, csv_dir)
        exit()


### MAIN ####
if __name__ == '__main__':

    main()
