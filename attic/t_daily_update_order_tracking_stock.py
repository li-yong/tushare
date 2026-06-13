# coding: utf-8

import os.path
import pandas as pd
import datetime

from sqlalchemy import create_engine
import mysql.connector

from optparse import OptionParser

parser = OptionParser()

parser.add_option("-b", "--base_dir", dest="base_dir", default='/home/ryan/DATA/DAY_Global/AG', type="str", help="base_dir, default /home/ryan/DATA/DAY_Global/AG")

parser.add_option("-a", "--add_miss", action="store_true", dest="add_miss", default=False, help="adding miss data. Use with -e/--exam_date to specify date")

parser.add_option("-p", "--pickle_only", action="store_true", dest="pickle_only", default=False, help="get today data, save to pickel, then exit")

parser.add_option("-e", "--exam_date", dest="exam_date", help="exam_date, YYYY-MM--DD, no default value, missing will calc the nearest trading day, most time is today")

(options, args) = parser.parse_args()

base_dir = options.base_dir
add_miss = options.add_miss
pickle_only = options.pickle_only
exam_date = options.exam_date

#load index
index_f = "/home/ryan/DATA/DAY_Global/AG/SH000001.csv"

if os.path.isfile(index_f):
    df_index = pd.read_csv(index_f, skiprows=1, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])
else:
    print("FATAL: Not found index file " + index_f)
    exit()

engine = create_engine('mysql://root:admin888.@_@@127.0.0.1/ryan_stock_db?charset=utf8')
mysql_host = '127.0.0.1'
cnx = mysql.connector.connect(host=mysql_host, user='root', password='admin888.@_@', database="ryan_stock_db")
cursor = cnx.cursor()

sql = 'SELECT * FROM `order_tracking_stock` WHERE `status`=\'OPEN\''
open_stocks = pd.read_sql_query(sql, engine)
for i in range(open_stocks.__len__()):
    id = open_stocks.iloc[i, open_stocks.columns.get_loc('id')]
    code = open_stocks.iloc[i, open_stocks.columns.get_loc('code')]
    name = open_stocks.iloc[i, open_stocks.columns.get_loc('name')]
    buy_date = open_stocks.iloc[i, open_stocks.columns.get_loc('buy_date')]
    buy_price = open_stocks.iloc[i, open_stocks.columns.get_loc('buy_price')]
    status = open_stocks.iloc[i, open_stocks.columns.get_loc('status')]

    if int(buy_price) == 0:
        print("not update record as buy_price is 0, record " + str(id) + ", " + code)
        continue

    print('updating record ' + str(id) + ", " + code)

    if code == 'SZ300238':
        print("debug")
    #print('updating record '+str(id)+", "+code+", "+name) #remove doesn't show name
    #Traceback (most recent call last):
    #File "/home/ryan/tushare_ryan/t_daily_update_order_tracking_stock.py", line 55, in <module>
    #print('updating record '+str(id)+", "+code+", "+name)

    db_tbl = 'order_tracking_stock'

    update_sql = ("UPDATE `" + db_tbl + "`  " "SET D1_inc = %(D1_inc)s, D2_inc = %(D2_inc)s, D3_inc = %(D3_inc)s, D5_inc = %(D5_inc)s, " " D10_inc = %(D10_inc)s, D15_inc = %(D15_inc)s, D20_inc = %(D20_inc)s, " " D60_inc = %(D60_inc)s, D120_inc = %(D120_inc)s, D240_inc = %(D240_inc)s," " ID_1 =%(ID_1)s, ID_2 = %(ID_2)s, ID_3 = %(ID_3)s, ID_5 = %(ID_5)s, " " ID_10 = %(ID_10)s, ID_15 = %(ID_15)s, ID_20 = %(ID_20)s, " " ID_60 = %(ID_60)s, ID_120 = %(ID_120)s, ID_240 = %(ID_240)s " " WHERE `id` = %(id)s")

    data_sql = {}
    data_sql['id'] = int(id)
    data_sql['D1_inc'] = data_sql['D2_inc'] = data_sql['D3_inc'] = data_sql['D5_inc'] = 0.0
    data_sql['D10_inc'] = data_sql['D15_inc'] = data_sql['D20_inc'] = 0.0
    data_sql['D60_inc'] = data_sql['D120_inc'] = data_sql['D240_inc'] = 0.0

    data_sql['ID_1'] = data_sql['ID_2'] = data_sql['ID_3'] = data_sql['ID_5'] = 0.0
    data_sql['ID_10'] = data_sql['ID_15'] = data_sql['ID_20'] = 0.0
    data_sql['ID_60'] = data_sql['ID_120'] = data_sql['ID_240'] = 0.0

    date_1 = buy_date + datetime.timedelta(1)
    date_2 = buy_date + datetime.timedelta(2)
    date_3 = buy_date + datetime.timedelta(3)
    date_5 = buy_date + datetime.timedelta(5)
    date_10 = buy_date + datetime.timedelta(10)
    date_15 = buy_date + datetime.timedelta(15)
    date_20 = buy_date + datetime.timedelta(20)
    date_60 = buy_date + datetime.timedelta(60)
    date_120 = buy_date + datetime.timedelta(120)
    date_240 = buy_date + datetime.timedelta(240)

    csv_f = base_dir + '/' + code + '.csv'

    if os.path.isfile(csv_f):
        df_csv = pd.read_csv(csv_f, skiprows=1, header=None, names=['code', 'date', 'o', 'h', 'l', 'c', 'vol', 'amnt', 'tnv'])

        # read SZZS SH00001 stock index data
        szzs_buy_date_row = df_index.loc[df_index['date'] == str(buy_date)]
        szzs_buy_date_index = szzs_buy_date_row.index.values[0]
        szzs_buy_date_close = szzs_buy_date_row['c'].values[0]

        # read this stock
        buy_date_row = df_csv.loc[df_csv['date'] == str(buy_date)]
        buy_date_index = buy_date_row.index.values[0]

        for i in (1, 2, 3, 5, 10, 15, 20, 60, 120, 240):

            date_i_row_index = buy_date_index + i
            if date_i_row_index < df_csv.__len__():
                print("\t" + code + " have data on Buy_date(" + str(buy_date) + ") + " + str(i) + ", " + str(eval("date_" + str(i))))
                date_i_row = df_csv.iloc[buy_date_index + i]
                day_price = date_i_row['c']
                if int(day_price) == 0:  #ting pai
                    day_price = buy_price

                data_sql['D' + str(i) + '_inc'] = round((day_price - buy_price) / buy_price * 100, 1)

                #SZZS increase% at the same period
                if szzs_buy_date_index + i >= df_index.__len__():
                    print("ERROR: the index doesn't have the date record, run t_get_index.py")
                    pass
                else:
                    szzs_date_i_row = df_index.iloc[szzs_buy_date_index + i]
                    data_sql['ID_' + str(i)] = round((szzs_date_i_row['c'] - szzs_buy_date_close) / szzs_buy_date_close * 100, 1)
                    pass

        cursor.execute(update_sql, data_sql)
        #print(update_sql)
        #print(data_sql)
        cnx.commit()
        pass

    pass

sql = 'SELECT avg(`D1_inc`), avg(`D2_inc`), avg(`D3_inc`), avg(`D5_inc`), avg(`D10_inc`), avg(`D15_inc`), avg(`D20_inc`),' \
      ' avg(`D60_inc`), avg(`D120_inc`), avg(`D240_inc`) FROM `order_tracking_stock` WHERE 1 '
sql_rtn = pd.read_sql_query(sql, engine)

D1_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D1_inc`)')], 2)
D2_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D2_inc`)')], 2)
D3_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D3_inc`)')], 2)
D5_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D5_inc`)')], 2)
D10_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D10_inc`)')], 2)
D15_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D15_inc`)')], 2)
D20_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D20_inc`)')], 2)
D60_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D60_inc`)')], 2)
D120_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D120_inc`)')], 2)
D240_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`D240_inc`)')], 2)


sql = 'SELECT avg(`ID_1`), avg(`ID_2`), avg(`ID_3`), avg(`ID_5`), avg(`ID_10`), avg(`ID_15`), avg(`ID_20`),' \
      ' avg(`ID_60`), avg(`ID_120`), avg(`ID_240`) FROM `order_tracking_stock` WHERE 1 '
sql_rtn = pd.read_sql_query(sql, engine)

ID1_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_1`)')], 2)
ID2_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_2`)')], 2)
ID3_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_3`)')], 2)
ID5_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_5`)')], 2)
ID10_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_10`)')], 2)
ID15_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_15`)')], 2)
ID20_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_20`)')], 2)
ID60_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_60`)')], 2)
ID120_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_120`)')], 2)
ID240_inc_avg = round(sql_rtn.iloc[0, sql_rtn.columns.get_loc('avg(`ID_240`)')], 2)

print('D1_increase_avg: ' + str(D1_inc_avg) + "%" + ", index inc avg: " + str(ID1_inc_avg) + "%")
print('D2_increase_avg: ' + str(D2_inc_avg) + "%" + ", index inc avg: " + str(ID2_inc_avg) + "%")
print('D3_increase_avg: ' + str(D3_inc_avg) + "%" + ", index inc avg: " + str(ID3_inc_avg) + "%")
print('D5_increase_avg: ' + str(D5_inc_avg) + "%" + ", index inc avg: " + str(ID5_inc_avg) + "%")
print('D10_increase_avg: ' + str(D10_inc_avg) + "%" + ", index inc avg: " + str(ID10_inc_avg) + "%")
print('D20_increase_avg: ' + str(D20_inc_avg) + "%" + ", index inc avg: " + str(ID20_inc_avg) + "%")
print('D60_increase_avg: ' + str(D60_inc_avg) + "%" + ", index inc avg: " + str(ID60_inc_avg) + "%")
print('D120_increase_avg: ' + str(D120_inc_avg) + "%" + ", index inc avg: " + str(ID120_inc_avg) + "%")
print('D240_increase_avg: ' + str(D240_inc_avg) + "%" + ", index inc avg: " + str(ID240_inc_avg) + "%")

sql = 'SELECT * FROM `order_tracking_stock` WHERE 1 '
sql_rtn = pd.read_sql_query(sql, engine)

csv_f = '/home/ryan/DATA/result/stock_order_tracking_dump.csv'
sql_rtn.to_csv(csv_f, encoding='UTF-8', index=False)
print("db table dump to " + csv_f)

engine.dispose()
cursor.close()
cnx.close()
print("Script Completed.")

os._exit(0)
