import pandas as pd
import finlib
import finlib_indicator

csv_in = "/home/ryan/DATA/DAY_Global/AG_qfq/SH600519.csv"
csv_in = "/home/ryan/DATA/DAY_Global/AG_qfq/SZ000651.csv"
#csv_in = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
csv_out = "/home/ryan/DATA/result/tmp/SZ000651_del.csv"

df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_in)

df = df.iloc[-300:].reset_index().drop('index', axis=1)

### ryan debug start

### ryan debug end


df = pd.DataFrame([''] * df.__len__(), columns=['open_vs_lastclose']).join(df) #today_open - yesterday_close, -1: dikai, 0:pingkai, 1: gaokai
df = pd.DataFrame([''] * df.__len__(), columns=['open_vs_lastclose_perc']).join(df) # (today_open - yesterday_close)/yesterday_close*100
df = pd.DataFrame([''] * df.__len__(), columns=['price_change']).join(df) #today_close - today_open, -1: fall, 0:nochange, 1:rise
df = pd.DataFrame([''] * df.__len__(), columns=['price_change_perc']).join(df) # (today_close - today_open)/today_open*100

dikai_cnt = 0
gaokai_cnt = 0
pingkai_cnt = 0


rst_dict = {}
rst_dict['gaokai']={'increase' : 0}
rst_dict['dikai']={'increase': 0}
rst_dict['pingkai']={'increase': 0}

for i in range(1, df.__len__()-1): #skip first line.

    row = df.iloc[i]
    o = row['open']
    c = row['close']
    h = row['high']
    l = row['low']
    print(str(i)+" "+str(row['date']))

    last_close = df.iloc[i-1]['close']


    if o <= 0 or c <= 0 or last_close <= 0:
        continue

    today_yesterday_inc_perc = round( (o-last_close)/last_close*100, 2) #if buy at yesterday close, by today open the increase.
    df.iloc[i, df.columns.get_loc('open_vs_lastclose_perc')] = today_yesterday_inc_perc
    if today_yesterday_inc_perc > 0:
        #row['open_vs_lastclose'] = 1
        df.iloc[i, df.columns.get_loc('open_vs_lastclose')] = 1
        today_open = "gaokai"
        gaokai_cnt += 1
    elif  today_yesterday_inc_perc == 0:
        #row['open_vs_lastclose'] = 0
        df.iloc[i, df.columns.get_loc('open_vs_lastclose')] = 0
        today_open = "pingkai"
        pingkai_cnt +=1


    elif today_yesterday_inc_perc < 0:
        #row['open_vs_lastclose'] = -1
        df.iloc[i, df.columns.get_loc('open_vs_lastclose')] = -1
        today_open = "dikai"
        dikai_cnt +=1



    tomorrow_close = df.iloc[i+1]['close']
    today_increase_perc = round( (c-o)/o*100, 2) #if buy at today open, by today end the increase.
    today_increase_perc = round( (tomorrow_close-o)/o*100, 2) #if buy at today open, by tomorrow end the increase.
    df.iloc[i, df.columns.get_loc('price_change_perc')] = today_increase_perc
    rst_dict[today_open]['increase'] +=  today_increase_perc

    if today_increase_perc > 0:
        df.iloc[i, df.columns.get_loc('price_change')] = 1
    elif  today_increase_perc == 0:
        df.iloc[i, df.columns.get_loc('price_change')] = 0
    elif today_increase_perc < 0:
        df.iloc[i, df.columns.get_loc('price_change')] = -1

    print(today_open + " " + str(today_increase_perc))
    pass

avg_gaokai_increase = round(rst_dict['gaokai']['increase']/gaokai_cnt, 2)
avg_dikai_increase = round(rst_dict['dikai']['increase']/dikai_cnt, 2)
avg_pingkai_increase = round(rst_dict['pingkai']['increase']/pingkai_cnt, 2)

print("gaokai #"+str(gaokai_cnt)+" all increase "+str(rst_dict['gaokai']['increase'])+" avg increase "+str(avg_gaokai_increase))
print("dikai #"+str(dikai_cnt)+" all increase "+str(rst_dict['dikai']['increase'])+" avg increase "+str(avg_dikai_increase))
print("pingkai #"+str(pingkai_cnt)+" all increase "+str(rst_dict['pingkai']['increase'])+" avg increase "+str(avg_pingkai_increase))

df.to_csv(csv_out, encoding='UTF-8', index=False)

pass



'''
SZ000651 GeLiDianQi

gaokai #1199 all increase 2257.939999999999 avg increase 1.88
dikai #1099 all increase 1366.3299999999988 avg increase 1.24
pingkai #251 all increase 1310.9300000000005 avg increase 5.22

if today_open == yesterday_close, then buy at today_open. Then by the end of today_close, average increase is 5.22%

last 300 records:
gaokai #159 all increase -5.49 avg increase -0.03
dikai #111 all increase 3.610000000000001 avg increase 0.03
pingkai #21 all increase 9.24 avg increase 0.44

sell by tomorrow close
gaokai #159 all increase -208.5599999999999 avg increase -1.31
dikai #110 all increase 1.1400000000000015 avg increase 0.01
pingkai #21 all increase 12.510000000000002 avg increase 0.6



-----------------------------
SH600519
gaokai #141 all increase 60.39999999999997 avg increase 0.43
dikai #142 all increase 13.42 avg increase 0.09
pingkai #16 all increase 2.129999999999999 avg increase 0.13

sell by tomorrow close
gaokai #141 all increase 85.79999999999998 avg increase 0.61
dikai #141 all increase 55.200000000000024 avg increase 0.39
pingkai #16 all increase -2.61 avg increase -0.16


---------------------------
SH000001:SZZS
gaokai #146 all increase 8.659999999999998 avg increase 0.06
dikai #148 all increase 13.290000000000001 avg increase 0.09
pingkai #5 all increase -0.9200000000000002 avg increase -0.18

sell by tomorrow close
gaokai #146 all increase 5.12 avg increase 0.04
dikai #147 all increase 12.909999999999995 avg increase 0.09
pingkai #5 all increase -2.37 avg increase -0.47

'''