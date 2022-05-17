import pandas as pd
import finlib
import finlib_indicator
import os
import logging
from optparse import OptionParser
import tabulate
import constant
from scipy import stats
import datetime

def verify_a_stock(df,ma_short=5,ma_middle=10,ma_long=20,period='D'):
    #df must have columns [code, date, open, low,high,close]

    # csv_in = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    # csv_in = "/home/ryan/DATA/DAY_Global/AG/SZ000651.csv"
    # #csv_in = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
    # csv_out = "/home/ryan/DATA/result/tmp/SZ000651_del.csv"

    #df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_in)
    #df = df.iloc[-300:].reset_index().drop('index', axis=1)
    ###################################
    # Prepare
    ###################################

    df = finlib_indicator.Finlib_indicator().add_ma_ema(df=df, short=ma_short, middle=ma_middle, long=ma_long)
    df = finlib_indicator.Finlib_indicator().add_tr_atr(df=df, short=ma_short, middle=ma_middle, long=ma_long)

    ######################################################
    # Buy/Sell condition based on MA4_MA27_Distance
    ######################################################
    df_ma4_ma27_condition = finlib_indicator.Finlib_indicator().buy_sell_decision_based_on_ma4_ma27_distance_condition(df, ma_short=ma_short, ma_middle=ma_middle,ma_long=ma_long,period=period)
    df_ma4_ma27_condition = df_ma4_ma27_condition[-1:].reset_index().drop('index', axis=1)    #<<<<<< TODAY

    ######################################################
    #'code', 'date', 'close', 'short_period', 'middle_period', 'long_period', 'jincha_minor', 'jincha_minor_strength', 'sicha_minor', 'sicha_minor_strength', 'jincha_major
    ######################################################


    ######################################################
    # Bar Style
    #         date      code  open  ...  long_lower_shadow  yunxian_buy  yunxian_sell
    #299  20200619  SZ000651  58.5  ...              False        False         False
    ######################################################
    df_bar_style = finlib_indicator.Finlib_indicator().upper_body_lower_shadow(df, ma_short=ma_short, ma_middle=ma_middle,ma_long=ma_long)
    df_today_bar_style = df_bar_style[-1:].reset_index().drop('index', axis=1)    #<<<<<< TODAY BAR_STYLE



    ######################################################
    # Junxian Style. Only One Row
    #
    #      code      date  close  ...  ema_short  ema_middle   ema_long
    #0  SZ000651  20200619  58.84  ...  58.433884    58.61309  58.362801
    ######################################################
    df_today_junxian_style = finlib_indicator.Finlib_indicator().sma_jincha_sicha_duotou_koutou(df,ma_short,ma_middle,ma_long)    #<<<<<< TODAY JUNXIAN


    ######################################################
    #H1, price 59.0, freq perc in 300 bars 96.4 freq 10.2
    #L1, price 58.0, freq perc in 300 bars 100.0 freq 12.0
    ######################################################
    analyzer_price_dict = finlib_indicator.Finlib_indicator().price_counter(df)

    price_dict = {'price_occurence_sum':[analyzer_price_dict['price_freq_dict'][list(analyzer_price_dict['price_freq_dict'].keys())[0]]['sum']]}

    for level in ['h1','l1','h2','l2','h3','l3','h4','l4','h5','l5']:
        if level in list(analyzer_price_dict.keys()):
            price_dict[level+"_price"]=[analyzer_price_dict[level]['price']]
            price_dict[level+"_frequency_percent"]=[analyzer_price_dict[level]['frequency_percent']]
            price_dict[level+"_occurrence_percent"]=[analyzer_price_dict[level]['occurrence_percent']]
            price_dict[level+"_occurrence"]=[analyzer_price_dict[level]['occurrence']]
        else:
            price_dict[level+"_price"]=None
            price_dict[level+"_frequency_percent"]=None
            price_dict[level+"_occurrence_percent"]=None
            price_dict[level+"_occurrence"]=None

    df_today_price_dict = pd.DataFrame.from_dict(price_dict)

    ######################################################
    # Get a stock report
    ######################################################
    #only df_today_bar_style, df_today_junxian_style, df_ma4_ma27_condition has reason column

    df_a_stock_report = df_today_bar_style.merge(df_today_junxian_style, left_index=True, right_index=True, suffixes=('', '_x'))
    df_a_stock_report['reason'] = df_a_stock_report['reason']+" " +df_a_stock_report['reason_x']
    df_a_stock_report['action'] = df_a_stock_report['action']+" " +df_a_stock_report['action_x']
    df_a_stock_report = df_a_stock_report.drop('reason_x', axis=1)
    df_a_stock_report = df_a_stock_report.drop('action_x', axis=1)


    df_a_stock_report = df_a_stock_report.merge(df_ma4_ma27_condition, left_index=True, right_index=True, suffixes=('', '_x'))
    df_a_stock_report['reason'] = df_a_stock_report['reason']+" " +df_a_stock_report['reason_x']
    df_a_stock_report['action'] = df_a_stock_report['action']+" " +df_a_stock_report['action_x']
    df_a_stock_report = df_a_stock_report.drop('reason_x', axis=1)
    df_a_stock_report = df_a_stock_report.drop('action_x', axis=1)

    df_a_stock_report = df_a_stock_report.merge(df_today_price_dict, left_index=True, right_index=True, suffixes=('', '_x'))

    #drop dup columns.
    df_a_stock_report = df_a_stock_report.drop('code_x', axis=1)
    df_a_stock_report = df_a_stock_report.drop('date_x', axis=1)
    df_a_stock_report = df_a_stock_report.drop('close_x', axis=1)

    ######################################################
    #115 columns. Adjust column sequence
    ######################################################
    sp = str(df_a_stock_report.iloc[0]['short_period'])
    mp = str(df_a_stock_report.iloc[0]['middle_period'])
    lp = str(df_a_stock_report.iloc[0]['long_period'])

    col = ['code', 'date', 'open', 'low', 'high',  'close', 'volume', 'short_period', 'middle_period', 'long_period']

    col.extend(['very_strong_down_trend', 'very_strong_up_trend'])

    #####################  ma4 ma27 distance
    # N/A

    #####################  junxian
    col.extend(['trend_short', 'trend_short_strength'])
    col.extend(['trend_middle', 'trend_middle_strength'])
    col.extend(['trend_long'])
    col.extend(['duotou_pailie',   'duotou_pailie_last_bars', 'last_kongtou_pailie_date', 'last_kongtou_pailie_n_days_before'])
    col.extend(['kongtou_pailie',  'kongtou_pailie_last_bars','last_duotou_pailie_date',  'last_duotou_pailie_n_days_before'])
    col.extend(['jincha_minor', 'jincha_minor_strength'])
    col.extend(['jincha_major', 'jincha_major_strength'])
    col.extend(['sicha_minor', 'sicha_minor_strength'])
    col.extend(['sicha_major', 'sicha_major_strength'])


    ##################### Bar
    col.extend(['yunxian_buy','yunxian_sell'])
    col.extend(['guangtou','guangjiao','small_body','cross_star'])
    col.extend(['long_upper_shadow','long_lower_shadow' ])
    col.extend(['upper_shadow_len','body_len','lower_shadow_len'])

    ##################### SMA, EMA, tr, etc
    col.extend(['sma_short_'+sp,'sma_middle_'+mp,'sma_long_'+lp,'ema_short_'+sp,'ema_middle_'+mp,'ema_long_'+lp])
    col.extend(['tr', 'atr_short_'+sp, 'atr_middle_'+mp, 'atr_long_'+lp])

    ##################### price frequence
    col.extend(['price_occurence_sum'])
    col.extend(['l5_price'])
    col.extend(['l4_price'])
    col.extend(['l3_price'])
    col.extend(['l2_price'])
    col.extend(['l1_price'])
    col.extend(['h1_price'])
    col.extend(['h2_price'])
    col.extend(['h3_price'])
    col.extend(['h4_price'])
    col.extend(['h5_price'])

    col.extend(['l5_frequency_percent'])
    col.extend(['l4_frequency_percent'])
    col.extend(['l3_frequency_percent'])
    col.extend(['l2_frequency_percent'])
    col.extend(['l1_frequency_percent'])
    col.extend(['h1_frequency_percent'])
    col.extend(['h2_frequency_percent'])
    col.extend(['h3_frequency_percent'])
    col.extend(['h4_frequency_percent'])
    col.extend(['h5_frequency_percent'])

    col.extend(['l5_occurrence'])
    col.extend(['l4_occurrence'])
    col.extend(['l3_occurrence'])
    col.extend(['l2_occurrence'])
    col.extend(['l1_occurrence'])
    col.extend(['h1_occurrence'])
    col.extend(['h2_occurrence'])
    col.extend(['h3_occurrence'])
    col.extend(['h4_occurrence'])
    col.extend(['h5_occurrence'])

    col.extend(['l5_occurrence_percent'])
    col.extend(['l4_occurrence_percent'])
    col.extend(['l3_occurrence_percent'])
    col.extend(['l2_occurrence_percent'])
    col.extend(['l1_occurrence_percent'])
    col.extend(['h1_occurrence_percent'])
    col.extend(['h2_occurrence_percent'])
    col.extend(['h3_occurrence_percent'])
    col.extend(['h4_occurrence_percent'])
    col.extend(['h5_occurrence_percent'])


    df_a_stock_report = finlib.Finlib().adjust_column(df=df_a_stock_report, col_name_list=col)
    # df_a_stock_report = df_a_stock_report[col]
    return(df_a_stock_report)



def show_result(file, dir, filebase,selected, stock_global):
    stock_global = stock_global.lower()
    logging.info("\ndf_yunxian_sell")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.BAR_YUNXIAN_SELL,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    logging.info("\ndf_yunxian_buy")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.BAR_YUNXIAN_BUY,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    logging.info("\ndf_jincha_minor")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA_JIN_CHA_MINOR,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    logging.info("\ndf_jincha_major")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA_JIN_CHA_MAJOR,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    logging.info("\ndf_very_strong_up_trend")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.VERY_STONG_UP_TREND,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    logging.info("\ndf_duotou_pailie")
    logging.info(tabulate.tabulate(finlib_indicator.Finlib_indicator().get_indicator_critirial(constant.MA_DUO_TOU_PAI_LIE,market=stock_global, selected=selected).head(5), headers='keys', tablefmt='psql'))

    exit(0)



def cnt_jin_cha_si_cha(ma_short, ma_middle,stock_global,selected, remove_garbage=False):
    a = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global,remove_garbage=remove_garbage, qfq=True)

    df_stock_list = a['stock_list']

    debug = True
    debug = False
    if debug:
        df_stock_list = df_stock_list[df_stock_list['code']=='SZ002647']


    df_csv_dir = a['csv_dir']
    df_out_dir = a['out_dir']
    df_rtn = pd.DataFrame()

    check_days = 200
    result_csv=df_out_dir+"/"+"jin_cha_si_cha_cnt.csv"

    i=1
    for index, row in df_stock_list.iterrows():
        name, code = row['name'], row['code']
        logging.info(str(i)+" of "+str(df_stock_list.__len__())+" , "+str(code)+" "+name)
        i+=1

        csv = df_csv_dir + "/"+ code+".csv"
        df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv)

        if df.__len__() < check_days:
            logging.info(str(code)+" " + name+ " insufficient data, expect "+str(check_days)+" actual "+str(df.__len__()))
            continue

        df = finlib_indicator.Finlib_indicator().count_jin_cha_si_cha(df=df, check_days=check_days,
                                                                      code=code,name=name,ma_short=ma_short, ma_middle=ma_middle)
        df_rtn = df_rtn.append(df)


    df_rtn = finlib.Finlib().add_stock_name_to_df(df=df_rtn)


    df_rtn = df_rtn.sort_values(by='sum_perc')
    df_rtn['ma_across_rare_score'] = df_rtn['sum_perc'].apply(
        lambda _d: round(1 - stats.percentileofscore(df_rtn['sum_perc'], _d) / 100, 4))

    df_rtn['jincha_sicha_days_ratio_score'] = df_rtn['jincha_sicha_days_ratio'].apply(
        lambda _d: round(stats.percentileofscore(df_rtn['jincha_sicha_days_ratio'], _d) / 100, 2))

    df_rtn['ma_x_score'] = round(0.7*df_rtn['jincha_sicha_days_ratio_score']+ 0.3*df_rtn['ma_across_rare_score'],2)

    df_rtn = df_rtn.sort_values(by='ma_x_score',ascending=False)

    df_rtn = df_rtn.reset_index().drop('index', axis=1)


    logging.info("head 10 df:\n"+finlib.Finlib().pprint(df_rtn.head(10)))
    logging.info("tail 10 df:\n"+finlib.Finlib().pprint(df_rtn.tail(10)))

    df_rtn.to_csv(result_csv, encoding='UTF-8', index=False)
    logging.info("jin_cha_si_cha cnt saved to "+result_csv+" , len "+str(df_rtn.__len__()))

    return()



def main():

    parser = OptionParser()
    parser.add_option("--ma_short", type="int", action="store", dest="ma_short", default=4, help="MA short period")
    parser.add_option("--ma_middle", type="int", action="store", dest="ma_middle", default=27, help="MA middle period")
    parser.add_option("--ma_long", type="int", action="store", dest="ma_long", default=60, help="MA long period")

    parser.add_option("-s", "--show_result",   action="store_true", dest="show_result_f", default=False, help="show previous calculated result")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=200, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)|AG_HOLD|HK_HOLD|US_HOLD], source is /home/ryan/DATA/DAY_global/xx/")
    parser.add_option("-p", "--period", dest="period", default='D', help="[D|W|M]")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")
    parser.add_option("--remove_garbage", action="store_true", dest="remove_garbage", default=False, help="remove garbage stocks from list before proceeding list")

    parser.add_option("--check_my_ma", action="store_true", dest="check_my_ma", default=False, help="run before market close")
    parser.add_option("--check_my_ma_allow_delay_min", type="int", action="store", dest="check_my_ma_allow_delay_min", default=30, help="minimal minutes to reuse cached market spot csv.")
    parser.add_option("--check_my_ma_force_fetch", action="store_true", dest="check_my_ma_force_fetch", default=False,help="force download current market spot via akshare.")


    parser.add_option("--hong_san_bin", action="store_true", dest="hong_san_bin", default=False,help="hong_san_bin bar style finder")
    parser.add_option("--calc_ma_across_price", action="store_true", dest="calc_ma_across_price", default=False,help="calculate target price let ma_short equal ma_middle")
    parser.add_option("--calc_ma_across_count", action="store_true", dest="calc_ma_across_count", default=False,help="calculate ma_short across ma_middle counts")


    # df_rtn = pd.DataFrame()
    df_rtn = pd.DataFrame(columns=["code", "name"])

    (options, args) = parser.parse_args()
    debug_f = options.debug_f
    show_result_f = options.show_result_f
    min_sample_f = options.min_sample_f
    selected = options.selected
    stock_global = options.stock_global
    period = options.period
    remove_garbage = options.remove_garbage

    check_my_ma = options.check_my_ma
    check_my_ma_allow_delay_min = options.check_my_ma_allow_delay_min
    check_my_ma_force_fetch = options.check_my_ma_force_fetch

    hong_san_bin = options.hong_san_bin
    calc_ma_across_price = options.calc_ma_across_price
    calc_ma_across_count = options.calc_ma_across_count

    ma_short = options.ma_short
    ma_middle = options.ma_middle
    ma_long = options.ma_long

    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global, remove_garbage=remove_garbage, qfq=True)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']
    out_f = ''

    if period == 'D':
        out_f = out_dir + "/" + stock_global.lower() + "_junxian_barstyle.csv"  #/home/ryan/DATA/result/ag_junxian_barstyle.csv
    elif period == 'W':
        out_f = out_dir + "/" + stock_global.lower() + "_junxian_barstyle_w.csv"  #/home/ryan/DATA/result/ag_junxian_barstyle.csv
    elif period == 'M':
        out_f = out_dir + "/" + stock_global.lower() + "_junxian_barstyle_m.csv"  #/home/ryan/DATA/result/ag_junxian_barstyle.csv
    else:
        logging.fatal("unknown period " + str(period))
        exit()


    if show_result_f:
        show_result(file=out_f, dir=out_dir, filebase= stock_global.lower() + "_junxian_barstyle", selected=selected, stock_global=stock_global)
        exit()
    elif check_my_ma:
        finlib_indicator.Finlib_indicator().check_my_ma(selected=selected, stock_global=stock_global,
                                                        allow_delay_min=check_my_ma_allow_delay_min,
                                                        force_fetch=check_my_ma_force_fetch)
        exit()
    elif hong_san_bin:
        df_rtn = finlib_indicator.Finlib_indicator().hong_san_bin()
        exit()
    elif calc_ma_across_price:
        df_rtn = finlib_indicator.Finlib_indicator().get_price_let_mashort_equal_malong(ma_short=ma_short, ma_middle=ma_middle, debug=debug_f)
        exit()
    elif calc_ma_across_count:
        df_rtn = cnt_jin_cha_si_cha(ma_short=ma_short, ma_middle=ma_middle,stock_global=stock_global,selected=selected, remove_garbage=remove_garbage,)
        exit()

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

   ###  Start of verify every stocks
    df_all = finlib.Finlib().load_all_ag_qfq_data(days=600)
    df_all = finlib.Finlib().add_stock_name_to_df(df=df_all)
    # df = finlib.Finlib().remove_garbage(df=df)

    codes = df_all['code'].unique()
    codes.sort()

    i = 0
    for c in codes:
        i += 1
        logging.info(str(i) + " of " + str(codes.__len__()))

        df = df_all[df_all['code']==c].reset_index().drop('index', axis=1)

        name, code = df.iloc[0]['name'], df.iloc[0]['code']

        # check df size at the beginging.
        if (df.__len__() < options.min_sample_f):
            logging.info("raw, insufficient sample for "+str(code)+" "+name+' , expected at least '+str(min_sample_f)+" actual "+str(df.__len__()))
            continue

        # resample
        if period=='D':
            df = df
        elif period=='W':
            min_sample_f = int(options.min_sample_f/5)
            df = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_weekly']
            df['date'] = df['date'].apply(lambda _d: datetime.datetime.strftime(_d, "%Y%m%d"))
        elif period=='M':
            min_sample_f = int(options.min_sample_f / 20)
            df = finlib.Finlib().daily_to_monthly_bar(df_daily=df)['df_monthly']
            df['date'] = df['date'].apply(lambda _d: datetime.datetime.strftime(_d, "%Y%m%d"))
        else:
            logging.fatal("Unknown period "+str(period))
            exit()

        # check df size after resample
        if (df.__len__() < min_sample_f):
            logging.info("period "+str(period)+", insufficient sample for "+str(code)+" "+name+' , expected at least '+str(min_sample_f)+" actual "+str(df.__len__()))
            continue

        code_name_map = stock_list

        code = df.iloc[0]['code']
        _tmp_df =  code_name_map[code_name_map['code'] == code]
        if _tmp_df.empty:
            logging.warning("juxian_barstyle, empty df for code "+str(code))
            continue
        else:
            name =_tmp_df.iloc[0]['name']

        #have to matching df and series index
        df = df.iloc[-min_sample_f:].reset_index().drop('index', axis=1)
        df['name']  = pd.Series([name]*df.__len__(),name='name')

        logging.info("verifying "+code + " " +name)
        df_t = verify_a_stock(df=df, ma_short=ma_short, ma_middle=ma_middle, ma_long=ma_long,period=period)

        #print(df_t)
        if not df_t.empty:
            df_rtn = pd.concat([df_rtn, df_t], sort=False).reset_index().drop('index', axis=1)
            df_rtn.to_csv(out_f, encoding='UTF-8', index=False)
            logging.debug("tmp output saved to " + out_f)

    df_ma_across_score = pd.read_csv('/home/ryan/DATA/result/jin_cha_si_cha_cnt.csv')

    df_rtn = pd.merge(df_rtn, df_ma_across_score[['code','name','ma_x_score']], on='code', how='inner', suffixes=('', '_x')).drop('name_x', axis=1)

    df_rtn.to_csv(out_f, encoding='UTF-8', index=False)
    logging.info("output saved to " + out_f)

    exit(0)


### MAIN ####
if __name__ == '__main__':
    main()