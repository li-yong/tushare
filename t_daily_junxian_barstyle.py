import pandas as pd
import finlib
import finlib_indicator
import os
import logging
from optparse import OptionParser
import tabulate



def verify_a_stock(df):
    #df must have column (code, date, open, low,high,close)

    csv_in = "/home/ryan/DATA/DAY_Global/AG/SH600519.csv"
    csv_in = "/home/ryan/DATA/DAY_Global/AG/SZ000651.csv"
    #csv_in = "/home/ryan/DATA/DAY_Global/AG_INDEX/000001.SH.csv"
    csv_out = "/home/ryan/DATA/result/tmp/SZ000651_del.csv"

    #df = finlib.Finlib().regular_read_csv_to_stdard_df(data_csv=csv_in)
    #df = df.iloc[-300:].reset_index().drop('index', axis=1)
    ###################################
    # Prepare
    ###################################
    df = finlib_indicator.Finlib_indicator().add_ma_ema(df=df, short=5, middle=10, long=20)
    df = finlib_indicator.Finlib_indicator().add_tr_atr(df=df, short=5, middle=10, long=20)



    ######################################################
    #'code', 'date', 'close', 'short_period', 'middle_period', 'long_period', 'jincha_minor', 'jincha_minor_strength', 'sicha_minor', 'sicha_minor_strength', 'jincha_major
    ######################################################


    ######################################################
    # Bar Style
    #         date      code  open  ...  long_lower_shadow  yunxian_buy  yunxian_sell
    #299  20200619  SZ000651  58.5  ...              False        False         False
    ######################################################
    df_bar_style = finlib_indicator.Finlib_indicator().upper_body_lower_shadow(df)
    df_today_bar_style = df_bar_style[-1:].reset_index().drop('index', axis=1)    #<<<<<< TODAY BAR_STYLE



    ######################################################
    # Junxian Style. Only One Row
    #
    #      code      date  close  ...  ema_short  ema_middle   ema_long
    #0  SZ000651  20200619  58.84  ...  58.433884    58.61309  58.362801
    ######################################################
    df_today_junxian_style = finlib_indicator.Finlib_indicator().sma_jincha_sicha_duotou_koutou(df,5,10,20)    #<<<<<< TODAY JUNXIAN


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
    df_a_stock_report = df_today_bar_style.merge(df_today_junxian_style, left_index=True, right_index=True, suffixes=('', '_x')).merge(df_today_price_dict, left_index=True, right_index=True, suffixes=('', '_x'))
    df_a_stock_report['reason'] = df_a_stock_report['reason']+" " +df_a_stock_report['reason_x']
    df_a_stock_report = df_a_stock_report.drop('reason_x', axis=1)

    ######################################################
    #115 columns. Adjust column sequence
    ######################################################
    sp = str(df_a_stock_report.iloc[0]['short_period'])
    mp = str(df_a_stock_report.iloc[0]['middle_period'])
    lp = str(df_a_stock_report.iloc[0]['long_period'])

    col = ['code', 'date', 'open', 'low', 'high',  'close', 'volume', 'short_period', 'middle_period', 'long_period']

    col.extend(['very_strong_down_trend', 'very_strong_up_trend'])

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
    return(df_a_stock_report)



def show_result(file, dir, filebase):
    #file = "/home/ryan/DATA/result/selected/ag_junxian_barstyle.csv"
    #file = "/home/ryan/DATA/result/ag_junxian_barstyle.csv"

    df = pd.read_csv(file,converters={'code': str}, encoding="utf-8")

    col = ['code','name', 'date', 'close', 'duotou_pailie', 'jincha_minor','jincha_major']
    col.extend(['yunxian_buy','yunxian_sell','very_strong_up_trend','very_strong_down_trend'])

    df_1 = df[col]

    df_yunxian_sell = df_1[df_1['yunxian_sell']==True].reset_index().drop('index', axis=1)
    df_yunxian_sell.to_csv(dir+"/"+filebase+"_yunxian_sell.csv", encoding='UTF-8', index=False)

    df_yunxian_buy = df_1[df_1['yunxian_buy']==True].reset_index().drop('index', axis=1)
    df_yunxian_buy.to_csv(dir+"/"+filebase+"_yunxian_buy.csv", encoding='UTF-8', index=False)

    df_duotou_pailie = df_1[df_1['duotou_pailie']==True].reset_index().drop('index', axis=1)
    df_duotou_pailie.to_csv(dir+"/"+filebase+"_duotou_pailie.csv", encoding='UTF-8', index=False)

    df_jincha_minor = df_1[df_1['jincha_minor']==True].reset_index().drop('index', axis=1)
    df_jincha_minor.to_csv(dir+"/"+filebase+"_jincha_minor.csv", encoding='UTF-8', index=False)

    df_jincha_major = df_1[df_1['jincha_major']==True].reset_index().drop('index', axis=1)
    df_jincha_major.to_csv(dir+"/"+filebase+"_jincha_major.csv", encoding='UTF-8', index=False)

    df_very_strong_up_trend = df_1[df_1['very_strong_up_trend']==True].reset_index().drop('index', axis=1)
    df_very_strong_up_trend.to_csv(dir+"/"+filebase+"_very_strong_up_trend.csv", encoding='UTF-8', index=False)

    df_very_strong_down_trend = df_1[df_1['very_strong_down_trend']==True].reset_index().drop('index', axis=1)
    df_very_strong_down_trend.to_csv(dir+"/"+filebase+"_very_strong_down_trend.csv", encoding='UTF-8', index=False)


    logging.info("\ndf_yunxian_sell")
    logging.info(tabulate.tabulate(df_yunxian_sell, headers='keys', tablefmt='psql'))

    logging.info("\ndf_yunxian_buy")
    logging.info(tabulate.tabulate(df_yunxian_buy, headers='keys', tablefmt='psql'))
    logging.info("\ndf_jincha_minor")
    logging.info(tabulate.tabulate(df_jincha_minor, headers='keys', tablefmt='psql'))
    logging.info("\ndf_jincha_major")
    logging.info(tabulate.tabulate(df_jincha_major, headers='keys', tablefmt='psql'))

    logging.info("\ndf_very_strong_up_trend")
    logging.info(tabulate.tabulate(df_very_strong_up_trend, headers='keys', tablefmt='psql'))

    logging.info("\ndf_duotou_pailie")
    logging.info(tabulate.tabulate(df_duotou_pailie, headers='keys', tablefmt='psql'))





def main():

    parser = OptionParser()

    parser.add_option("-s", "--show_result",   action="store_true", dest="show_result_f", default=False, help="show previous calculated result")

    parser.add_option("--min_sample", type="int", action="store", dest="min_sample_f", default=200, help="minimal samples number of input to analysis")

    parser.add_option("-d", "--debug", action="store_true", dest="debug_f", default=False, help="debug ")

    parser.add_option("--save_fig", action="store_true", dest="save_fig_f", default=False, help="save the matplot figure ")

    parser.add_option("--show_fig", action="store_true", dest="show_fig_f", default=False, help="display the matplot figure ")

    parser.add_option("--log_price", action="store_true", dest="log_price_f", default=False, help="log for y-axis price ")

    parser.add_option("-x", "--stock_global", dest="stock_global", help="[CH(US)|KG(HK)|KH(HK)|MG(US)|US(US)|AG(AG)|dev(debug)], source is /home/ryan/DATA/DAY_global/xx/")

    parser.add_option("--selected", action="store_true", dest="selected", default=False, help="only check stocks defined in /home/ryan/tushare_ryan/select.yml")

    #df_rtn = pd.DataFrame()
    df_rtn = pd.DataFrame(columns=["code", "name"])

    (options, args) = parser.parse_args()
    debug_f = options.debug_f
    show_result_f = options.show_result_f
    show_fig_f = options.show_fig_f
    save_fig_f = options.save_fig_f
    min_sample_f = options.min_sample_f
    log_price_f = options.log_price_f
    selected = options.selected
    stock_global = options.stock_global

    rst = finlib.Finlib().get_stock_configuration(selected=selected, stock_global=stock_global)
    out_dir = rst['out_dir']
    csv_dir = rst['csv_dir']
    stock_list = rst['stock_list']
    out_f = out_dir + "/" + stock_global.lower() + "_junxian_barstyle.csv"  #/home/ryan/DATA/result/ag_junxian_barstyle.csv

    if show_result_f:
        show_result(file=out_f, dir=out_dir, filebase= stock_global.lower() + "_junxian_barstyle")
        exit()


    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    i = 0

    for index, row in stock_list.iterrows():
        i += 1
        logging.info(str(i) + " of " + str(stock_list.__len__()))
        name, code = row['name'], row['code']

        csv_f = csv_dir + "/" + code + ".csv"
        logging.info(csv_f)

        if not os.path.isfile(csv_f):
            logging.warning(__file__+" "+"file not exist. " + csv_f)
            continue

        df = finlib.Finlib().regular_read_csv_to_stdard_df(csv_f,add_market=False)

        if (df.__len__() < min_sample_f):
            continue


        code_name_map = stock_list

        code = df.iloc[0]['code']
        name = code_name_map[code_name_map['code'] == code].iloc[0]['name']

        #have to matching df and series index
        df = df.iloc[-min_sample_f:].reset_index().drop('index', axis=1)
        df['name']  = pd.Series([name]*df.__len__(),name='name')



        df_t = verify_a_stock(df=df)

        #print(df_t)
        if not df_t.empty:
            df_rtn = pd.concat([df_rtn, df_t], sort=False).reset_index().drop('index', axis=1)

    df_rtn.to_csv(out_f, encoding='UTF-8', index=False)
    print(df_rtn)
    print("output saved to " + out_f)

    exit(0)



### MAIN ####
if __name__ == '__main__':



    main()
