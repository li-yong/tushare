
echo "clean up result dir"

cd /home/ryan/DATA/result

rm -f curv_plot/*.png
rm -f curv_plot/SZ*.png
rm -f curv_plot/SH*.png
rm -f curv_plot/{A..Z}*.png
rm -f curv_plot/{0..9}*.png
rm -f curv_plot/*

rm -f zigzag_div/*

rm -f report_202101*
rm -f report_new_dev_B_202101*
rm -f report_new_dev_S_202101*

rm -f stocks_amount_*
rm -f daily_basic_*
rm -f average_daily_mktcap_sorted*
rm -f average_daily_amount_sorted*
rm -f latest_daily_basic_*

rm -f wei_pan_la_sheng/*20210[1-9]*
rm -f wei_pan_la_sheng/*202110*
rm -f wei_pan_la_sheng/*202111*
rm -f today/*
rm -f fib_plot/*



rm -f report*20210[1-9]*
rm -f report*202110*
rm -f report*202111*
rm -f daily_basic_20210[1-9]*
rm -f daily_basic_202110*
rm -f daily_basic_202111*
rm -f garbage/*
rm -fr result_new_dev_B/*20210[1-9]*
rm -fr result_new_dev_B/*20211[0-1]*
rm -fr result_new_dev_S/*20210[1-9]*
rm -fr result_new_dev_S/*20211[0-1]*



date --date="1 year ago" -I  | rm -f report*`awk -F- '{print $1}'`*
date --date="3 month ago" -I | rm -f report*`awk -F- '{print $1""$2}'`*
date --date="2 month ago" -I | rm -f report*`awk -F- '{print $1""$2}'`*
date --date="1 month ago" -I | rm -f report*`awk -F- '{print $1""$2}'`*



rm -f basic_summary/*20210[1-9]*
rm -f basic_summary/*20211[0-1]*
rm -f basic_summary/*2020*





cd /home/ryan/DATA/pickle/Stock_Fundamental/akshare/source
rm -f ag_spot_*.csv
rm -f ag_kcb_spot_*.csv
rm -f wei_pan_la_sheng_*.csv

rm -rf /home/ryan/DATA/pickle/Stock_Fundamental/ak_share

