
echo "clean up result dir"

cd /home/ryan/DATA/result

rm -f curv_plot/*.png
rm -f curv_plot/SZ*.png
rm -f curv_plot/SH*.png
rm -f curv_plot/{A..Z}*.png
rm -f curv_plot/{0..9}*.png
rm -f curv_plot/* 
rm -f report_202101*
rm -f report_new_dev_B_202101*
rm -f report_new_dev_S_202101*

rm -f stocks_amount_*
rm -f daily_basic_*
rm -f average_daily_mktcap_sorted*
rm -f average_daily_amount_sorted*
rm -f latest_daily_basic_*

rm -f wei_pan_la_sheng/*20210[1-6]*
rm -f today/*
rm -f fib_plot/*



rm -f report*20210[1-6]*
rm -f daily_basic_20210[1-6]*
rm -f garbage/*
rm -fr result_new_dev_B/*20210[1-6]*
rm -fr result_new_dev_S/*20210[1-6]*

rm -f wei_pan_la_sheng/*20210[1-6]*
rm -f basic_summary/*20210[1-6]*
rm -f basic_summary/*2020*





cd /home/ryan/DATA/pickle/Stock_Fundamental/akshare/source
rm -f ag_spot_*.csv
rm -f ag_kcb_spot_*.csv
rm -f wei_pan_la_sheng_*.csv

rm -rf /home/ryan/DATA/pickle/Stock_Fundamental/ak_share

