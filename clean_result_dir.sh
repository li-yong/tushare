
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

rm -f wei_pan_la_sheng/*202102*
rm -f today/*
rm -f fib_plot/*



cd /home/ryan/DATA/pickle/Stock_Fundamental/akshare/source
rm -f ag_spot_*.csv
rm -f ag_kcb_spot_*.csv
rm -f wei_pan_la_sheng_*.csv

rm -rf /home/ryan/DATA/pickle/Stock_Fundamental/ak_share

rm -f report_202103*
rm -f report_202104*
rm -f report*_202103*
rm -f daily_basic_202104*
rm -f report*202104[0-1]*
rm -f wei_pan_la_sheng/*

