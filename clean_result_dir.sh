
echo "clean up result dir"

cd /home/ryan/DATA/result

rm -f curv_plot/*.png
rm -f curv_plot/SZ*.png
rm -f curv_plot/SH*.png
rm -f curv_plot/{A..Z}*.png
rm -f curv_plot/{0..9}*.png
rm -f curv_plot/* 
rm -f report_20201*
rm -f report_new_dev_20201*
rm -f wei_pan_la_sheng/*
rm -f today/*
rm -f fib_plot/*



cd /home/ryan/DATA/pickle/Stock_Fundamental/akshare/source
rm -f ag_spot_*.csv
rm -f ag_kcb_spot_*.csv
rm -f wei_pan_la_sheng_*.csv

rm -rf /home/ryan/DATA/pickle/Stock_Fundamental/ak_share
