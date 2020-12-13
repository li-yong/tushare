
echo "clean up result dir"

cd /home/ryan/DATA/result

rm -f curv_plot/*.png
rm -f curv_plot/SZ*.png
rm -f curv_plot/SH*.png
rm -f curv_plot/{A..Z}*.png
rm -f curv_plot/{0..9}*.png
rm curv_plot/* 
rm -f report_20201*.cv
rm -f report_20201*
rm -f wei_pan_la_sheng/*
rm -f today/*
rm -f fib_plot/*

