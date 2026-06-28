
import tushare as ts
import finlib

exam_date = finlib.Finlib().get_last_trading_day()

ts.get_stock_basics()

ts.get_profit_data(2014,3)

print(ts)
