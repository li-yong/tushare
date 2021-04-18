#########
# Garbage reason
#########

STOP_PROCESS = "stop process"
BENEISH_LOW_RATE = "beneish low rate"
STOCK_CHANGED_NAME = "stock changed name"
NONE_STANDARD_AUDIT_REPORT = "none standard audit report"
NAG_PROFIT_RECENT_2_YEARS = "negative profit recent two years"
DEBIT_GT_ASSETS = "debit great than assets"
ST_STOCK = "ST stock"
LOW_ROE_PE_RATIO = "low ROE PE ratio"
NAGTIVE_OR_ZERO_PE = "PE <= 0"

VERY_STONG_DOWN_TREND = "very strong down trend"
VERY_STONG_UP_TREND = "very strong up trend"


###############
# Bar style
###############
BAR_SMALL_BODY = "bar small body"
BAR_CROSS_STAR = "bar cross star"
BAR_GUANG_TOU = "bar guang tou"
BAR_GUANG_JIAO = "bar guang jiao"
BAR_LONG_UPPER_SHADOW = "bar long upper shadow"
BAR_LONG_LOWER_SHADOW = "bar long lower shadow"
BAR_YUNXIAN_BUY = "bar yunxian buy"
BAR_YUNXIAN_SELL = "bar yunxian sell"

BAR_STEADY_UP = "bar steady up"




##############
#  Operation
#############

BUY_EARLY = "BUY_EARLY"
SELL_EARLY = "SELL_EARLY"

BUY_MUST = "BUY_MUST"
SELL_MUST = "SELL_MUST"

BUY_CHECK = "BUY_CHECK" 
SELL_CHECK = "SELL_CHECK"

##############
#  Criterial Price
#############
PRICE_HIGH = "price high"
PRICE_LOW  = "price low"

##############
#  Criterial MA. Close Price CROSS OVER/DOWN
#############
CROSS_OVER_SMA5 = "cross over sma5"
CROSS_OVER_SMA21 = "cross over sma21"
CROSS_OVER_SMA60 = "cross over sma60"

CROSS_DOWN_SMA5  = "cross down sma5"
CROSS_DOWN_SMA21 = "cross down sma21"
CROSS_DOWN_SMA60 = "cross down sma60"

#Price vs MA DiKou
CLOSE_ABOVE_MA5_KOUDI = "close above ma5 koudi"
CLOSE_ABOVE_MA21_KOUDI = "close above ma21 koudi"
CLOSE_ABOVE_MA60_KOUDI = "close above ma60 koudi"

CLOSE_UNDER_MA5_KOUDI = "close under ma5 koudi"
CLOSE_UNDER_MA21_KOUDI = "close under ma21 koudi"
CLOSE_UNDER_MA60_KOUDI = "close under ma60 koudi"

MA5_UP_KOUDI_DISTANCE_GT_5 = "ma5 up koudi gt 5 perc"
MA21_UP_KOUDI_DISTANCE_GT_5 = "ma21 up koudi gt 5 perc"
MA55_UP_KOUDI_DISTANCE_GT_5 = "ma55 up koudi gt 5 perc"

MA5_UP_KOUDI_DISTANCE_LT_1 = "ma5 up koudi lt 1 perc"
MA21_UP_KOUDI_DISTANCE_LT_1 = "ma21 up koudi lt 1 perc"
MA55_UP_KOUDI_DISTANCE_LT_1 = " ma55 up koudi lt 1 perc"

TWO_WEEK_FLUC_SMA_5_LT_3 = "two week fluctation sma5 lt 3 perc"
TWO_WEEK_FLUC_SMA_21_LT_3 = "two week fluctation sma21 lt 3 perc"
TWO_WEEK_FLUC_SMA_55_LT_3 = "two week fluctation sma55 lt 3 perc"

#Price vs SMA
CLOSE_ABOVE_SMA60 = "close above sma60"
CLOSE_UNDER_SMA60 = "close under sma60"
CLOSE_GT_SMA60_5_perc = "close great than 1.05 sma60"
CLOSE_GT_SMA200_20_perc = "close great than 1.2 sma200"

SMA21_UNDER_SMA60 = "sma21 under sma60"

MA_JIN_CHA_MINOR = "ma jin cha minor"
MA_JIN_CHA_MAJOR = "ma jin cha major"

MA_SI_CHA_MINOR = "ma si cha minor"
MA_SI_CHA_MAJOR = "ma si cha major"


PV2_VOLUME_RATIO_BOTTOM_10P = "volume_ratio_bottom_10p"
PV2_VOLUME_RATIO_TOP_20P = "volume_ratio_top_20p"
PV2_ZHANGTING_VOLUME_RATIO_LT_1 = "zhangting_volume_ration_lt_1"
PV2_POCKET_PIVOT = "pocket_pivot"
PV2_DIE_TING = "die_ting"
PV2_ZHANG_TING = "zhang_ting"
PV2_PE_TOP_30P = "pe_top_30p"
PV2_PE_BOTTOM_30P = "pe_bottom_30p"
PV2_STABLE_PRICE_VOLUME = "stable_price_volume"




MA_DUO_TOU_PAI_LIE = "ma duo tou pai lie"
MA_DUO_TOU_PAI_LIE_N_days = "ma duo tou pai lie n days"
MA_LAST_KONG_TOU_PAI_LIE_N_days = "current is ma dtpl, last ktpl is n days before "


MA_KONG_TOU_PAI_LIE = "ma kong tou pai lie"
MA_KONG_TOU_PAI_LIE_N_days = "ma kong tou pai lie n days"
MA_LAST_DUO_TOU_PAI_LIE_N_days = "current is ma ktpl, last dtpl is n days before "



SHORT_TREND_UP = "short trend up"
SHORT_TREND_DOWN = "short trend down"

MIDDLE_TREND_UP = "middle trend up"
MIDDLE_TREND_DOWN = "middle trend down"

LONG_TREND_UP = "long trend up"
LONG_TREND_DOWN = "long trend down"


##############
# CLOSE above MA5.
# small number indicates starting stage of burst
# large number indicates middle stage of burst
##############

CLOSE_ABOVE_MA5_N_DAYS = "close above ma5 n days"
CLOSE_NEAR_MA5_N_DAYS = "close near ma5 n days"
MA21_NEAR_MA55_N_DAYS = "ma21 near ma55 n days"



##############
# MA55 closing to MA21
##############
MA55_NEAR_MA21 = "ma55 near ma21"

##############
# Critical MA fast cross MA slow
##############
SMA_CROSS_OVER = "SMA FAST cross over SMA SlOW"

##############
#  Criterial MACD
#############

DIF_LT_0 = "dif less than 0"
DIF_GT_0 = "dif great than 0"

SIG_LT_0 = "sig less than 0"
SIG_GT_0 = "sig great than 0"

DIF_CROSS_OVER_0 = "dif cross over 0"
DIF_CROSS_DOWN_0 = "dif cross down 0"

SIG_CROSS_OVER_0 = "sig cross over 0"
SIG_CROSS_DOWN_0 = "sig cross down 0"

MACD_CROSS_OVER_0 = "macd cross over 0"
MACD_CROSS_DOWN_0 = "macd cross down 0"

MACD_DECLINE_NEAR_0 = "macd decline near 0"
MACD_CLIMB_NEAR_0 = "macd climb near 0"

MACD_DIF_MAIN_OVER_0_N_DAYS = "macd dif_main over 0 n days"
MACD_DEA_SIGNAL_OVER_0_N_DAYS = "macd dea_signal over 0 n days"
MACD_HISTOGRAM_OVER_0_N_DAYS = "macd historgram over 0 n days"


DIF_LT_SIG = "dif less than sig" #down trend
DIF_GT_SIG = "dif great than sig" #up trend

DIF_CROSS_OVER_SIG = "dif cross over sig"
DIF_CROSS_DOWN_SIG = "dif cross down sig"

##############
#  Criterial KDJ
#############



#####################
# index candidates
#####################
TO_BE_KEPT = "To_Be_Kept"
TO_BE_REMOVED = "To_Be_Removed"
TO_BE_ADDED = "To_Be_Added"

HS300_INDEX_BUY_CANDIDATE = 'HS300_INDEX_BUY_CANDIDATE'
HS300_INDEX_SELL_CANDIDATE = 'HS300_INDEX_SELL_CANDIDATE'
SZ100_INDEX_BUY_CANDIDATE = 'SZ100_INDEX_BUY_CANDIDATE'
SZ100_INDEX_SELL_CANDIDATE = 'SZ100_INDEX_SELL_CANDIDATE'
ZZ100_INDEX_BUY_CANDIDATE = 'ZZ100_INDEX_BUY_CANDIDATE'
ZZ100_INDEX_SELL_CANDIDATE = 'ZZ100_INDEX_SELL_CANDIDATE'
SZCZ_INDEX_BUY_CANDIDATE = 'SZCZ_INDEX_BUY_CANDIDATE'
SZCZ_INDEX_SELL_CANDIDATE = 'SZCZ_INDEX_SELL_CANDIDATE'


######################
# pledge ration
######################
PLEDGE_STATISTIC_RATIO_GT_THRESHOLD = 'pledge statistic ratio gt threshold'
PLEDGE_DETAIL_RATIO_SUM_GT_THRESHOLD = 'pledge detail ratio sum gt threshold'


######################
# double bottom
######################
# DOUBLE_BOTTOM_AG_SELECTED = 'double bottom ag selected'
# DOUBLE_BOTTOM_AG = 'double bottom ag'
DOUBLE_BOTTOM_123_LONG_TREND_REVERSE = 'db123 long trend reverse'   # trend reversed, from down to up. buy at early up.
DOUBLE_BOTTOM_123_LONG_TREND_CONTINUE = 'db123 long trend continue' #by at up trend
DOUBLE_BOTTOM_VERY_GOOD_RIGHT_MIN_SLOP_DEGREE = "right min slop degree"
DOUBLE_BOTTOM_VERY_GOOD_RIGHT_MAX_SLOP_DEGREE = "right max slop degree"



##################
## TradingView columns 
##################
TRADINGVIEW_COLS = {
    "Ticker" : "code",  #商品代码,
    "Last" : "close",  #最新价
    "Market Capitalization" : "xxxx",  #市场价值
    "Rating" : "xxxx",  #评级
    "Exchange" : "xxxx",  #交易所
    "Industry" : "xxxx",  #行业
    "Return on Equity (TTM)" : "roe_ttm",  #股本回报率
    "Return on Assets (TTM)" : "roa_ttm",  #资产回报率
    "Enterprise Value (MRQ)" : "xxxx",  #企业价值
    "Enterprise Value/EBITDA (TTM)" : "xxxx",  #企业价值/EBITDA
    "Price to Earnings Ratio (TTM)" : "pe_ttm",  #市盈率
    "Price to Sales (FY)" : "ps",  #市销率
    "Volume" : "volume",  #成交量
    "Shares Float" : "xxxx",  #流通股
    "Relative Volume" : "xxxx",  #相对成交量
    "Operating Margin (TTM)" : "xxxx",  #营业利润率
    "Gross Margin (TTM)" : "xxxx",  #毛利率
    "Volatility Month" : "vola_month",  #月波动率
    "Average Day Range (14)" : "adr_14",  #Average Day Range (14)
    "Volatility" : "vola",  #波动率
    "Sector" : "xxxx",  #板块
    "Average True Range (14)" : "atr_14",  #真实波幅均值ATR
    "Change 15m, %" : "xxxx",  #涨跌幅15分钟
    "Change 1h, %" : "xxxx",  #涨跌幅1小时
    "Change 5m, %" : "xxxx",  #涨跌幅5分钟
    "Pre-market Change from Open %" : "xxxx",  #盘前涨跌%
    "Post-market Change %" : "xxxx",  #盘后帐跌%
    "Change 1m, %" : "xxxx",  #涨跌幅1m
    "Volume*Price" : "amount",  #成交量*价格
    "1-Month Pivot Camarilla P" : "xxxx",  #1个月枢轴点 卡玛利亚 P
    "1-Month Pivot Camarilla R2" : "xxxx",  #1个月枢轴点 卡玛利亚 R2
    "1-Month Pivot Camarilla R1" : "xxxx",  #1个月枢轴点 卡玛利亚 R1
    "Yearly Performance" : "xxxx",  #年表现
    "YTD Performance" : "xxxx",  #年初至今表现
    "Williams Percent Range (14)" : "xxxx",  #威廉百分比变动
    "Weekly Performance" : "xxxx",  #单周表现
    "Volume Weighted Moving Average (20)" : "vwma_20",  #成交量加权移动平均线
    "Volume Weighted Average Price" : "vmap",  #成交量加权平均价格
    "Volatility Week" : "vola_week",  #周波动率
    "Upcoming Earnings Date" : "xxxx",  #将近收益日期
    "Ultimate Oscillator (7, 14, 28)" : "xxxx",  #终极震荡指标UO
    "Total Shares Outstanding (MRQ)" : "xxxx",  #已发行股票总数
    "Total Revenue (FY)" : "xxxx",  #总收入
    "Total Debt (MRQ)" : "xxxx",  #总负债
    "Total Assets (MRQ)" : "xxxx",  #总资产
    "Total Current Assets (MRQ)" : "xxxx",  #总流动资产
    "Stochastic RSI Slow (3, 3, 14, 14)" : "xxxx",  #
    "Stochastic RSI Fast (3, 3, 14, 14)" : "xxxx",  #
    "Stochastic %K (14, 3, 3)" : "xxxx",  #
    "Simple Moving Average (50)" : "xxxx",  #简单移动平均线
    "Stochastic %D (14, 3, 3)" : "xxxx",  #
    "Simple Moving Average (5)" : "xxxx",  #
    "Simple Moving Average (30)" : "xxxx",  #
    "Simple Moving Average (200)" : "xxxx",  #
    "Simple Moving Average (20)" : "xxxx",  #
    "Simple Moving Average (100)" : "xxxx",  #
    "Simple Moving Average (10)" : "xxxx",  #
    "Revenue per Employee (TTM)" : "xxxx",  #员工人均收入
    "1-Month Pivot Camarilla R3" : "xxxx",  #1个月枢轴点 卡玛利亚 R3
    "1-Month Pivot Camarilla S1" : "xxxx",  #
    "1-Month Pivot Camarilla S2" : "xxxx",  #
    "1-Month Pivot Camarilla S3" : "xxxx",  #
    "1-Month Pivot Classic P" : "xxxx",  #
    "1-Month Pivot Classic R1" : "xxxx",  #
    "1-Month Pivot Classic R2" : "xxxx",  #1个月枢轴点 经典 R2
    "1-Month Pivot Classic R3" : "xxxx",  #
    "1-Month Pivot Classic S1" : "xxxx",  #
    "1-Month Pivot Classic S2" : "xxxx",  #
    "1-Month Pivot Classic S3" : "xxxx",  #
    "1-Month Pivot DM P" : "xxxx",  #
    "1-Month Pivot DM R1" : "xxxx",  #1个月枢轴点 DM R1
    "1-Month Pivot DM S1" : "xxxx",  #
    "1-Month Pivot Fibonacci P" : "xxxx",  #
    "1-Month Pivot Fibonacci R1" : "xxxx",  #
    "1-Month Pivot Fibonacci R2" : "xxxx",  #
    "1-Month Pivot Fibonacci R3" : "xxxx",  #
    "1-Month Pivot Fibonacci S1" : "xxxx",  #
    "1-Month Pivot Fibonacci S2" : "xxxx",  #1个月枢轴点 斐波那契 S2
    "1-Month Pivot Fibonacci S3" : "xxxx",  #
    "1-Month Pivot Woodie P" : "xxxx",  #1个月枢轴点 伍迪 P
    "1-Month Pivot Woodie R1" : "xxxx",  #
    "1-Month Pivot Woodie R2" : "xxxx",  #
    "1-Month Pivot Woodie R3" : "xxxx",  #
    "1-Month Pivot Woodie S1" : "xxxx",  #
    "1-Month Pivot Woodie S2" : "xxxx",  #
    "1-Month Pivot Woodie S3" : "xxxx",  #
    "1-Year Beta" : "xxxx",  #1年期Beta值
    "3-Month High" : "xxxx",  #3个月高点
    "3-Month Low" : "xxxx",  #3个月低点
    "3-Month Performance" : "xxxx",  #3个月表现
    "52 Week High" : "xxxx",  #52周最高
    "52 Week Low" : "xxxx",  #52周最低
    "6-Month High" : "xxxx",  #6个月高点
    "6-Month Low" : "xxxx",  #6个月低点
    "6-Month Performance" : "xxxx",  #6个月表现
    "All Time High" : "xxxx",  #历史高点
    "All Time Low" : "xxxx",  #历史低点
    "Aroon Down (14)" : "xxxx",  #
    "Aroon Up (14)" : "xxxx",  #
    "Average Directional Index (14)" : "xxxx",  #平均趋向指数ADX
    "Average Volume (10 day)" : "xxxx",  #平均成交量(10天)
    "Average Volume (30 day)" : "xxxx",  #
    "Average Volume (60 day)" : "xxxx",  #
    "Average Volume (90 day)" : "xxxx",  #
    "Awesome Oscillator" : "xxxx",  #动量震荡指标(AO)
    "Basic EPS (FY)" : "xxxx",  #基本每股收益 FY
    "Basic EPS (TTM)" : "xxxx",  #基本每股收益 TTM
    "Bollinger Lower Band (20)" : "xxxx",  #布林带下轨(20)
    "Bollinger Upper Band (20)" : "xxxx",  #
    "Bull Bear Power" : "xxxx",  #牛熊力量(BBP)
    "Chaikin Money Flow (20)" : "xxxx",  #蔡金资金流量 CMF(20)
    "Change" : "xxxx",  #涨跌
    "Change %" : "xxxx",  #涨跌幅 %,
    "Change 15m" : "xxxx",  #涨跌15分钟
    "Change 1h" : "xxxx",  #
    "Change 1m" : "xxxx",  #
    "Change 4h" : "xxxx",  #
    "Change 4h, %" : "xxxx",  #涨跌幅4小时, %
    "Change 5m" : "xxxx",  #
    "Change from Open" : "xxxx",  #从开始涨跌
    "Change from Open %" : "xxxx",  #从开盘价涨跌%,
    "Commodity Channel Index (20)" : "xxxx",  #CCI指标(20)
    "Country" : "xxxx",  #国家/地区
    "Current Ratio (MRQ)" : "xxxx",  #流动比率(MRQ)
    "Debt to Equity Ratio (MRQ)" : "xxxx",  #债务股本比(MRQ)
    "Dividends Paid (FY)" : "xxxx",  #已付股利
    "Dividends per Share (FY)" : "xxxx",  #每股股利
    "Dividends Yield (FY)" : "xxxx",  #股息率
    "Dividends per Share (MRQ)" : "xxxx",  #每股股息
    "Donchian Channels Lower Band (20)" : "xxxx",  #唐奇安通道(20)下轨
    "Donchian Channels Upper Band (20)" : "xxxx",  #
    "EBITDA (TTM)" : "xxxx",  #EBITDA 税息折旧及摊销前利润 Earnings Before Interest, Taxes, Depreciation and Amortization
    "EPS Diluted (FY)" : "xxxx",  #摊薄每股收益
    "EPS Diluted (MRQ)" : "xxxx",  #摊薄每股收益
    "EPS Diluted (TTM)" : "xxxx",  #摊薄每股收益
    "EPS Forecast (FQ)" : "xxxx",  #每股收益预期
    "Exponential Moving Average (10)" : "xxxx",  #指数移动平均线
    "Exponential Moving Average (100)" : "xxxx",  #
    "Exponential Moving Average (20)" : "xxxx",  #
    "Exponential Moving Average (200)" : "xxxx",  #
    "Exponential Moving Average (30)" : "xxxx",  #
    "Exponential Moving Average (5)" : "xxxx",  #
    "Exponential Moving Average (50)" : "xxxx",  #
    "Gap %" : "xxxx",  #跳空
    "Goodwill" : "xxxx",  #商誉
    "Gross Profit (FY)" : "xxxx",  #毛利润
    "Gross Profit (MRQ)" : "xxxx",  #
    "High" : "xxxx",  #最高价
    "Hull Moving Average (9)" : "xxxx",  #船体移动平均线 Hull MA (9)
    "Ichimoku Cloud Base Line (9, 26, 52, 26)" : "xxxx",  #一目均衡表基准线 (9, 26, 52, 26)
    "Ichimoku Cloud Conversion Line (9, 26, 52, 26)" : "xxxx",  #一目均衡表转换线
    "Ichimoku Lead 1 (9, 26, 52, 26)" : "xxxx",  #
    "Ichimoku Lead 2 (9, 26, 52, 26)" : "xxxx",  #
    "Keltner Channels Lower Band (20)" : "xxxx",  #肯特纳通道下轨
    "Keltner Channels Upper Band (20)" : "xxxx",  #肯特纳通道上轨
    "Last Year Revenue (FY)" : "xxxx",  #去年收入
    "Low" : "xxxx",  #最低价
    "MACD Level (12, 26)" : "xxxx",  #
    "MACD Signal (12, 26)" : "xxxx",  #
    "Momentum (10)" : "xxxx",  #动量指标
    "Money Flow (14)" : "xxxx",  #资金流量 MF
    "Monthly Performance" : "xxxx",  #月表现
    "Moving Averages Rating" : "xxxx",  #移动平均评级
    "Negative Directional Indicator (14)" : "xxxx",  #反向方向性指数 NDI
    "Net Debt (MRQ)" : "xxxx",  #净债务
    "Net Income (FY)" : "xxxx",  #净收入
    "Net Margin (TTM)" : "xxxx",  #净利率
    "Number of Employees" : "xxxx",  #员工数量
    "Open" : "xxxx",  #开盘价
    "Number of Shareholders" : "xxxx",  #股东数量
    "Oscillators Rating" : "xxxx",  #振荡指标评级
    "Parabolic SAR" : "xxxx",  #抛物线转向指标(PSAR)
    "Pattern" : "xxxx",  #形态
    "Positive Directional Indicator (14)" : "xxxx",  #正向方向性指数 PDI
    "Post-market Change" : "xxxx",  #盘后涨跌
    "Post-market Close" : "xxxx",  #盘后时段
    "Post-market High" : "xxxx",  #盘后高点
    "Post-market Low" : "xxxx",  #盘后低点
    "Post-market Open" : "xxxx",  #盘后开市
    "Post-market Volume" : "xxxx",  #盘后成交量
    "Pre-market Change" : "xxxx",  #盘前涨跌
    "Pre-market Change %" : "xxxx",  #盘前时段涨跌 %
    "Pre-market Change from Open" : "xxxx",  #盘前涨跌
    "Pre-market Close" : "xxxx",  #盘前结束
    "Pre-market Gap %" : "xxxx",  #盘前跳空%
    "Pre-market High" : "xxxx",  #盘前高点
    "Pre-market Low" : "xxxx",  #盘前低点
    "Pre-market Open" : "xxxx",  #盘前开市
    "Pretax Margin (TTM)" : "xxxx",  #税前利润率(TTM)
    "Pre-market Volume" : "xxxx",  #盘前成交量
    "Price to Book (FY)" : "xxxx",  #市净率
    "Price to Free Cash Flow (TTM)" : "xxxx",  #价格/自由现金流量(TTM)
    "Price to Book (MRQ)" : "xxxx",  #市净率
    "Price to Revenue Ratio (TTM)" : "xxxx",  #市营率
    "Quick Ratio (MRQ)" : "xxxx",  #速动比率
    "Recent Earnings Date" : "xxxx",  #最近收益日期
    "Rate Of Change (9)" : "xxxx",  #Rate Of Change 
    "Relative Strength Index (14)" : "xxxx",  #RSI
    "Relative Strength Index (7)" : "xxxx",  #
    "Return on Invested Capital (TTM)" : "xxxx",  #投资资本回报率
    "1-Month Low" : "xxxx",  #月低点
    "1-Month High" : "xxxx",  #月高点
}