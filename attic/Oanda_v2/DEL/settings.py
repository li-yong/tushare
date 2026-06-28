#V20 config start
HOSTNAME = "api-fxpractice.oanda.com"
STREAMING_HOSTNAME = "stream-fxpractice.oanda.com"
PORT = 443
SSL = "true"
TOKEN = "43b32f61afca7adc3a85ebd5d8fcfb8a-015be2ef2131fa8db6d68a675d2c1a64"
USERNAME = "sunraise2007"
DATETIME_FORMAT = "RFC3339"
ACTIVE_ACCOUNT = "101-011-7847380-001"
#V20 config end



#BELOW ARE V10 API
# Account settings
ACCOUNT_ID = "7930886"
ACCESS_TOKEN = "980a700f21bd8d47196f08fb51d30aa0-8e47f1dad016cda90f2950e8675ba518"
ENVIRONMENT = "practice" # change this to "live" for production

# Pair to trade.
# only one allowed for now - run multiple instances if you want multiple pairs
ACCOUNT_CURRENCY = "USD"
INSTRUMENT = "EUR_USD"
#INSTRUMENT = "XAU_USD"

# Home / Base exchange rate
# Examples: instrument: "USD_JPY", home: "USD", home/base: "USD_USD"
#           instrument: "EUR_USD", home: "USD", home/base: "EUR_USD"
#           instrument: "AUD_CAD", home: "USD", home/base: "USD_AUD"
HOME_BASE_CURRENCY_PAIR = "EUR_USD"
HOME_BASE_CURRENCY_PAIR_DEFAULT_EXCHANGE_RATE = 0.88

# Size of candles in minutes
#CANDLES_MINUTES = 120
#CANDLES_MINUTES = 60*24  #294 profile
#CANDLES_MINUTES = 1/60
#CANDLES_MINUTES = 60*5
CANDLES_MINUTES = 1

#Risk settings
MAX_PERCENTAGE_ACCOUNT_AT_RISK = 2 # NO more then 2% of account per trade

#Email credentials
EMAIL_RECIPIENT = "youremail@gmail.com"
EMAIL_FROM="oandabot@yourserver.com"
EMAIL_SERVER="mail.yourserver.com"
EMAIL_PORT=25
EMAIL_PASSWORD="SuchSecurePasswordStoredUnecrypted"

# Special bot name for identification
# In case you have many and want to distinguish between them 
# Leave default if only running one bot
BOT_NAME = "OANDAPYBOT"

# For backtesting
#BACKTESTING_FILENAME = "backtest/data/EURUSD/DAT_ASCII_EURUSD_M1_2015.csv"
#BACKTESTING_FILENAME = "backtest/data/EURUSD/DAT_ASCII_EURUSD_M1_2015_debug.csv"
BACKTESTING_FILENAME = "backtest/data/XAUUSD/XAUUSD_debug.csv"
#BACKTESTING_FILENAME = "backtest/data/XAUUSD/XAUUSD.csv"
#BACKTESTING_FILENAME = "backtest/data/XAUUSD/XAUUSD_2016.csv"
#BACKTESTING_FILENAME = "backtest/data/XAUUSD/XAUUSD_2016_100f.csv"
