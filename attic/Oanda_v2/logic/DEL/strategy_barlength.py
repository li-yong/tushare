from logic import movingaverage
from logic import MarketTrend


import logging
import traceback

class Strategy_barlength(object):

    #SHORT_EMA_PERIOD = 7
    SHORT_EMA_PERIOD = 1
    #LONG_EMA_PERIOD = 21
    LONG_EMA_PERIOD = 2
    TIME_OF_AVG_BAR_LENGTH=5

    def __init__(self, oanda, candle_size = 120, email = None, risk = 2):
        self._oanda = oanda
        self._current_candle = None
        self._candle_size = candle_size
        self._short_ema = movingaverage.SimpleMovingAverage(Strategy_barlength.SHORT_EMA_PERIOD)
        self._long_ema = movingaverage.SimpleMovingAverage(Strategy_barlength.LONG_EMA_PERIOD)
        self._logging_current_price = 0.0
        self.trading_enabled = False
        self._action=None


    def Start(self):
        logging.info("Starting strategy")
        # Prefeed the strategy with historic candles
        candle_count = self._long_ema.AmountOfDataStillMissing() + 1
        Candles = self._oanda.GetCandles(candle_count, self._candle_size)
        for c in Candles:
            self._short_ema.Update(c)
            self._long_ema.Update(c)



    def Update(self, _current_candle):
        # at this point, the candle is closed.
        self._short_ema.Update(_current_candle)
        self._long_ema.Update(_current_candle)



        logging.info("short_ema: "+str(self._short_ema.value))
        logging.info("long_ema: "+str(self._long_ema.value))

        delta=_current_candle.High - _current_candle.Low
        _current_candle_bar_length=abs(delta)





        logging.info("avg bar length "+str( self._long_ema.bar_length_avg)+", this bar length "+str(_current_candle_bar_length))
        if (_current_candle_bar_length> Strategy_barlength.TIME_OF_AVG_BAR_LENGTH * self._long_ema.bar_length_avg) and (delta>0):
            self._action=MarketTrend.ENTER_LONG

        if (_current_candle_bar_length > Strategy_barlength.TIME_OF_AVG_BAR_LENGTH * self._long_ema.bar_length_avg) and (delta<0):
            self._action=MarketTrend.ENTER_SHORT

