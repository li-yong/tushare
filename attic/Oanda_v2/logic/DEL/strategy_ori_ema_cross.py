from logic import movingaverage
from logic import MarketTrend


import logging
import traceback

class Strategy_ori_ema_cross(object):

    SHORT_EMA_PERIOD = 5
    LONG_EMA_PERIOD = 20
    
    def __init__(self, oanda, candle_size = 120, email = None, risk = 2):
        self._oanda = oanda
        self._current_candle = None
        self._candle_size = candle_size
        self._short_ema = movingaverage.ExponentialMovingAverage(Strategy_ori_ema_cross.SHORT_EMA_PERIOD)
        self._long_ema = movingaverage.ExponentialMovingAverage(Strategy_ori_ema_cross.LONG_EMA_PERIOD)
        self._logging_current_price = 0.0
        self.trading_enabled = False
        self._action = None


    def Start(self):
        logging.info("Starting strategy")
        # Prefeed the strategy with historic candles
        candle_count = self._long_ema.AmountOfDataStillMissing() + 1
        Candles = self._oanda.GetCandles(candle_count, self._candle_size)
        for c in Candles:
            self._short_ema.Update(c)
            self._long_ema.Update(c)




    def Update(self, _current_candle):

        self._short_ema.Update(_current_candle)
        self._long_ema.Update(_current_candle)


        if (self._short_ema.value > self._long_ema.value):
                self._action = MarketTrend.ENTER_LONG


        if (self._long_ema.value > self._short_ema.value):
                self._action = MarketTrend.ENTER_SHORT


