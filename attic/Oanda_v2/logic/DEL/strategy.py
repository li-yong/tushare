import datetime
from exchange.oanda import Oanda
from logic.candle import Candle
from logic import movingaverage
from logic import MarketTrend
from logic.risk import RiskManager
from logic.timestop import TimeStop
from logic.stoploss import StopLoss
from logic.takeprofit import  TakeProfit
from logic.trailingstop import TrailingStop

from strategy_ori_ema_cross import Strategy_ori_ema_cross
from strategy_barlength import  Strategy_barlength
from t_ph_strategy import t_ph_strategy




import logging
import traceback

class Strategy(object):

    #SHORT_EMA_PERIOD = 7
    SHORT_EMA_PERIOD = 1
    #LONG_EMA_PERIOD = 21
    LONG_EMA_PERIOD = 2
    TIME_OF_AVG_BAR_LENGTH=5

    def __init__(self, oanda, candle_size = 120, email = None, risk = 2):
        self._oanda = oanda
        self._oanda.SubscribeTicker(self)
        self._current_candle = None
        self._candle_size = candle_size
        self._risk = RiskManager(oanda, risk)
        self._email = email
        #self._short_ema = movingaverage.ExponentialMovingAverage(Strategy.SHORT_EMA_PERIOD)
        self._short_ema = movingaverage.SimpleMovingAverage(Strategy.SHORT_EMA_PERIOD)
        #self._long_ema = movingaverage.ExponentialMovingAverage(Strategy.LONG_EMA_PERIOD)
        self._long_ema = movingaverage.SimpleMovingAverage(Strategy.LONG_EMA_PERIOD)
        self._timestop = TimeStop()
        self._logging_current_price = 0.0
        self.trading_enabled = False
        self._stoploss= StopLoss()
        self._takeprofit=TakeProfit()
        self._trailingstop=TrailingStop(atr_period_length=2)

        #Ryan: register strategy here.
        #self._str_ema_cross=Strategy_ori_ema_cross(oanda,candle_size,email,risk)
        #self._str_bar_length=Strategy_barlength(oanda,candle_size,email,risk)
        self._str_ph_strategy = t_ph_strategy(oanda)
        self._str_ph_strategy.Update("candle")



    def Start(self):
        logging.info("Starting strategy")
        #self._str_ema_cross.Start()
        #self._str_bar_length.Start()
        self._str_ph_strategy.Start()

        self._oanda.StartPriceStreaming()
        self.trading_enabled = True



    def PauseTrading(self):
        logging.info("Pausing strategy")
        self.trading_enabled = False

    def ResumeTrading(self):
        logging.info("Resuming strategy")
        self.trading_enabled = True

    def TradingStatus(self):
        return self.trading_enabled

    def SetTradingStatus(self, tstatus):
        #self._str_ema_cross.SetTradingStatus(self, tstatus)
        #self._str_bar_length.SetTradingStatus(self, tstatus)
        self._str_ph_strategy.SetTradingStatus(self, tstatus)

        self.trading_enabled = tstatus

    def Stop(self):
        logging.info("Stop strategy")
        self.SetTradingStatus(False)
        self._oanda.StopPriceStreaming()
        #self._str_ema_cross._oanda.StopPriceStreaming()
        #self._str_bar_length._oanda.StopPriceStreaming()
        self._str_ph_strategy._oanda.StopPriceStreaming()

    def Update(self, datapoint):

        if not isinstance(datapoint, Candle):#Ryan: This is for backtest <<< may not true. 2018.02
            if not self._current_candle:
                openTime = datapoint["now"]
                closeTime = datapoint["now"] + datetime.timedelta(minutes=self._candle_size)
                self._current_candle = Candle(openTime, closeTime)
                logging.info("A candle is born, openTime:"+ str(openTime)+" CloseTime "+str(closeTime))
                self._current_candle.Update(datapoint)
            else:
                self._current_candle.Update(datapoint)  #<<< live trading hit here
            self._logging_current_price = datapoint["value"]
        else: #Ryan: This is Real stream trading
            self._current_candle = datapoint  #Ryan: What is datapoint?
            self._logging_current_price = datapoint.Close

        # Check if it is Friday night and we should seize trading
        self._timestop.Update(datapoint)
        _state = self._timestop.GetState()
        if _state == MarketTrend.STOP_LONG or _state == MarketTrend.STOP_SHORT:
            if (self._oanda.CurrentPosition() > 0):
                logging.info("Timing Stop fired, TGIF!: "+str(_state) + " price: "+ str(self._logging_current_price))
                self.ClosePosition()
                return

       #RYAN: Check on each ticket, even the candle is not completed.
        self._stoploss.Update(datapoint)

        if MarketTrend.STOP_LONG == self._stoploss.state:
            logging.info("close order as the long stop loss condition meat")
            self.ClosePosition()
            self._stoploss.state = MarketTrend.NO_STOP

        if MarketTrend.STOP_SHORT == self._stoploss.state:
            logging.info("close order as the short stop loss condition meat")
            self.ClosePosition()
            self._stoploss.state=MarketTrend.NO_STOP


        self._trailingstop.Update(datapoint)

        if MarketTrend.STOP_LONG == self._trailingstop.state:
            logging.info("close order as the long trailing stop condition meat")
            self.ClosePosition()
            self._trailingstop.state = MarketTrend.NO_STOP

        if MarketTrend.STOP_SHORT == self._trailingstop.state:
            logging.info("close order as the short trailing stop condition meat")
            self.ClosePosition()
            self._trailingstop.state=MarketTrend.NO_STOP


        if not self._current_candle.SeenEnoughData():
            logging.info("Candle still open, not see enough data")
            return

        # The stratege based on candles, not tickers
        #_current_candle=datapoint
        #self._str_ema_cross.Update(self._current_candle)
        #self._str_bar_length.Update(self._current_candle)
        self._str_ph_strategy.Update(self._current_candle)


        self._stoploss.Update(self._current_candle)
        self._trailingstop.Update(self._current_candle)


      
        if MarketTrend.STOP_LONG == self._stoploss.state:
            self.ClosePosition()
            self._stoploss.state = MarketTrend.NO_STOP


        if MarketTrend.STOP_SHORT == self._stoploss.state:
            self.ClosePosition()
            self._stoploss.state=MarketTrend.NO_STOP

        if MarketTrend.STOP_LONG == self._trailingstop.state:
            self.ClosePosition()
            self._trailingstop.state = MarketTrend.NO_STOP


        if MarketTrend.STOP_SHORT == self._trailingstop.state:
            self.ClosePosition()
            self._trailingstop.state=MarketTrend.NO_STOP


        #Make a LONG order

        #if MarketTrend.ENTER_LONG == self._str_ema_cross._action and \
        #   MarketTrend.ENTER_LONG == self._str_bar_length._action:
        if MarketTrend.ENTER_LONG == self._str_ph_strategy._action:
            logging.info("Both Strategy agree to Long")
            if (self._oanda.CurrentPosition() > 0) and (self._oanda.CurrentPositionSide == MarketTrend.ENTER_LONG):
                return
            else:
                self.ClosePosition()
                self.Buy(self._current_candle.Close)
                self._stoploss.SetStop(self._logging_current_price, MarketTrend.ENTER_LONG)  # << RYAN add
                self._trailingstop.SetStop(MarketTrend.ENTER_LONG)


        #Make a SHORT order
        #if MarketTrend.ENTER_SHORT == self._str_ema_cross._action and \
        #   MarketTrend.ENTER_SHORT == self._str_bar_length._action:
        if MarketTrend.ENTER_SHORT == self._str_ph_strategy._action:
            logging.info("Both Strategy agree to Short")
            if (self._oanda.CurrentPosition() > 0) and (self._oanda.CurrentPositionSide == MarketTrend.ENTER_SHORT):
                return
            else:
                self.ClosePosition()
                self.Sell(self._current_candle.Close)
                self._stoploss.SetStop(self._logging_current_price, MarketTrend.ENTER_SHORT)  # << RYAN add
                self._trailingstop.SetStop(MarketTrend.ENTER_SHORT)

        self._current_candle = None
        #self._str_ema_cross._current_candle=None
        #self._str_bar_length._current_candle=None
        self._str_ph_strategy._current_candle=None
        logging.info("self._current_candle set to None")




    def Buy(self, open_time):
        logging.info("OpenB order at "+str(open_time) +", Price "+  str(self._logging_current_price) )
        #print("OpenB order at "+str(open_time) +", Price "+  str(self._logging_current_price) )

        self._oanda._OrderOpenTime=open_time
        
        if not self.trading_enabled:
            logging.info("Strategy trading disabled, doing nothing")
            return
        
        # Enter the long position on the instrument
        units = self._risk.GetLongPositionSize()
        if units > 2000: #Ryan: max allowed unit is 2000
            units=2000

        logging.info("Got the number of units to trade from RiskManager: "+str(units))
        if units == 0:
            logging.info("Cant trade zero units, doing nothing")
            return

        try:
            self._oanda.Buy(units)
        except Exception as e:
            self._catchTradeException(e,"enter long")
        
    def Sell(self, open_time):
        logging.info("OpenS order at " + str(open_time) +", Price "+ str(self._logging_current_price))
        #print("OpenS order at " + str(open_time) +", Price "+ str(self._logging_current_price))
        self._oanda._OrderOpenTime = open_time

        if not self.trading_enabled:
            logging.info("Trading disabled, doing nothing")
            return

        # Enter the short position on the instrument
        units = self._risk.GetShortPositionSize()
        if units > 2000: #Ryan: max allowed unit is 2000
            units=2000
        logging.info("Got the number of units to trade from RiskManager: "+str(units))
        if units == 0:
            logging.info("Cant trade 0 units, doing nothing")
            return

        try:
            self._oanda.Sell(units)
        except Exception as e:
            self._catchTradeException(e,"enter short")

    def ClosePosition(self):

        logging.info("Closing position, and all stops")
        if not self.trading_enabled:
            logging.info("Trading disabled, doing nothing")
            return

        try:
            self._oanda.ClosePosition()
        except Exception as e:
            self._catchTradeException(e,"close")

    def GetStopLossPrice(self):
        return 0.0

    def GetTrailingStopPrice(self):
        return 0.0

    def _catchTradeException(self, e, position):
            logging.critical("Failed to "+position+" position")
            logging.critical(traceback.format_exc())
            if self._email:
                txt = "\n\nError while trying to "+position+" position\n"
                txt += "It was caught, I should still be running\n\n"
                txt += traceback.format_exc()+"\n"+str(e)
                #self._email.Send(txt) #RYAN: disable email
