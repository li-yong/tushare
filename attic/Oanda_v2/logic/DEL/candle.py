# Candle sticks
import datetime
import time
import logging
import sys

from logic import Indicator, ValidateDatapoint

class Candle(Indicator):
    
    # Opening price
    Open = 0.0
    
    # Closing price
    Close = 0.0
    
    # Highest price
    High = 0.0
    
    # Lowest price
    Low = 0.0
    
    # Open timestamp
    OpenTime = datetime.datetime.fromtimestamp(time.time())

    # Close timestamp
    CloseTime = OpenTime;

    def __init__(self, openTime, closeTime):
        if (isinstance(openTime, datetime.datetime)):
            self.OpenTime = openTime
        if (isinstance(closeTime, datetime.datetime)):
            self.CloseTime = closeTime
        
        self._is_closed = False

    # Returns true if candle stick accumulated enough data to represent the 
    # time span between Opening and Closing timestamps
    def SeenEnoughData(self):
        return self._is_closed

    def AmounOfDataStillMissing(self):
        if (self.SeenEnoughData()):
            return 0
        return 1
            
    def Update(self, data):

        if ( self.CloseTime < self.OpenTime ):
            self._resetPrice(0.0)
            self._is_closed = False
            logging.info("SHOULD NOT BE HERE! CANDLE CLOSETIME < OPENTIME")
            return

        if (not ValidateDatapoint(data)):
            logging.info("CANDAL NOT PASS VALIDATE")
            return

        if(self._is_closed ==True):
            print("SHOULD NOT BE HERE, Candle is being update while candle was closed")
            #sys.exit(1)



        _current_timestamp = data["now"]
        _price = data["value"]

        logging.info("candle start:"+str(self.OpenTime)+ ", candle close:"+str(self.CloseTime)+", ticker current:"+str(_current_timestamp))


        if( (_current_timestamp <= self.CloseTime and _current_timestamp >= self.OpenTime) or (_current_timestamp > self.CloseTime )):
            msg="Current time between closeTime "+str(self.CloseTime)+"AND Start time "+str(self.OpenTime)+" "+str(_current_timestamp)+", candle updateData."
            #print(msg)
            #logging.info(msg)

            #ryan: backtest we know the holc
            if (('h' in data) and ('l' in data) and ('o' in data)  and ('c' in data)):
                self.High=data['h']
                self.Low=data['l']
                self.Open=data['o']
                self.Close=data['c']
                #self._is_closed=True

            self._updateData(_price)

        if (_current_timestamp >= self.CloseTime):
            # logging.info("current_time "+str(_current_timestamp)+" > closeTime"+str(self.CloseTime)+", candle closed.")
            self._is_closed = True
            logging.info("candle closed."+" "+str(self.OpenTime)+", "+str(self.CloseTime)+". O:" +str(self.Open)+". H: "+str(self.High)+". L: "+str(self.Low)+". C: "+str(self.Close))


    def _resetPrice(self, price):
        self.High = price
        self.Low = price
        self.Open = price
        self.Close = price
       
    # Update the running timestamps of the data    
    def _updateData(self, price):
        # If this is the first datapoint, initialize the values
        if ( self.High == 0.0 and self.Low == 0.0 and self.Open == 0.0 and self.Close == 0.0):
            self._resetPrice(price)
            self._is_closed = False
            return
        
        # Update the values in case the current datapoint is a current High/Low
        self.Close = price
        self.High = max(price,self.High)
        self.Low = min(price,self.Low)

        #self.bar_length_hl=self.High - self.Low  # compute in stragegy.py

        tmp=self.Close - self.Open
        if tmp < 0:
            self.Trend="short"
        else:
            self.Trend="long"

        self.bar_length_oc = abs(tmp)
        logging.info("Updating candle as: Open "+str(self.Open)+" Close "+str(self.Close)+" High "+str(self.High)+" Low "+str(self.Low))

