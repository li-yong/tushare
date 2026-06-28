from logic.candle import Candle
from logic import Indicator
from logic.heikinashi import HeikinAshi
import logging
import pprint

# Moving averages indicators

def GetDataPointValue(datapoint):
    result = {}

    if not datapoint:
        return None

    if (isinstance(datapoint, Candle)):
        result["value"] = float(datapoint.Close)
        result["bar_length"] = abs(float(datapoint.High - datapoint.Low))
        return result

    try:
        #if datapoint.has_key("value"):
        if "value" in datapoint.keys():
            result["value"] = float(datapoint["value"])
            result["bar_length"] = float(abs(datapoint["h"] - datapoint["l"]))
            return result
    except:
        return None

    return None


# Base class for real indicators (SimpleMovingAverage, ExponentialMovingAverage)
class pattern_hit(Indicator):

    def __init__(self, period_count = 30):
        print("woo! entering my indicator")
        self.value = 0.0
        self.bar_length_avg=0.0 #the average bar length
        self.period_count = period_count
        self._data = []

    def DataPointsCount(self):
        return len(self._data)

    def AmountOfDataStillMissing(self):
        return max(0, self.period_count - len(self._data))

    def SeenEnoughData(self):
        return ( len(self._data) >= self.period_count )

    def Update(self, d):

        data = GetDataPointValue(d)

        if (not data):
            return


        if ( len(self._data) >= self.period_count ):
            logging.info("MA see enought data "+str(len(self._data)) +", expected:"+str(self.period_count))
            #pprint.pprint(self._data)

            for d in self._data:
                logging.info("Data Bar_length:"+str(d["bar_length"]))
                logging.info("Data Value:"+str(d["value"]))

            # Update current moving average, avoiding the loop over all datapoints
            self.value = self.value + ( data["value"] - self._data[0]["value"] ) / len(self._data)

            if (len(self._data)-1) == 0:
                self.bar_length_avg=0
            else:
                s=0
                for d in self._data:
                    s = s+d['bar_length']

                self.bar_length_avg =s/(len(self._data))
                msg="Average Bar_Length:"+str(self.bar_length_avg)
                #print(str(len(self._data)))
                logging.info(msg)
                #pprint.pprint(msg)
                # Remove outdated datapoint from the storage
                self._data.pop(0)
        else:
            # Not enough data accumulated. Compute cumulative moving average
            logging.info("MA Not see enough data, "+str(len(self._data)) +", expected:"+str(self.period_count))
            #pprint.pprint(self._data)

            #self.value = self.value + (data["value"] - self.value) / ( len(self._data) + 1.0 )
            self.value = (self.value + (data["value"] - self.value)) / ( len(self._data) + 1.0 )
            self.bar_length_avg = (self.bar_length_avg * (len(self._data)) + data["bar_length"] ) / (len(self._data)+1)
            msg = "Average Bar_Length:" + str(self.bar_length_avg)
            logging.info(msg)
            #pprint.pprint(msg)

        self._data.append(data)

        # Add the data point to the storage