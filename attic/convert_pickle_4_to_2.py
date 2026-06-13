#This script run on python3
#convert pickle format from protocl 4 to 2

import pandas
import os

from optparse import OptionParser

parser = OptionParser()

parser.add_option("-p", "--pickle_file", dest="pickle_file", type="str", help="pickle file to be converted from protcol 4 to 2")

(options, args) = parser.parse_args()

pickle_file = options.pickle_file

print("load pick file in format 4 " + pickle_file)

#fh = open(pickle_file,"rb")
#obj = pickle.load(fh)

obj = pandas.read_pickle(pickle_file)

print("dump to " + pickle_file + ".2")

#pickle.dump(obj, open(pickle_file+".2","wb"), protocol=2) #from 4 to 2
obj.to_pickle(pickle_file + ".2", protocol=2)

os.rename(pickle_file, pickle_file + ".DEL")
os.rename(pickle_file + ".2", pickle_file)
