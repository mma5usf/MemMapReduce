import pickle
import os
import sys

class Collector(object):
    def __init__(self, basename, outputfile):
    	all_files = os.listdir('.') #get all the file name in the current directory
        self.input_file = [ f for f in all_files if f.startswith(basename)]
        self.outputfile = outputfile
        self.result = []

    def collect(self):
        combine_result = {}
        for f in self.input_file:
    	    file_handler = open(f, "r")
            combine_result.update(pickle.load(file_handler))
            file_handler.close()
        
        keys = combine_result.keys()
        keys.sort()

        for key in keys:
        	self.result.append(key + ':' + str(combine_result[key]))

        f = open(self.outputfile, "w")
        f.write("\n".join(self.result))
        f.close

    def merge(self):
        combine_result = []
        for f in self.input_file:
            file_handler = open(f, "r")
            combine_result.append(pickle.load(file_handler))
            file_handler.close()

        f = open(self.outputfile, "w")
        f.write("".join(combine_result))
        f.close

if __name__ == "__main__":
    collector = Collector(sys.argv[1], sys.argv[2])
    if sys.argv[3] == "collect":
        collector.collect()
    else:
        collector.merge()

