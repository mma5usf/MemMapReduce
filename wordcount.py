import sys

#import mapreduce_class as mapreduce
import mapreduce

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k, count)


def map_engine(split, reducer_num):
    """
    :param assigned_chunks: (file, start, end) Master assign the chunks for each worker
    :return: map_result = { 1 :{word1: count1, word3: count3, ...},
                            2 :{word2: count2, word4: count4, ...} 
                            ...}
                          { 1:{chunk_idex: chunk}
                            2:{}
                           }
    """
    #MapReduce counter
    #print "Call map engine for", split
    mapper = WordCountMap()
    f = open(split[0], "r")
    f.seek(split[1])
    mapper.map(1, f.read(split[2] - split[1]))
    f.close()

    #map_result[i] is the result for reducer i
    map_result = {}
    for i in range(reducer_num):
        map_result[i] = {}

    # Sort intermediate keys
    table = mapper.get_table()
    keys = table.keys()
    keys.sort()

    # partition the result and combine the map result
    for key in keys:
        reducer_index = hash(key) % reducer_num
        map_result[reducer_index][key] = len(table[key])

    return map_result
 

def collect_engine(assigned_table, combine_table):
    """
    :param assigned_tables: (key, list of value)
    :return: list of (key, value)
    """
    ###############################################
    #Get map result from all the mappers
    #Combine the map result together into one table
    #We need this table
    # input: assigned_tables, a list of mapper result tables, each table is mapper key/values pairs.
    # From mapper1:
    #   k1 -> v1
    #   k2 -> v2
    # From mapper2:
    #   k1 -> v3
    #   k3 -> v4
    # The reducer will group keys from the same table and generate
    # non duplicate key/values pairs, i.e.,
    #   k1 -> [v1, v2, v3, v6]
    #   k2 -> [v4, v5]
    #   k3 -> [v7, v8]
    # Each reducer will consume each key/values pair and generate final result.
    ################################################  
    for k, value in assigned_table.iteritems():
        if k in combine_table:
            combine_table[k].append(value)
        else:
            combine_table[k] = [value]



def reduce_engine(combine_table):
    reducer = WordCountReduce()
    # Sort intermediate keys
    keys = combine_table.keys()
    keys.sort()
    for k in keys:
        reducer.reduce(k, combine_table[k])
    result_dic = reducer.get_result()
    return result_dic

def test_import():
    print "code import success"



        



