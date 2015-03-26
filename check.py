__author__ = 'mingluma'

import sys
import mapreduce


def parity(s, indicies):
    # Compute the parity bit for the given string s and indicies
    sub = ""
    for i in indicies:
        sub += s[i]
    return str(str.count(sub, "1") % 2)

def get_ascii(byte_str):
    # Get ASCII character from a binary string.
    return chr(int(byte_str, 2))


def get_bytestr(char):
    return "{0:08b}".format(ord(char))


class CheckMap(mapreduce.Map):
    def map(self, k, hamming_str):
        position = 0
        t1 = parity(hamming_str, [2,4,6,8,10])
        t2 = parity(hamming_str, [2,5,6,9,10])
        t4 = parity(hamming_str, [4,5,6,11])
        t8 = parity(hamming_str, [8,9,10,11])
        if t1 != hamming_str[0]:
            position += 1
        if t2 != hamming_str[1]:
            position += 2
        if t4 != hamming_str[3]:
            position += 4
        if t8 != hamming_str[7]:
            position += 8
        if position != 0:
            self.emit("err position " + str(position - 1), "1")
            self.emit("total errors ", "1")
        else:
            self.emit("correct hamming code", "1")


class CheckReduce(mapreduce.Reduce):

    def reduce(self, k, value_list):
        count = 0
        for v in value_list:
            count = count + int(v)
        self.emit(k, count)


def map_engine(split, reducer_num):
    """
    :param split: Hamming split structure: (chunk_index, [file, start, end])
    :return: map_result = { 1:{chunk_index: chunk}
                            2:{}
                            ...
                           }
    """
    print "Call map engine for", split
    mapper = CheckMap()

    chunk_index, num_chunks, file, start, end = split
    f = open(file, "r")
    f.seek(start)
    code = ""
    for c in f.read(end - start):
        code += c
        if len(code) == 12:
            # Get hamming code
            mapper.map(chunk_index, code)
            code = ""
    f.close()

    # Map_result[i] is the result for reducer i
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
    :param assigned_tables: (chunk_index1, value), (chunk_index2, value)
    :return:
    """
    # Each chunk is assigned to only one mapper, no merge needed, thus use
    # dict.update to combine tables.
    for k, value in assigned_table.iteritems():
        if k in combine_table:
            combine_table[k].append(value)
        else:
            combine_table[k] = [value]


def reduce_engine(combine_table):
    reducer = CheckReduce()
    # Sort intermediate keys
    keys = combine_table.keys()
    keys.sort()
    for k in keys:
        reducer.reduce(k, combine_table[k])
    result_dic = reducer.get_result()
    return result_dic


def test_import():
    print "code import success"

