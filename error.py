__author__ = 'mingluma'

import sys
import mapreduce
from random import randint


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


class ErrorMap(mapreduce.Map):
    def map(self, k, hamming_str):
        err = randint(1, 100)
        erred_code = list(hamming_str)
        if len(hamming_str) > 12:
            print "hamming_str: ", hamming_str, len(hamming_str)
        if err < 50:
            posi = randint(0, 11)
            erred_code[posi] = '0' if hamming_str[posi] == '1' else '1'
        #err_hamming_str = "".join(erred_code)
        #self.check_hamming(hamming_str, err_hamming_str)

        self.emit(k, "".join(erred_code))

    def check_hamming(self, old_hamming_str, hamming_str):
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
        if position > 12:
            print "err hamming: ", old_hamming_str, hamming_str, len(hamming_str), t1, hamming_str[0], t2, hamming_str[1], t4, hamming_str[3], t8, hamming_str[7]


class ErrorReduce(mapreduce.Reduce):

    def reduce(self, k, value_list):
        self.emit(k, ''.join(value_list))


def map_engine(split, reducer_num):
    """
    :param split: Hamming split structure: (chunk_index, [file, start, end])
    :return: map_result = { 1:{chunk_index: chunk}
                            2:{}
                            ...
                           }
    """
    print "Call map engine for", split
    mapper = ErrorMap()

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

    # We don't need sort in this case. table is {chunk_index: hamming1, hamming2, hamming3...}
    table = mapper.get_table()

    # Partition the result and combine the map result
    chunks_per_reducer = (num_chunks + reducer_num - 1) / reducer_num
    reducer_index = chunk_index / chunks_per_reducer
    map_result[reducer_index][chunk_index] = table[chunk_index]
    return map_result

def collect_engine(assigned_table, combine_table):
    """
    :param assigned_tables: (chunk_index1, value), (chunk_index2, value)
    :return:
    """
    # Each chunk is assigned to only one mapper, no merge needed, thus use
    # dict.update to combine tables.
    combine_table.update(assigned_table)


def reduce_engine(combine_table):
    reducer = ErrorReduce()
    # Sort intermediate keys
    keys = combine_table.keys()
    keys.sort()
    for k in keys:
        reducer.reduce(k, combine_table[k])
    result_dic = reducer.get_result()
    return ''.join(result_dic.values())


def test_import():
    print "code import success"

