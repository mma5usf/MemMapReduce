__author__ = 'mingluma'

import sys

#import mapreduce_class as mapreduce
import mapreduce


def delete_hamming(hamming_str):
    # Return original code without hamming
    return hamming_str[2] + hamming_str[4:7] + hamming_str[8:12]

def parity(s, indicies):
    # Compute the parity bit for the given string s and indicies
    sub = ""
    for i in indicies:
        sub += s[i]
    return str(str.count(sub, "1") % 2)


def get_ascii(byte_str):
    # Get ASCII character from a binary string.
    return chr(int(byte_str, 2))


class DecodeMap(mapreduce.Map):

    def map(self, k, code):
        """
        code is a 12 character long string.
        """
        if len(code) != 12:
            return
        # return decoded text as a string.
        byte_str = delete_hamming(code)
        decoded = get_ascii(byte_str)
        self.emit(k, decoded)


class DecodeReduce(mapreduce.Reduce):

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
    mapper = DecodeMap()

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
    reducer = DecodeReduce()
    # Sort intermediate keys
    keys = combine_table.keys()
    keys.sort()
    for k in keys:
        reducer.reduce(k, combine_table[k])
    result_dic = reducer.get_result()
    return ''.join(result_dic.values())


def test_import():
    print "code import success"


