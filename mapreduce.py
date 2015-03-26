# mapreduce.py

class Map(object):

    def __init__(self):
        self.table = {}

    def map(self, k, v):
        pass

    def emit(self, k, v):
        if k in self.table:
            self.table[k].append(v)
        else:
            self.table[k] = [v]

    def get_table(self):
        return self.table


class Reduce(object):

    def __init__(self):
        self.result_dic = {}

    def reduce(self, k, vlist):
       pass

    def emit(self, k, v):
        self.result_dic[k] = v

    def get_result(self):
        return self.result_dic


class Engine(object):

    def __init__(self, input_list, map_class, reduce_class):
        self.input_list = input_list
        self.map_class = map_class
        self.reduce_class = reduce_class
        self.result_list = None

    def execute(self):

        mapper = self.map_class()
        # Map phase
        for i, v in enumerate(self.input_list):
            mapper.map(i, v)

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        keys.sort()

        # Reduce phase
        reducer = self.reduce_class()
        for k in keys:
            reducer.reduce(k, table[k])
        self.result_list = reducer.get_result_list()

    def get_result_list(self):
        return self.result_list


