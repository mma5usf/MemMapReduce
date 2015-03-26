# worker.py - Master used for MapReduce
#
# To run this code you need to install ZeroMQ, Gevent, and ZeroRPC
#
# http://zeromq.org
# http://www.gevent.org
# http://zerorpc.dotcloud.com
# 
# These will require some additional libraries
# In order to run an election you need to create a config file:
# 
# $python worker.py 127.0.0.1:9000
# 
# You can kill (CTRL-C) a server to see the election run again.
# This file was change from bully.py

import sys
import imp
import gevent
import zerorpc
import pickle
import signal
import logging

from gevent.event import Event

class Worker(object):

    def __init__(self, addr, config_file='config'):
        self.addr = addr
        self.finished_job = []
        self.combine_table = {}

        


    def connect_master(self, master_addr):
        ####################################################################
        #
        #We will try to add some ssh code to start the workers automatically
        #
        ####################################################################
        self.client = zerorpc.Client()
        self.client.connect('tcp://' + master_addr)

        ###########################################
        #self.pool.spawn(client_task).join()
        #Access client task
        ###########################################

    def connect_mapper(self, reducer_index, mapper_addr):
        '''
        mapper_addr = [(index, ip), (index, ip)]
        ''' 
        self.finished_job = []
        self.reducer_index = reducer_index
        self.mapper_clients = {}
        self.combine_table = {}
        for i, server in mapper_addr:
            c = zerorpc.Client(timeout=1)
            c.connect('tcp://' + server)
            self.mapper_clients[i] = c



    def get_job(self, worker_job, index):
        '''Get the MapReduce job informantion'''
        self.index = index
        self.worker_job = worker_job
        self.mr_module = self.importCode("client_job")
        self.mr_module.test_import()
        self.connect_master(worker_job["master_addr"])


    def map(self, split, task_id):
        logging.debug('Start mapping process for %s', split)
        self.map_result = self.mr_module.map_engine(split, self.worker_job["reducer_num"])
        #Hack for debugging
        #if self.addr == "10.0.0.216:23000":
        #    sys.exit(1)
        #After the map job was done tell master
        try:
            logging.debug('Mapper %s call reducer for %s', self.addr, split)
            self.client.call_reducer(self.index, task_id)
        except:
            print "Can't talk to master"

    def collect(self, current_task, mapper_index):
        logging.debug("worker collect 1: %s, %s, %s, %s", current_task, mapper_index, self.finished_job, self.reducer_index)
        if current_task not in self.finished_job:
            try:
                self.finished_job.append(current_task)
                logging.debug("finished job: %s", self.finished_job)
                assign_table = self.mapper_clients[mapper_index].get_map_result(self.reducer_index)
                logging.debug("assign table: %s", len(assign_table))
                self.mr_module.collect_engine(assign_table, self.combine_table)
                logging.debug('Reducer%d get the mapping result for %s', self.reducer_index, current_task)
                #Hack for debugging
                #if self.addr == "10.0.0.215:22001":
                #    sys.exit(1)
            except zerorpc.TimeoutExpired:
                self.finished_job.pop()
                logging.debug('Failed to collect %s',  current_task)
                raise MapperFailed("Mapper failed") #mapper failed

    def reduce(self):

        logging.debug('Reduce process in Reducer%d start...', self.reducer_index)
        result = self.mr_module.reduce_engine(self.combine_table)

        #********Output the reducer result to the file*********
        output_file = self.worker_job["output_file"] + "_reducer" + str(self.reducer_index)
        f = open(output_file, "w")
        pickle.dump(result, f)
        f.close()
        print "reducer job finished :)"



    def get_map_result(self, reducer_index):
        '''
        Partition the map result using hash function
        return the split to the reducer
        '''
        logging.debug('Reducer%d gets map result from %s', reducer_index, self.addr)
        logging.debug("mapresult: %s", len(self.map_result[reducer_index]))
        return self.map_result[reducer_index]


    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr
        return ans

    def importCode(self, name):
        '''
        Reference: http://code.activestate.com/recipes/82234-importing-a-dynamically-generated-module/
        '''
        logging.debug('Code ship to %s successfully', self.addr)
        module = imp.new_module(name)
        exec self.worker_job["code"] in module.__dict__
        return module

class MapperFailed(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ReducerFailed(Exception):
    def __init__(self, value, index):
        self.value = value
        self.index = index
    def __str__(self):
        return repr(self.value)
    def get_index(self):
        return index


if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig()
    addr = sys.argv[1]
    worker = Worker(addr)
    logging.debug('Worker %s start working', worker)
    s = zerorpc.Server(worker)
    s.bind('tcp://' + addr)
    gevent.signal(signal.SIGTERM, s.stop)
    # Start server
    s.run()
