# master.py - Master used for MapReduce
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
# $ cat config
# 127.0.0.1:9000
# 127.0.0.1:9001
# 127.0.0.1:9002
#
# You can kill (CTRL-C) a server to see the election run again.
# This file was change from bully.py


import sys
import gevent
import signal
import zerorpc
import logging
import time
from sets import Set
from splitter import Splitter
from gevent.queue import Queue
from gevent.event import Event
from worker import MapperFailed
from worker import ReducerFailed
from gevent.coros import BoundedSemaphore

class Master(object):

    def __init__(self, addr, config_file='config'):
        self.addr = addr

        # The index of alive workers
        self.workers = []

        # the worker server list from config file
        self.servers = []

        # the connection list of the workers
        self.connections = []
        self.running_tasks = {} #the tasks each workers are working on 
                                #(if it failed we will insert the task into the task list again)

        self.map_tasks = Queue()
        self.evt = Event()
        self.evt.set()
        self.sem = BoundedSemaphore(1)
        self.reduce_done = False
        self.task_id = 0

        f = open(config_file, 'r')
        self.servers = [l.rstrip() for l in f.readlines() if l.rstrip() != addr]
        print 'My addr: %s' % (self.addr)
        print 'Server list: %s' % (str(self.servers))

        # number of worker servers
        self.n = len(self.servers)

        for i, server in enumerate(self.servers):
            c = zerorpc.Client(timeout=1)
            c.connect('tcp://' + server)
            self.connections.append(c)
            #print "connetions: " + server

    def start(self):
        ####################################################################
        #
        #We will try to add some ssh code to start the workers automatically
        #
        ####################################################################
      
        #start heartbeat
        ping = gevent.spawn(self._ping).join()


    def start_mr_job(self):
        self.task_id += 1
        file_splitter = Splitter(self.mr_job["input_file"], self.mr_job["split_size"], self.mr_job["file_type"])
        if self.mr_job["file_type"] == "binary":
            self.split_result = file_splitter.hamming_split()
        else:
            self.split_result = file_splitter.split()

        #record the left task
        self.tasks = Set(self.split_result)
        self.reduce_done = False
        #MapReduce counter
        #print self.split_result
        for i in self.split_result:
            self.map_tasks.put_nowait(i) #put the task into the Queue

        threads = [gevent.spawn(self.connect_mapper, i, self.mapper_addr) for i in self.reducer_workers]
        gevent.joinall(threads)

        #Assign the map tasks
        self.mapper_threads = [gevent.spawn(self.map, i, self.task_id) for i in self.mappers]
        gevent.joinall(self.mapper_threads)

    def setup(self):
        '''After the set up process, start the working process'''
        self._get_workers()
        if len(self.workers) == 0:
            print "No workers running. Please check your servers!!!"
            sys.exit()

        print "Setup process succeed  :-)"
        print "Worker list:"
        for i in self.workers:
            print self.servers[i]
        print "Now you can run your MapReduce jobs."

    def _get_workers(self):
        '''Get all alive workers'''
        self.workers = []

        logging.debug('Start getting workers ')
        for j in set(range(self.n)):
            try:
                ans = self.connections[j].are_you_there()
                if not ans:
                    continue
                self.workers.append(j)
            except zerorpc.TimeoutExpired:
                continue
        logging.debug("Alive workers: %s", tuple(self.workers))

    def _ping(self):
        '''Called periodically, check the status of the workers'''
        while True:
            gevent.sleep(1)
            
            for i in self.workers:
                try:
                    ans = self.connections[i].are_you_there()
                    #print "Get response from: " + ans["addr"]
                except:
                    logging.debug('Timeout: %s', self.servers[i])
                    continue

    def map(self, worker_index, task_id):
        '''
        worker_index: index of the worker
        '''
        while not self.map_tasks.empty():
            self.evt.wait()
            if task_id < self.task_id:
                return
            task = self.map_tasks.get()

            #Backup tasks
            if self.map_tasks.empty() and len(self.tasks) != 0:
                for task in self.tasks:
                    self.map_tasks.put_nowait(task)

            while task not in self.tasks:
                if not self.map_tasks.empty():
                    task = self.map_tasks.get()
                elif len(self.tasks) != 0:
                    for task in self.tasks:
                        self.map_tasks.put_nowait(task)
                else:
                    return

            logging.debug('Worker %s got task %s', worker_index, task)
            self.running_tasks[worker_index] = task
            #Call the map engine in the worker
            try:
                self.connections[worker_index].map(task, self.task_id)
            except zerorpc.TimeoutExpired:
                #handle mapper failure
                self.map_tasks.put_nowait(task)
                self.evt = Event()
                self.workers = [j for j in self.workers if j != worker_index]
                self.mappers = [j for j in self.mappers if j != worker_index]
                self.running_tasks.pop(worker_index, None)
                self.evt.set()
            gevent.sleep(0)
        logging.debug('Worker %d quit successfully :)', worker_index)


    def connect_mapper(self, reducer_worker_index, mapper_addr):
        try:
            self.connections[reducer_worker_index].connect_mapper(self.reducer_index[reducer_worker_index], mapper_addr)
        except zerorpc.TimeoutExpired:
            logging.warning("!!!!!! Reducer%d failed", self.reducer_index[reducer_worker_index])

    def call_reducer(self, mapper_index, task_id):
        threads = [gevent.spawn(self.collect, i, mapper_index, task_id) for i in self.reducer_workers]
        gevent.joinall(threads)
        for thread in threads:
            if not thread.successful():
                #we should add the task back if we want to handle the network error
                #if mapper_index in self.running_tasks.keys()
                #    self.map_tasks.put_nowait(self.running_tasks[mapper_index]) 
                #print "$$$$$$$$$$failed in ", thread.successful()
                return

        if mapper_index in self.running_tasks.keys() and task_id == self.task_id:
            if self.running_tasks[mapper_index] in self.tasks:
                logging.debug("Task %s has been finished.", self.running_tasks[mapper_index])
                self.tasks.remove(self.running_tasks[mapper_index])
            

        if len(self.tasks) == 0 and self.reduce_done == False:
            self.reduce_done = True
            logging.debug("[------colleciton finished------]")
            threads = [gevent.spawn(self.reduce, i, task_id) for i in self.reducer_workers]
            gevent.joinall(threads)
            print "Task finished successfully"
        return
    
    def collect(self, reducer_worker_index, mapper_index, task_id):
        self.evt.wait()
        try:
            if task_id == self.task_id:
                self.connections[reducer_worker_index].collect(self.running_tasks[mapper_index], mapper_index)
        except MapperFailed as e:
            #handle mapper failed
            logging.warning("!!!!!! Mapper%d failed", mapper_index)
            raise Exception("Mapper failed")
        except zerorpc.TimeoutExpired:
            #reducer failed
            logging.warning("!!!!!! Reducer%d failed", self.reducer_index[reducer_worker_index])
            if task_id == self.task_id:
                self._fail_recover()
            else:
                return
        except Exception, e:
            return

    def reduce(self, reducer_worker_index, task_id):
        self.evt.wait()
        logging.debug("Reducer%d starting reduce task", reducer_worker_index)
        try:
            if task_id == self.task_id:
                self.connections[reducer_worker_index].reduce()
        except zerorpc.TimeoutExpired:
            #reducer failed
            logging.warning("!!!!!! Reducer%d failed", self.reducer_index[reducer_worker_index])
            if self.task_id == task_id:
                self._fail_recover()
            else:
                return
        except Exception, e:
            return

    def _fail_recover(self):
        self.task_id += 1
        self.evt = Event()
        self.mapper_num = len(self.mapper_threads)
        print "Task Restart !!!!"
        self.setup()
        self._allocate_roles()
        for i in self.split_result:
            self.map_tasks.put_nowait(i) #put the task into the Queue
        self.tasks = Set(self.split_result)
        self.reduce_done = False
        threads = [gevent.spawn(self.connect_mapper, i, self.mapper_addr) for i in self.reducer_workers]
        gevent.joinall(threads)
        self.evt.set()

        self.mapper_threads = [gevent.spawn(self.map, i, self.task_id) for i in self.mappers]
        gevent.joinall(self.mapper_threads)

    def _deploy_code(self):
        threads = [gevent.spawn(self._pass_code, i) for i in self.workers]
        gevent.joinall(threads)


    def _pass_code(self, worker_index):
        '''Deploy mapreduce jobs to workers'''
        try:
            self.connections[worker_index].get_job(self.worker_job, worker_index)
        except zerorpc.TimeoutExpired:
            logging.debug("Remove %s because of server failure", self.servers[i])
            #Remove the failed worker
            self.workers = [i for i in self.workers if i != worker_index]

    def _allocate_roles(self):
        self.mappers = [self.workers[i] for i in range(len(self.workers)) if i >= self.mr_job["reducer_num"]] #mapper indexes
        self.mapper_addr = [ (i, self.servers[i]) for i in self.mappers]
        self.reducer_workers = [self.workers[i] for i in range(len(self.workers)) if i < self.mr_job["reducer_num"]] #reducer indexes

        # reducer_index is a map from reducer_worker_index to reducer_index
        self.reducer_index = {}
        for i, j in enumerate(self.reducer_workers):
            self.reducer_index[j] = i
        logging.debug("Mappers: %s", tuple(self.mappers))
        logging.debug("Reducer_workers: %s", tuple(self.reducer_workers))

    def mr_job(self, mr_code, job_info):
        '''
        Get the MapReduce job informations from client
        '''
        self.mr_job = job_info
        self.mr_job["code"] = mr_code
        self.worker_job = {}
        self.worker_job["code"] = mr_code
        self.worker_job["master_addr"] = job_info["master_addr"] 
        self.worker_job["reducer_num"] = job_info["reducer_num"]
        self.worker_job["output_file"] = job_info["output_file"]
        self._deploy_code()
        self._allocate_roles()
        start_time = time.time()
        self.start_mr_job()
        end_time = time.time()
        print "execution time: ", end_time - start_time
        

    def are_you_there(self):
        ans = {}
        ans["addr"] = self.addr
        return ans

if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig()
    addr = sys.argv[1]
    master = Master(addr)
    logging.debug("Master %s start working", addr)
    s = zerorpc.Server(master)
    s.bind('tcp://' + addr)
    gevent.signal(signal.SIGTERM, s.stop)
    # Start server
    gevent.Greenlet.spawn(s.run)
    master.setup()
    master.start()
