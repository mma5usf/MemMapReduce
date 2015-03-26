import sys
import gevent
import zerorpc

class Client(object):
    def __init__(self, addr, code_file, split_size, num, input_file, output_file, file_type):
        self.mr_job = {}
        '''filename is the file of the mapreduce code'''
        self.mr_job["master_addr"] = addr
        self.mr_job["file_type"] = file_type # "b": binary, "S" :text file
        self.mr_job["split_size"] = split_size
        self.mr_job["reducer_num"] = num
        self.mr_job["input_file"] = input_file
        self.mr_job["output_file"] = output_file

        with open(code_file, 'r') as mapreduce_file:
            self.code = mapreduce_file.read()       
    def get_code(self):
        return self.code

    def get_mr_job(self):
        return self.mr_job

if __name__ == '__main__':
    
    addr = sys.argv[1]
    code_file = sys.argv[2]
    split_size = int(sys.argv[3])
    num = int(sys.argv[4])
    input_file = sys.argv[5]
    output_file = sys.argv[6]
    file_type = sys.argv[7]

    client = Client(addr, code_file, split_size, num, input_file, output_file, file_type)

    c = zerorpc.Client()
    c.connect('tcp://' + addr)
    mr_code = client.get_code()
    #print mr_code
    mr_job = client.get_mr_job()
    #print mr_job
    c.mr_job(str(mr_code), mr_job)








