__author__ = 'mingluma'

import os


class Splitter(object):
    def __init__(self, file_basename, chunk_size, file_type):
        self.chunk_size = chunk_size
        self.file_type = file_type
        self.input_file = [f for f in os.listdir('.') if f.startswith(file_basename)]


    def split(self):
        """
        Split files into splits, each split has multiple chunks.
        :return: An array of split information, i.e., (file, start, end)
        """
        overall_chunks = []
        for file in self.input_file:
            file_chunks = self.split_single_file(file)
            overall_chunks.extend(file_chunks)
        return overall_chunks

    def hamming_split(self):
        """
        :return:
        """
        if self.chunk_size % 2 == 1:
            self.chunk_size += 1
        overall_chunks = []
        for file in self.input_file:
            file_size = os.path.getsize(file)
            pos = 0
            while pos < file_size:
                next_pos = min(pos + self.chunk_size, file_size)
                overall_chunks.append((file, pos, next_pos))
                pos = next_pos

        num_chunks = len(overall_chunks)
        overall_chunks_ext = []
        for index, (file, start, end) in enumerate(overall_chunks):
            overall_chunks_ext.append((index, num_chunks, file, start, end))

        return overall_chunks_ext

    def split_single_file(self, file):
        """
        Split a single file into parts in chunk size
        :return: An array of split position of the chunks, i.e., (file, start, end).
        """
        file_size = os.path.getsize(file)
        file_handler = open(file, "r")
        chunks = []
        pos = 0
        while pos < file_size:
            next_pos = min(pos + self.chunk_size, file_size)
            if pos == 0:
                chunks.append((file, pos, self.find_next_newline(file_handler, next_pos)))
            else:
                chunks.append((file, self.find_next_newline(file_handler, pos), self.find_next_newline(file_handler, next_pos)))
            pos = next_pos
        file_handler.close()
        return chunks

    def find_next_newline(self, file_handler, pos):
        file_handler.seek(pos)
        line = file_handler.readline()
        return pos + len(line)

if __name__ == "__main__":
    splitter = Splitter("england.txt", 10240, "b")
    print splitter.split()

"""
    def get_all_files(self):
        if os.path.isdir(self.path):
            return os.listdir(self.path)
        else:
            return [self.path]
"""