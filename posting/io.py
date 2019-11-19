from itertools import zip_longest

from typing import Optional, Type, IO
from .post import Posting
from collections import defaultdict
from threading import RLock

TOKENS = {'\n', '\t', '\v', '\f'}


class PostingWriter:
    """
    PostingWriter enables you to write postings to a file and keep track of their indexes. Do not write to the file
    the same key multiple times.
    """

    def __init__(self, file_name: str):
        """
        Initialize the writer pointed at the file name. This will be opened with w+ permissions so ensure that the file
        is unique or disposable.
        :param file_name: the base name of the file.
        """
        self.open = open(f"{file_name}.index", 'w+')
        self.index = open(f"{file_name}.index.position", 'w+')
        self.keys = dict()

    def write_key(self, key: str):
        """
        Write a key at the current position.
        :param key:
        :return:
        """
        self.keys[key] = self.open.tell()
        if self.keys[key] == 0:
            self.open.write(f"{key}")
        else:
            self.open.write(f"\n{key}")

    def write_posting(self, posting: Posting):
        self.open.write(f"\f{str(posting)}")

    def write(self, *postings):
        for posting in postings:
            self.write_posting(posting)

    def flush(self):
        self.open.flush()

    def close(self):
        for key in sorted(self.keys):
            self.index.write(f"{key}\t{self.keys[key]}\n")
        self.keys.clear()
        self.index.close()
        self.open.close()


class PostingIterator:
    def __init__(self, lock: RLock, file: IO, posting: Type[Posting], init_buffer=None):
        self.lock = lock
        self.file = file
        self.posting = posting
        self.buffer = "" if not init_buffer else init_buffer
        self.end = False if not init_buffer else init_buffer.endswith('\n')

    def __enter__(self):
        self.lock.acquire()
        return self

    def __iter__(self):
        return self

    def __next__(self) -> Posting:
        buffer = self.buffer
        while "\f" not in buffer and not self.end:
            buffer = self.file.readline(64000)
            if buffer.endswith('\n') or not buffer:
                self.end = True
            self.buffer += buffer
        if not self.buffer.rstrip():
            self.end = True
            raise StopIteration
        try:
            index = self.buffer.index('\f')
            posting = self.buffer[:index].rstrip()
            self.buffer = self.buffer[index + 1:]
        except ValueError:
            posting = self.buffer.rstrip()
            self.buffer = ""
        return self.posting.parse(posting)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


class PostingReader:
    def __init__(self, file_name: str, posting: Type[Posting]):
        self.open = open(f"{file_name}.index", 'r')
        self.index = open(f"{file_name}.index.position", 'r')
        keys = dict()
        for row in self.index:
            s = row.split('\t')
            keys[s[0]] = int(s[1])
        self.index.close()

        self.keys = keys

        self.posting_type = posting
        self.current_key = None
        self.read_lock = RLock()
        self.buffer = ""
        self.__eof = False
        self.posting_iterator = None

        self.__read__()

    def __read__(self):
        self.buffer = ""
        buffer = self.open.readline(4096)
        while '\f' not in buffer and buffer:
            self.buffer += buffer
            buffer = self.open.readline(4096)
        try:
            index = buffer.index('\f')
            self.posting_iterator = PostingIterator(self.read_lock, self.open, self.posting_type, buffer[index + 1:])
            self.current_key = buffer[:index]
        except ValueError:
            self.posting_iterator = None
            self.__eof = True

    def read_posting(self) -> Optional[Posting]:
        try:
            return self.posting_iterator.__next__()
        except StopIteration:
            return None

    def get_iterator(self):
        return self.posting_iterator

    def read_key(self):
        self.__read__()

    def seek(self, position: int):
        self.open.seek(position)

    def current_row(self) -> str:
        return self.current_key

    def eof(self):
        return self.__eof


def merge(merged: PostingWriter, *files: [PostingReader]):
    keys = [(file.current_row(), file) for file in files if not file.eof()]
    keys.sort()
    while keys:
        minimum_keys = [keys[0]]
        for _, key in enumerate(keys, 1):
            if key[0] == minimum_keys[0][0]:
                minimum_keys.append(key)
        merged.write_key(minimum_keys[0][0])
        for posting in merge_postings(*[file.get_iterator() for _, file in minimum_keys]):
            merged.write_posting(posting)
        for _, file in minimum_keys:
            file.read_key()
        keys = [(file.current_row(), file) for file in files if not file.eof()]

def merge_postings(*postings):
    for elements in zip_longest(*postings):
        postings = []
        for posting in elements:
            if posting:
                postings.append(posting)
        postings.sort()
        for posting in postings:
            yield posting
