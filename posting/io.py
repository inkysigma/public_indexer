from itertools import zip_longest

from typing import Optional, Type, IO
from .post import Posting
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
            buffer = self.file.readline(8000)
            if buffer.endswith('\n') or not buffer:
                self.end = True
            self.buffer += buffer
        if not self.buffer.rstrip():
            self.end = True
            raise StopIteration
        try:
            index = self.buffer.index('\f')
            posting = self.buffer[:index].rstrip('\n')
            self.buffer = self.buffer[index + 1:]
        except ValueError:
            posting = self.buffer.rstrip('\n')
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
        try:
            buffer = self.open.readline(4096)
            if '\f' in buffer:
                self.buffer += buffer
            else:           
                while '\f' not in buffer and buffer:
                    buffer = self.open.readline(4096)
                    self.buffer += buffer
        except EOFError:
            self.posting_iterator = None
            self.__eof = True
            return
        try:
            index = self.buffer.index('\f')
            self.posting_iterator = PostingIterator(self.read_lock, self.open, self.posting_type, self.buffer[index + 1:])
            self.current_key = self.buffer[:index]
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
        if not self.current_key:
            raise EOFError
        return self.current_key

    def seek(self, position: int or str):
        if type(position) is int:
            self.open.seek(position)
            self.__eof = False
            self.__read__()
        else:
            self.open.seek(self.keys[position])
            self.__eof = False
            self.__read__()

    def current_row(self) -> str:
        return self.current_key

    def eof(self):
        return self.__eof


def merge(merged: PostingWriter, *files: [PostingReader]):
    keys = [(file.current_row(), file) for file in files if not file.eof()]
    while keys:
        minimum = min(keys, key=lambda x: x[0])[0]
        minimum_keys = []
        for key, reader in keys:
            if key == minimum:
                minimum_keys.append(reader)
        merged.write_key(minimum)
        if reader.current_row() == '':
            print(minimum_keys)
            input("ERROR")
        for posting in merge_postings(*[file.get_iterator() for file in minimum_keys]):
            merged.write_posting(posting)
        for file in minimum_keys:
            file.read_key()
        keys = [(file.current_row(), file) for file in files if not file.eof()]


def merge_postings(*postings) -> Posting:
    iterations = postings
    current_heads = [next(it) for it in iterations]
    running = True
    while running:
        running = False
        try:
            minimum = min(x for x in current_heads if x)
        except ValueError:
            return
        for i, head in enumerate(current_heads):
            if head and head == minimum:
                yield head
                try:
                    current_heads[i] = next(iterations[i])
                except StopIteration:
                    current_heads[i] = None
                running = True

