from itertools import zip_longest

from typing import Optional, Type
from .post import Posting
from collections import defaultdict


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


class PostingReader:
    def __init__(self, file_name: str, posting: Type[Posting]):
        self.open = open(f"{file_name}.index", 'r')
        self.index = open(f"{file_name}.index.position", 'r')
        self.posting_type = posting
        self.current_key = None
        self.sequence = None
        self.index = 0
        self.__read__()

    def __read__(self):
        line = self.open.readline()
        if not line:
            self.sequence = None
            self.current_key = None
        line.rstrip()
        self.sequence = line.split('\f')
        self.current_key = self.sequence[0]

    def read_posting(self) -> Optional[Posting]:
        if self.sequence is None or self.index == self.sequence:
            return None
        return self.posting_type.parse(self.sequence[self.index])

    def read_key(self):
        self.__read__()

    def seek(self, position: int):
        self.open.seek(position)

    def current_row(self) -> str:
        return self.current_key


def merge(merged: PostingWriter, *files: [PostingReader]):
    pass


def merge_postings(*postings):
    for elements in zip_longest(*postings):
        postings = []
        for posting in elements:
            if posting:
                postings.append(posting)
        postings.sort()
        for posting in postings:
            yield posting
