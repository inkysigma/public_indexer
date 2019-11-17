from itertools import zip_longest

from posting import PostingType
from typing import Optional


class PostingWriter:
    def __init__(self, file_name: str):
        self.open = open(f"{file_name}.index", 'w+')
        self.index = open(f"{file_name}.index.position", 'w+')
        self.keys = dict()

    def write_key(self, key: str):
        self.keys[key] = self.open.tell()
        self.open.write(f"\n{key}")

    def write_posting(self, posting):
        self.open.write(f"\f{str(posting)}")

    def write(self, *postings):
        for posting in postings:
            self.write_posting(posting)

    def flush(self):
        self.open.flush()

    def close(self):
        for key in sorted(self.keys):
            self.index.write(f"{key}\t{self.keys[key]}\n")
        self.open.close()


class PostingReader:
    def __init__(self, file_name: str, posting_type):
        self.open = open(f"{file_name}.index", 'r')
        self.index = open(f"{file_name}.index.position", 'r')
        self.posting_type = posting_type
        self.current_key = None
        self.sequence = None
        self.index = 0
        self.__read__()

    def __read__(self):
        line = self.open.readline()
        line.rstrip()
        self.sequence = line.split('\f')
        self.current_key = self.sequence[0]

    def read_posting(self) -> Optional[PostingType]:
        if self.index == self.sequence:
            return None
        return self.posting_type.parse(self.sequence[0])

    def __iter__(self):
        pass

    def __next__(self):
        pass

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
