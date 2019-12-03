from itertools import zip_longest

from typing import Optional, Type, IO
from .post import Posting, IntersectPosting
from threading import RLock
from collections import defaultdict
from typing import Iterator

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
        self.keys = defaultdict(lambda: list([0, 0]))
        self.curr_key = None

    def write_key(self, key: str):
        """
        Write a key at the current position.
        :param key:
        :return:
        """
        self.curr_key = key
        if len(self.keys[key]) > 0:
            self.open.write('\n')
        self.keys[key][0] = self.open.tell()
        self.open.write(f"{key}")

    def write_posting(self, posting: Posting):
        self.keys[self.curr_key][1] += 1
        self.open.write(f"\f{str(posting)}")

    def write(self, *postings):
        partition = "\f" + "\f".join([str(posting) for posting in postings])
        self.open.write(partition)

    def flush(self):
        self.open.flush()

    def close(self):
        for key in sorted(self.keys):
            self.index.write(f"{key}\t{self.keys[key][0]}\t{self.keys[key][1]}\n")
        self.keys.clear()
        self.index.close()
        self.open.close()


class PostingIterator:
    BUFFER_SIZE = 65536

    def __init__(self, lock: RLock, file: IO, posting: Type[Posting]):
        self.lock = lock

        self.file = file
        self.posting = posting

        self.posting_buffer = []

        self.buffer = file.readline(PostingIterator.BUFFER_SIZE)

        self.end = False
        with self.lock:
            buffer = self.buffer
            while '\f' not in buffer and not buffer.endswith('\n') and not self.end:
                buffer = self.file.readline(PostingIterator.BUFFER_SIZE)
                if buffer.endswith('\n'):
                    self.end = True
                self.buffer += buffer
            if self.buffer.endswith('\n'):
                self.buffer = self.buffer.rstrip('\n')
                self.end = True
        self.position = file.tell()

        parsed = self.buffer.split('\f')
        self.current = parsed[0]
        parsed = parsed[1:]
        if self.end:
            self.posting_buffer.extend([self.posting.parse(segment) for segment in parsed])
        else:
            self.buffer = parsed[-1]
            parsed = parsed[:-1]
            self.posting_buffer.extend([self.posting.parse(segment) for segment in parsed])

    def __read__(self):
        with self.lock:
            buffer = self.buffer
            self.file.seek(self.position)
            while '\f' not in buffer and not buffer.endswith('\n') and not self.end:
                buffer = self.file.readline(8096)
                if buffer.endswith('\n') or len(buffer) == 0:
                    self.end = True
                self.buffer += buffer

            self.position = self.file.tell()
            if self.buffer.endswith('\n'):
                self.buffer = self.buffer.rstrip('\n')
                self.end = True
            if not self.buffer:
                return
            parsed = self.buffer.split('\f')
            if self.end:
                self.posting_buffer.extend([self.posting.parse(segment) for segment in parsed])
            else:
                self.buffer = parsed[-1]
                parsed = parsed[:-1]
                self.posting_buffer.extend([self.posting.parse(segment) for segment in parsed])

    def current_key(self):
        return self.current

    def __iter__(self):
        return self

    def __next__(self) -> Posting:
        if self.posting_buffer:
            return self.posting_buffer.pop(0)
        if not self.end:
            self.__read__()
            if self.posting_buffer:
                return self.posting_buffer.pop(0)
        raise StopIteration


class PostingReader:
    def __init__(self, file_name: str, posting: Type[Posting]):
        self.open = open(f"{file_name}.index", 'r')
        self.index = open(f"{file_name}.index.position", 'r')
        keys = dict()
        for row in self.index:
            s = row.split('\t')
            keys[s[0]] = [int(s[1]), int(s[2])]
        self.index.close()

        self.keys = keys

        self.posting_type = posting
        self.read_lock = RLock()

    def __contains__(self, item):
        return item in self.keys

    def count(self, key: str):
        return self.keys[key][1]

    def get_iterator(self):
        return PostingIterator(self.read_lock, self.open, self.posting_type)

    def seek(self, position: int or str):
        if type(position) is int:
            self.open.seek(position)
        else:
            self.open.seek(self.keys[position][0])

    def close(self):
        self.open.close()


def intersect(*postings: PostingIterator) -> Iterator[IntersectPosting]:
    if len(postings) == 0:
        return []

    final = []

    postings = list(postings)
    try:
        heads = [next(posting) if posting else None for posting in postings]
    except StopIteration:
        return

    running = True
    while running:
        maximum = max((head for head in heads if head))
        all_equal = True
        for i in range(len(heads)):
            if heads[i] is None:
                final.append(None)
                continue
            try:
                while heads[i] is not None and heads[i] < maximum:
                    heads[i] = next(postings[i])
            except StopIteration:
                return
            if heads[i] != maximum:
                all_equal = False
            else:
                final.append(heads[i])
        if running and all_equal:
            yield IntersectPosting(*final)
            try:
                heads = [next(posting) for posting in postings]
            except StopIteration:
                return
        final.clear()


def merge(merged: PostingWriter, *files: [PostingReader]):
    keys = [(file.current_row(), file) for file in files if not file.eof()]
    while keys:
        minimum = min(keys, key=lambda x: x[0])[0]
        minimum_keys = []
        for key, reader in keys:
            if key == minimum:
                minimum_keys.append(reader)
        merged.write_key(minimum)
        for posting in merge_postings(*[file.get_iterator() for file in minimum_keys]):
            merged.write_posting(posting)
        for file in minimum_keys:
            file.read_key()
        keys = [(file.current_row(), file) for file in files if not file.eof()]


def merge_postings(*postings) -> Iterator[Posting]:
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
