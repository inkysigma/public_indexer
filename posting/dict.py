from .io import PostingWriter
from collections import defaultdict
from typing import Dict, List, Any


class PostingDictionary:
    def __init__(self, name: str):
        self.dictionary: Dict[str, List[Any]] = defaultdict(list)
        self.counter = 0
        self.total_count = 0
        self.name = name

    def add_posting(self, key: str, posting):
        self.total_count += 1
        self.dictionary[key].append(posting)

    def __len__(self):
        return self.total_count

    def flush(self):
        writer = PostingWriter(f"{self.name}/{self.counter}")
        for key in self.dictionary:
            writer.write_key(key)
            writer.write(self.dictionary[key])
        writer.flush()
        writer.close()
        self.counter += 1
        self.total_count = 0
