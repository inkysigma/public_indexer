from .tf_idf import *
from posting.io import PostingReader
from typing import Callable, List, Tuple


def score_word(scheme: Callable[[DocumentIdDictionary, PostingIterator], List[Tuple[int, float]]], word: str,
               dictionary: DocumentIdDictionary, reader: PostingReader) -> List[Tuple[int, float]]:
    reader.seek(word)
    return scheme(dictionary, reader.get_iterator())


def get_normalized_vector(v: List[float]):
    normalize = sum(f ** 2 for f in v)
    return map(lambda x: x / normalize, v)


def score_cosine(a: List[float], b: List[float]):
    if len(a) != len(b):
        raise ValueError("Vectors are not of equal length")
    return sum(i * j for i, j in zip(get_normalized_vector(a), get_normalized_vector(b)))
