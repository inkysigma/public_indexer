from .tf_idf import *
from posting.io import PostingReader
from posting.tokenizer import TokenizeResult
from typing import Callable, List, Tuple, Type


class QueryScoringScheme:
    def get_posting_type(self) -> Type[Posting]:
        raise NotImplementedError

    def score(self, query: [str], iterator: PostingIterator) -> List[Tuple[Posting, float]]:
        raise NotImplementedError

    def create_posting(self, document: str, result: TokenizeResult) -> [Posting]:
        raise NotImplementedError

    def finalize_posting(self, iterator: PostingIterator):
        raise NotImplementedError


def score_word(scheme: Callable[[DocumentIdDictionary, PostingIterator], List[Tuple[int, float]]], word: str,
               dictionary: DocumentIdDictionary, reader: PostingReader) -> List[Tuple[int, float]]:
    reader.seek(word)
    return scheme(dictionary, reader.get_iterator())


def get_normalized_vector(v: List[float]):
    normalize = math.sqrt(sum(f ** 2 for f in v))
    return map(lambda x: x / normalize, v)


def score_cosine(a: List[float], b: List[float]):
    assert len(a) != len(b), "Vectors are not of equal length"
    return sum(i * j for i, j in zip(get_normalized_vector(a), get_normalized_vector(b)))
