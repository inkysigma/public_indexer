from posting.io import PostingReader, PostingIterator
from posting.post import Posting
from doc import DocumentIdDictionary
import math
from posting.tokenizer import TokenizeResult
from typing import Callable, List, Tuple, Type


class QueryScoringScheme:
    def get_posting_type(self) -> Type[Posting]:
        raise NotImplementedError

    def score(self, query: [str]) -> List[Tuple[Posting, float]]:
        raise NotImplementedError

    def create_posting(self, document: str, result: TokenizeResult) -> [Tuple[str, Posting]]:
        raise NotImplementedError

    def finalize_posting(self, iterator: PostingIterator):
        raise NotImplementedError


class MultiScoringScheme:
    def score(self, query: [str]) -> List[Tuple[int, float]]:
        pass

    def __init__(self, document_id: DocumentIdDictionary,
                 *schemes: Tuple[DocumentIdDictionary, PostingReader, QueryScoringScheme]):
        """
        Scoring multiple schemes simultaneously
        :param document_id:
        :param schemes:
        """
        self.schemes = schemes


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
