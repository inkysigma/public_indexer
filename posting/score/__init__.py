import math
from collections import defaultdict
from heapq import nlargest
from time import time
from typing import Callable, List, Tuple, Type, Iterator

from doc import DocumentIdDictionary
from fixed import FixedScoreDictionary
from posting.io import PostingReader, PostingIterator
from posting.post import Posting
from posting.tokenizer import TokenizeResult, Tokenizer


class QueryScoringScheme:
    def get_posting_type(self) -> Type[Posting]:
        raise NotImplementedError

    def score(self, query: [str]) -> Iterator[Tuple[Posting, float]]:
        raise NotImplementedError

    def create_posting(self, document: str, result: TokenizeResult) -> [Tuple[str, Posting]]:
        raise NotImplementedError

    def finalize_posting(self, iterator: PostingIterator):
        raise NotImplementedError


class MultiScoringScheme:
    def score(self, query: str, limit: int = 10) -> List[Tuple[int, float]]:
        # maps the id to the score and the number of
        array = defaultdict(lambda: [0, 0])
        # the total number of doc_ids intersected by all of them
        total_filled = 0
        start = time()

        iterators = dict()
        factors = dict()
        tqs = dict()
        counter = 0
        for factor, tokenizer, scheme in self.schemes:
            tq = tokenizer.tokenizer_query(query)
            if not tq:
                continue
            tqs[counter] = tq
            iterators[counter] = scheme.score(tq)
            factors[counter] = factor
            counter += 1

        while total_filled < 40 and time() - start < 0.200 and len(array) < 200 and len(iterators) > 0:
            to_delete = []
            for i, it in iterators.items():
                try:
                    doc_id, score = next(it)
                    array[doc_id][0] += factors[i] * score
                    array[doc_id][1] += 1
                    if array[doc_id][1] == len(iterators):
                        total_filled += 1
                except StopIteration:
                    to_delete.append(i)
            for i in to_delete:
                del iterators[i]
        if len(array) < 10:
            tq = tqs[0][0]
            if tq:
                for doc, score in self.schemes[0][2].score(tq):
                    if time() - start > 0.26:
                        break
                    array[doc][0] += self.schemes[0][0] * score
        if self.fixed:
            for doc_id in array:
                if doc_id in self.fixed.keys:
                    array[doc_id][0] += self.fixed[doc_id]
        return nlargest(limit, [(item[0], item[1][0]) for item in array.items()], key=lambda x: x[1])

    def __init__(self, fixed: FixedScoreDictionary, *schemes: Tuple[int, Tokenizer, QueryScoringScheme]):
        """
        Scoring multiple schemes simultaneously
        :param document_id:
        :param schemes:
        """
        self.fixed = fixed
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
