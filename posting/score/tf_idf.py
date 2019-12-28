from posting.post import Posting
from posting.io import PostingIterator, PostingReader
from doc.doc_id import DocumentIdDictionary
import math
from typing import List, Tuple, Type, Optional
from collections import defaultdict
from posting.score import QueryScoringScheme
from posting.tokenizer import TokenizeResult
from posting import create_posting_type, intersect
from statistics import mean, stdev


def get_normalized_vector(v: List[float]):
    normalize = math.sqrt(sum(f ** 2 for f in v if f))
    return map(lambda x: x / normalize, v)


def score_cosine(a: List[float], b: List[float]):
    assert len(a) == len(b), "Vectors are not of equal length"
    return sum(
        i * j for i, j in zip(get_normalized_vector(a), get_normalized_vector(b)) if i is not None and j is not None)


class TfIdfScoring(QueryScoringScheme):
    def __init__(self, dictionary: DocumentIdDictionary, file: Optional[str]):
        self.posting_type = create_posting_type("tf_idf", {"count": int, "tf": float, "tf_idf": float})
        self.dictionary = dictionary
        if file:
            self.reader = PostingReader(file, self.posting_type)
        else:
            self.reader = None

    def get_posting_type(self) -> Type[Posting]:
        return self.posting_type

    def score(self, query: [str]) -> List[Tuple[int, float]]:
        assert self.reader is not None, "Did not provide a reader at initialization"
        query_vec = []
        word_dict = defaultdict(int)
        counts = [self.reader.count(word) if word in self.reader.keys else 0 for word in query]
        avg = mean(counts)
        if len(query) > 2:
            std = stdev(counts)
            for word in query:
                if word not in self.reader.keys:
                    continue
                if self.reader.count(word) <= avg + 2 * std:
                    word_dict[word] += 1
        else:
            for word in query:
                if word not in self.reader.keys:
                    continue
                word_dict[word] += 1
        for word in word_dict:
            inverse_document_frequency = math.log10(len(self.dictionary) / self.reader.count(word))
            query_vec.append((math.log10(1 + word_dict[word] / len(query))) * inverse_document_frequency)
        iterators = []
        for word in word_dict:
            if word in self.reader.keys:
                self.reader.seek(word)
                iterators.append(self.reader.get_iterator())
            else:
                iterators.append(None)
        iterator = intersect(*iterators)
        for posting in iterator:
            yield (posting.doc_id, score_cosine(query_vec, posting.get_properties("tf_idf")))

    def create_posting(self, document: str, result: TokenizeResult) -> [Tuple[str, Posting]]:
        postings = []
        for token in result.tokens:
            postings.append((token.word,
                             self.posting_type(self.dictionary.find_doc_id(document),
                                               {"tf": float(token.count) / result.total_count,
                                                "tf_idf": 0,
                                                "count": token.count})))
        return postings

    def finalize_posting(self, iterator: PostingIterator) -> [Posting]:
        postings = list(iterator)
        idf = math.log10(len(self.dictionary.doc_id) / len(postings))
        for posting in postings:
            posting.set_property("tf_idf", idf * math.log10(1 + posting.get_property("tf")))
        return postings


def tf_idf(dictionary: DocumentIdDictionary, postings: PostingIterator) -> ([int], [float]):
    scores = []
    ids = []
    total_count = 0
    for posting in postings:
        frequency = posting["count"] / int(dictionary.get_document_property(posting.doc_id, "total_count"))
        scores.append(frequency)
        total_count += 1
        ids.append(posting.doc_id)
    inverse_document_frequency = math.log(total_count / len(dictionary))
    return map(lambda x: (x[0], (1 + math.log(x[1])) * inverse_document_frequency), zip(ids, scores))


def tf_idf_query(words: List[str], dictionary: DocumentIdDictionary, reader: PostingReader) -> List[float]:
    scores = []
    word_dict = defaultdict(int)
    for word in words:
        word_dict[word] += 1
    for word in word_dict:
        reader.seek(word)
        inverse_document_frequency = math.log10(len(dictionary) / len(list(reader.get_iterator())))
        scores.append((1 + math.log10(word_dict[word] / len(words))) * inverse_document_frequency)
    return scores
