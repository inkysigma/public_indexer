from posting.post import Posting
from posting.io import PostingIterator, PostingReader
from doc.doc_id import DocumentIdDictionary
import math
from typing import List, Tuple, Type
from collections import defaultdict
from posting.score import QueryScoringScheme
from posting.tokenizer import TokenizeResult
from posting import create_posting_type


class TfIdfScoring(QueryScoringScheme):
    def __init__(self, dictionary: DocumentIdDictionary):
        self.posting_type = create_posting_type("tf_idf", {"tf_idf": float})
        self.dictionary = dictionary

    def get_posting_type(self) -> Type[Posting]:
        return self.posting_type

    def score(self, query: [str], iterator: PostingIterator) -> List[Tuple[Posting, float]]:
        pass

    def create_posting(self, document: str, result: TokenizeResult) -> [Posting]:
        postings = []
        for token in result.tokens:
            postings.append(Posting(self.dictionary.get_doc_id()))
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
