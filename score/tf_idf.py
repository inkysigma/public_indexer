from posting.post import Posting
from posting.io import PostingIterator, PostingReader
from doc.doc_id import DocumentIdDictionary
import math
from typing import List
from collections import defaultdict


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
        inverse_document_frequency = math.log(len(list(reader.get_iterator())) / len(dictionary))
        scores.append((1 + math.log(word_dict[word] / len(words))) * inverse_document_frequency)
    return scores
