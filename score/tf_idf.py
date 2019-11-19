from posting.post import Posting
from posting.io import PostingIterator
from doc.doc_id import DocumentIdDictionary
import math


def tf_idf(dictionary: DocumentIdDictionary, postings: PostingIterator) -> [float]:
    scores = []
    total_count = 0
    for posting in postings:
        frequency = posting["count"] / int(dictionary.get_document_property(posting.doc_id, "total_count"))
        scores.append(frequency)
        total_count += 1
    inverse_document_frequency = total_count / len(dictionary)
    return map(lambda x: (1 + math.log2(frequency)) * math.log2(inverse_document_frequency), scores)
