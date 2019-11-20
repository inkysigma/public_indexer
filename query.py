from heapq import nlargest
from posting.io import PostingReader
from tokenizer.ngram import WordTokenizer
from doc import from_document_dictionary
from score import tf_idf, tf_idf_query, score_word, score_cosine
import re
from collections import defaultdict
from nltk import PorterStemmer
import json

STEMMER = PorterStemmer()
SEQUENCE = re.compile(r"\w+", re.ASCII)

if __name__ == "__main__":
    query = input("What documents would you like to lookup?")
    sequence = SEQUENCE.findall(query)
    Posting = WordTokenizer().get_posting_type()
    reader = PostingReader("finalized/default", Posting)
    dictionary = from_document_dictionary("default")

    query_vec = tf_idf_query([STEMMER.stem(word) for word in sequence], dictionary, reader)
    word_scores = defaultdict(lambda: defaultdict(float))
    for word in sequence:
        for doc_id, score in score_word(tf_idf, STEMMER.stem(word), dictionary, reader):
            word_scores[doc_id][word] = score
    word_vectors = []
    for doc in word_scores:
        vec = []
        for word in sequence:
            vec.append(word_scores[doc][word] if word in word_scores[doc] else 0)
        word_vectors.append((doc, vec))
    documents = nlargest(20, map(lambda x: (x[0], score_cosine(x[1], query_vec)), word_vectors), key=lambda x: x[1])
    for document in documents:
        with open(dictionary[document[0]], 'r') as f:
            print(json.load(f)['url'], document[1])
