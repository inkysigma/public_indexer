from heapq import nlargest
from doc import from_document_dictionary
from posting.score.tf_idf import TfIdfScoring
import re
from nltk import PorterStemmer
import json
from flask import Flask, render_template

from posting.tokenizer.ngram import WordTokenizer

STEMMER = PorterStemmer()
SEQUENCE = re.compile(r"\w+", re.ASCII)

app = Flask(__name__)

INDEXES = {"primary"}
SCHEMES = dict()

for index in INDEXES:
    dictionary = from_document_dictionary(index)
    scheme = TfIdfScoring(dictionary, f"{index}/scheme")
    SCHEMES[index] = (dictionary, scheme)


@app.route("/", methods=["GET", "POST"])
def search(request):
    if "query" in request.form:
        top_k = []
        tokenizer = WordTokenizer()
        dictionary = SCHEMES["primary"][0]
        query = tokenizer.tokenizer_query(request.form)
        documents = nlargest(20, SCHEMES["primary"][1].score(query), key = lambda x: x[1])
        for i, document in enumerate(documents):
            with open(dictionary.get_doc_file(document[0]), 'r') as f:
                top_k.append(i, json.load(f)['url'], document[1])
        return render_template("index.html", query_result=True, query_results=top_k)
    return render_template("index.html", query_result=False, query_results=[])


"""
    query = input("What documents would you like to lookup? ")
    sequence = SEQUENCE.findall(query.lower())
    dictionary = from_document_dictionary("secondary")
    query_vec = tf_idf_query([STEMMER.stem(word) for word in sequence], dictionary, reader)
    word_scores = defaultdict(lambda: defaultdict(float))
    for word in sequence:
        for doc_id, score in score_word(tf_idf, STEMMER.stem(word), dictionary, reader):
            word_scores[doc_id][word] = score
    word_vectors = []
    for doc in word_scores:
        vec = []
        ignore = False
        for word in sequence:
            if word in word_scores[doc]:
                vec.append(word_scores[doc][word])
            else:
                ignore = True
                break
        if ignore: continue
        word_vectors.append((doc, vec))"""

if __name__ == "__main__":
    query = None
    tokenizer = WordTokenizer()
    dictionary = SCHEMES["primary"][0]
    while query != "exit":
        query = input("What would you like to look up? ")
        query = tokenizer.tokenizer_query(query)
        documents = nlargest(20, SCHEMES["primary"][1].score(query), key=lambda x: x[1])
        for i, document in enumerate(documents):
            with open(dictionary.get_doc_file(document[0]), 'r') as f:
                print(i, json.load(f)['url'], document[1])
    app.run(port=8081)
