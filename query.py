from heapq import nlargest
from doc import from_document_dictionary
from posting.score.tf_idf import TfIdfScoring
import re
from nltk import PorterStemmer
import json
from flask import Flask, render_template, request
from heapq import nlargest
from posting.score import MultiScoringScheme
from posting.tokenizer.ngram import WordTokenizer
from time import time

STEMMER = PorterStemmer()
SEQUENCE = re.compile(r"\w+", re.ASCII)

app = Flask(__name__)

INDEXES = {"default": 1, "bigram": 2, "trigram": 4, "bold": 3}
SCHEMES = dict()

for index in INDEXES:
    dictionary = from_document_dictionary(index)
    scheme = TfIdfScoring(dictionary, f"{index}/schemed")
    SCHEMES[index] = (INDEXES[index], dictionary, scheme)

MULTIWAY = MultiScoringScheme(*SCHEMES.values())


class QueryResult:
    def __init__(self, url: str, score: float):
        self.url = url
        self.score = score


@app.route("/", methods=["GET", "POST"])
def search():
    start_time = time()
    if "query" in request.form:
        top = nlargest(10, MULTIWAY.score(request.form['query']), key=lambda x: x[1])
        end_time = time()
        return render_template("index.html", query_result=True, query_results=top, difference=end_time - start_time)
    end_time = time()
    return render_template("index.html", query_result=False, query_results=[], difference=end_time - start_time)


if __name__ == "__main__":
    app.run(port=8081)
    query = None
    tokenizer = WordTokenizer()
    dictionary = SCHEMES["primary"][1]
    while query != "exit":
        query = input("What would you like to look up? ")
        start = time()
        query = tokenizer.tokenizer_query(query)
        documents = nlargest(20, SCHEMES["primary"][1].score(query), key=lambda x: x[1])
        end = time()
        print("Result in " + str(end - start))
        for i, document in enumerate(documents):
            with open(dictionary.get_doc_file(document[0]), 'r') as f:
                print(i, json.load(f)['url'], document[1])
