import json
from heapq import nlargest
from time import time

from flask import Flask, render_template, request

from doc import from_document_dictionary
from fixed import FixedScoreDictionary
from posting.score import MultiScoringScheme
from posting.score.tf_idf import TfIdfScoring
from posting.tokenizer.bold import BoldTokenizer
from posting.tokenizer.ngram import WordTokenizer, BigramTokenizer

app = Flask(__name__)

INDEXES = {"indexes/default": (1, WordTokenizer()), "indexes/bigram": (4, BigramTokenizer()),
           "indexes/bold": (3, BoldTokenizer())}
DICTIONARY = from_document_dictionary("indexes/default")
SCHEMES = dict()

for index in INDEXES:
    dictionary = from_document_dictionary(index)
    scheme = TfIdfScoring(dictionary, f"{index}/schemed")
    SCHEMES[index] = (INDEXES[index][0], INDEXES[index][1], scheme)

FIXED = FixedScoreDictionary("indexes/page_rank", read=True)
MULTIWAY = MultiScoringScheme(FIXED, *SCHEMES.values())


class QueryResult:
    def __init__(self, url: str, score: float):
        self.url = url
        self.score = score


@app.route("/", methods=["GET", "POST"])
def search():
    start_time = time()
    if "query" in request.form:
        top = [QueryResult(DICTIONARY.find_url_by_id(doc_id), score) for doc_id, score in
               MULTIWAY.score(request.form['query'])]
        end_time = time()
        return render_template("index.html", query_result=True, query_results=top, difference=end_time - start_time)
    end_time = time()
    return render_template("index.html", query_result=False, query_results=[], difference=end_time - start_time)


if __name__ == "__main__":
    print("Starting the actual application")
    query_input = None
    tokenizer = WordTokenizer()
    dictionary = DICTIONARY
    scheme = TfIdfScoring(dictionary, "indexes/default/schemed")
    while query_input != "exit":
        query_input = input("What would you like to look up? ")
        start = time()
        query = tokenizer.tokenizer_query(query_input)
        print(query)
        documents = nlargest(20, scheme.score(query), key=lambda x: x[1])
        end = time()
        print("Result in " + str(end - start))
        for i, document in enumerate(documents):
            with open(dictionary.get_doc_file(document[0]), 'r') as f:
                print(i, json.load(f)['url'], document[1])
    print("Starting the server")
    app.run(port=8081)
