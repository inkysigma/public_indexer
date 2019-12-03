from heapq import nlargest
from doc import from_document_dictionary
from posting.score.tf_idf import TfIdfScoring
import re
from nltk import PorterStemmer
import json
from flask import Flask, render_template
from heapq import nlargest
from posting.tokenizer.ngram import WordTokenizer

STEMMER = PorterStemmer()
SEQUENCE = re.compile(r"\w+", re.ASCII)

app = Flask(__name__)

INDEXES = {"primary"}
SCHEMES = dict()

for index in INDEXES:
    dictionary = from_document_dictionary(index)
    scheme = TfIdfScoring(dictionary, f"{index}/schemed")
    SCHEMES[index] = (dictionary, scheme)


class QueryResult:
    def __init__(self, url: str, score: float):
        self.url = url
        self.score = score


@app.route("/", methods=["GET", "POST"])
def search(request):
    if "query" in request.form:
        tokens = WordTokenizer().tokenizer_query(request.form['query'])
        top = nlargest(10, SCHEMES["primary"][1].score(tokens), key=lambda x: x[1])
        top = [QueryResult(SCHEMES["primary"][0].find_url_by_id(doc_id), score) for doc_id, score in top]
        return render_template("index.html", query_result=True, query_results=top)
    return render_template("index.html", query_result=False, query_results=[])


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
