from posting.tokenizer import Tokenizer, TokenizeResult, Token
from doc import DocumentIdDictionary, normalize_url
from typing import Optional
import json
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer
from collections import defaultdict
from nltk.stem import PorterStemmer
import re

PERMITTED_ENCODINGS = {
    "utf-8", "latin-1", "utf-16", "utf-32", "ascii", "ISO-8859-1".lower(), "UTF-8-SIG".lower(), "EUC-KR".lower(),
    "EUC-JP".lower()
}

RE_MATCH = re.compile(r"\w+", re.ASCII)
STEMMER = PorterStemmer()


def tokenizer(tag):
    return RE_MATCH.findall(tag.strip())


def process_token(token):
    return STEMMER.stem(token.lower().strip())


class BoldTokenizer(Tokenizer):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        with open(file_name, 'r') as f:
            obj = json.load(f)

            if obj["encoding"].lower() not in PERMITTED_ENCODINGS:
                return None

            # Link finding taken from
            # https://stackoverflow.com/questions/1080411/retrieve-links-from-web-page-using-python-and-beautifulsoup
            document = BeautifulSoup(obj["content"], 'lxml', from_encoding=obj["encoding"])
            total_count = 1
            words = defaultdict(int)

            def extract(tag):
                nonlocal total_count
                tags = list(document.find_all(text=True))
                for element in tags:
                    for t in map(process_token, tokenizer(element.string)):
                        words[t] += 1
                        total_count += 1

            extract('b')
            extract('h1')
            extract('h2')
            extract('h3')
            extract('h4')
            if document.title:
                for token in map(process_token, document.title):
                    words[token] += 1

            tokens = [Token(word, count) for word, count in words.items()]
            return TokenizeResult(normalize_url(obj['url']), tokens, total_count)

    def tokenizer_query(self, query: str):
        return list(map(process_token, tokenizer(query)))
