import json
import re
from collections import defaultdict
from typing import Optional

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

from doc import normalize_url
from posting.tokenizer import Tokenizer, TokenizeResult, Token

PERMITTED_ENCODINGS = {
    "utf-8", "latin-1", "utf-16", "utf-32", "ascii", "ISO-8859-1".lower(), "UTF-8-SIG".lower(), "EUC-KR".lower(),
    "EUC-JP".lower()
}

RE_MATCH = re.compile(r"\w+", re.ASCII)
STEMMER = PorterStemmer()


def tokenizer(tag):
    if tag is None:
        return []
    try:
        return RE_MATCH.findall(tag.strip())
    except TypeError:
        return []


def process_token(token):
    if token is None:
        return None
    try:
        return STEMMER.stem(token.lower().strip())
    except TypeError:
        return None


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
                tags = list(document.find_all(tag, text=True))
                for element in tags:
                    for t in map(process_token, tokenizer(element.string)):
                        if not t:
                            continue
                        words[t] += 1
                        total_count += 1

            extract('b')
            extract('h1')
            extract('h2')
            extract('h3')
            extract('h4')
            if document.title:
                for token in map(process_token, tokenizer(document.title.string)):
                    if not token:
                        continue
                    words[token] += 1

            tokens = [Token(word, count) for word, count in words.items()]
            return TokenizeResult(normalize_url(obj['url']), tokens, total_count)

    def tokenizer_query(self, query: str):
        return list(map(process_token, tokenizer(query)))
