import json
import re
from collections import defaultdict
from typing import Optional

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

from doc import DocumentIdDictionary, normalize_url
from posting.tokenizer import Tokenizer, TokenizeResult, Token

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


class AnchorTokenizer(Tokenizer):
    def __init__(self, document: DocumentIdDictionary):
        self.document = document

    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        with open(file_name, 'r') as f:
            obj = json.load(f)

            if obj["encoding"].lower() not in PERMITTED_ENCODINGS:
                return None

            # Link finding taken from
            # https://stackoverflow.com/questions/1080411/retrieve-links-from-web-page-using-python-and-beautifulsoup
            document = BeautifulSoup(obj["content"], 'lxml', from_encoding=obj["encoding"])
            total_count = 0
            targets = defaultdict(lambda: defaultdict(int))
            for element in document.find_all('a', href=True):
                link = element['href']
                if self.document.contains_url(normalize_url(link)):
                    target_id = self.document.find_doc_id_by_url(normalize_url(link))
                    text_body = element.getText()
                    for token in map(process_token, tokenizer(text_body)):
                        targets[token][target_id] += 1

            tokens = []
            for word in targets:
                for target in targets[word]:
                    tokens.append(Token(word, targets[word][target], {"target_id": target_id}))
            return TokenizeResult(normalize_url(obj['url']), tokens, total_count)

    def tokenizer_query(self, query: str):
        return list(map(process_token, tokenizer(query)))


class PositionalTokenizer(Tokenizer):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        pass

    def tokenizer_query(self, query: str):
        pass
