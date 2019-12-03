from posting.tokenizer import Tokenizer, TokenizeResult
from doc import DocumentIdDictionary, normalize_url
from typing import Optional
import json
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

PERMITTED_ENCODINGS = {
    "utf-8", "latin-1", "utf-16", "utf-32", "ascii", "ISO-8859-1".lower(), "UTF-8-SIG".lower(), "EUC-KR".lower(),
    "EUC-JP".lower()
}


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
            for element in document.find_all('a', href=True):
                element['href']

            return TokenizeResult()

    def tokenizer_query(self, query: str):
        pass


class PositionalTokenizer(Tokenizer):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        pass

    def tokenizer_query(self, query: str):
        pass
