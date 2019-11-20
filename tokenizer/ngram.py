from typing import Type, Optional

from posting import Posting, create_posting_type
from tokenizer import Tokenizer, TokenizeResult
from bs4 import BeautifulSoup
from bs4.element import Comment
from collections import defaultdict
import re
from nltk.stem import PorterStemmer
import json

RE_MATCH = re.compile(r"\w+", re.ASCII)
STEMMER = PorterStemmer()

PERMITTED_ENCODINGS = {
    "utf-8", "latin-1", "utf-16", "utf-32", "ascii", "ISO-8859-1".lower(), "UTF-8-SIG".lower(), "EUC-KR".lower(),
    "EUC-JP".lower()
}


def tag_visible(element):
    """
    Snippet was obtained via https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text
    """
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def parse_int(i):
    try:
        return int(i)
    except ValueError:
        return None


def tokenizer(tag):
    return RE_MATCH.findall(tag.string.strip())


def process_token(token):
    return STEMMER.stem(token.lower().strip())


class WordTokenizer(Tokenizer):
    def get_posting_type(self) -> Type[Posting]:
        return create_posting_type("word_tokenizer", {"count": int})

    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        with open(file_name) as file:
            obj = json.load(file)
            if obj["encoding"].lower() not in PERMITTED_ENCODINGS:
                return None
            document = BeautifulSoup(obj["content"], 'lxml', from_encoding=obj["encoding"])

            words = defaultdict(int)
            token_count = 0
            for tag in filter(tag_visible, document.find_all(text=True)):
                for token in map(process_token, tokenizer(tag)):
                    if parse_int(token) and len(token) > 5:
                        continue
                    if token.count('-') > 3 or token.count("\\") > 3 or token.count('/') > 3:
                        continue
                    words[token] += 1
                    token_count += 1
            return TokenizeResult(obj["url"], list(words.items()), {"total_count": token_count})
