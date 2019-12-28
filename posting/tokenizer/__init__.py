import abc
from typing import Dict, List, Optional


class Token:
    def __init__(self, word: str, count: int, properties: Dict[str, float or List[float]] = None):
        self.word = word
        self.count = count
        self.properties: Dict[str, float] = properties if properties else []

    def __lt__(self, other: "Token"):
        if type(other) is not Token:
            raise TypeError
        return self.word < other.word

    def __eq__(self, other):
        if type(other) is not Token:
            raise TypeError
        return self.word == other.word


class TokenizeResult:
    def __init__(self, url: str, tokens: List[Token], total_count: int, **properties: Dict[str, int or float]):
        """
        A result of tokenizing a document.
        :param url: the url associated with the tokenization
        :param tokens: the tokens from the result
        :param properties: the properties of the document
        """
        self.url = url
        self.tokens = tokens
        self.total_count = total_count
        self.properties = properties


class Tokenizer(abc.ABC):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        raise NotImplementedError

    def tokenizer_query(self, query: str):
        raise NotImplementedError
