import abc
from typing import Dict, List, Tuple, Optional, Type
from posting import Posting


class TokenizeResult:
    def __init__(self, url: str, tokens: List[Tuple[str, int] or Tuple[str, int, Dict[str, int or float or [float] or [int]]]],
                 properties: Dict[str, int or float]):
        self.url = url
        self.tokens = tokens
        self.properties = properties


class Tokenizer(abc.ABC):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        raise NotImplementedError

    def get_posting_type(self) -> Type[Posting]:
        raise NotImplementedError
