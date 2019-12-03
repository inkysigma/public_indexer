
from posting.tokenizer import Tokenizer, TokenizeResult
from typing import Optional

class PositionTokenizer(Tokenizer):
    def tokenize(self, file_name: str) -> Optional[TokenizeResult]:
        raise NotImplementedError

    def tokenizer_query(self, query: str):
        raise NotImplementedError
