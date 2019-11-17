import abc


class Tokenizer(abc.ABC):
    def tokenize(self, file_name: str):
        raise NotImplementedError
