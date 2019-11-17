from tokenizer import Tokenizer
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
import json


class WordTokenizer(Tokenizer):
    def tokenize(self, file_name: str):
        with open(file_name) as file:
            obj = json.load(file)
