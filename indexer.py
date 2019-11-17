import glob
import json
import os
from bs4 import BeautifulSoup
from bs4.element import Comment
from collections import defaultdict
import lxml
import re
import multiprocessing as multi
import shelve
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import pickle
from urllib.parse import urlsplit, urldefrag
import threading

ROOT_DIR = "/home/lopes/Datasets/IR/DEV"
# VALID_ENCODINGS = {"utf-8", "latin-1", "utf-16", "utf-32", "ascii"}
VALID_ENCODINGS = {"ISO-8859-1".lower(), "UTF-8-SIG".lower(), "EUC-KR".lower(), "EUC-JP".lower()}
STEMMER = PorterStemmer()
RE_MATCH = re.compile(r"[0-9a-zA-Z_\-/\\]+")

VISITED = dict()
URLS = dict()

# Maps a word to the hash it contains
WORDS_TO_DOCUMENTS = defaultdict(lambda: defaultdict(int))

# Maps a word to the hash it contains as well, but does it with the tokenizer from nltk
SECONDARY_WORD_TOKENIZER = defaultdict(lambda: defaultdict(int))

# Maps a set of document hashes to a word frequency
WORD_FREQUENCIES = defaultdict(lambda: defaultdict(int))
HASH_TO_URL = dict()

RUNNING = True
FLUSH = False
RESET_COUNT = 0


def tag_visible(element):
    """
    Snippet was obtained via https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text
    """
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def test_int(i):
    try:
        return int(i)
    except ValueError:
        return None


def tokenizer(tag):
    return RE_MATCH.findall(tag.string.strip())


def process_token(token):
    return STEMMER.stem(token.lower().strip())


def secondary_tokenizer(tag):
    return word_tokenize(tag.string.strip())


def get_hash(file):
    return os.path.splitext(os.path.basename(file))[0]


FREQUENCY_DIR = "frequencies"


def save_document(url, hashurl, words):
    url = urlsplit(url).netloc
    if not os.path.exists(f"{FREQUENCY_DIR}/{url}"):
        os.mkdir(f"{FREQUENCY_DIR}")
    with shelve.open(f'{FREQUENCY_DIR}/{url}.shelve', writeback=True) as file:
        file[hashurl] = words
        file.sync()


def flush():
    print("flushing")
    with shelve.open('index.shelve', writeback=True) as index:
        for word in WORDS_TO_DOCUMENTS:
            if word not in index:
                index[word] = dict()
            index[word].update(WORDS_TO_DOCUMENTS[word])
        index.sync()
    with shelve.open('secondary.shelve', writeback=True) as secondary:
        for word in SECONDARY_WORD_TOKENIZER:
            if word not in secondary:
                secondary[word] = dict()
            secondary[word].update(SECONDARY_WORD_TOKENIZER[word])
        secondary.sync()
    with shelve.open('hash.shelve', writeback=True) as hash_map:
        hash_map.update(HASH_TO_URL)
        hash_map.sync()
    with shelve.open('visited.shelve', writeback=True) as visited_shelve:
        visited_shelve.update(VISITED)
        visited_shelve.sync()
    with shelve.open("frequencies.shelve", writeback=True) as frequencies:
        frequencies.update(WORD_FREQUENCIES)
        frequencies.sync()
    SECONDARY_WORD_TOKENIZER.clear()
    HASH_TO_URL.clear()
    WORD_FREQUENCIES.clear()
    WORDS_TO_DOCUMENTS.clear()


def main():
    print("Starting the process")
    global FLUSH
    global RESET_COUNT

    for file in glob.glob(ROOT_DIR + "/**/*.json"):
        if not RUNNING:
            return
        if FLUSH:
            flush()
            FLUSH = False
        if RESET_COUNT > 100:
            flush()
            print(len(VISITED))
            RESET_COUNT = 0
        if get_hash(file) in VISITED:
            continue
        with open(file, 'r') as f:
            obj = json.load(f)
            if obj['encoding'].lower() not in VALID_ENCODINGS:
                continue
            print(obj['url'], get_hash(file))

        if urldefrag(obj['url']) in URLS:
            continue

        b = BeautifulSoup(obj['content'], 'lxml', from_encoding=obj['encoding'])
        tags = b.find_all(text=True)
        words = defaultdict(int)
        for tag in filter(tag_visible, tags):
            for token in map(process_token, tokenizer(tag)):
                if test_int(token) and len(token) > 5:
                    continue
                if token.count('-') > 3 or token.count("\\") > 3 or token.count('/') > 3:
                    continue
                WORDS_TO_DOCUMENTS[token][get_hash(file)] += 1
                words[token] += 1
            for secondary in map(process_token, secondary_tokenizer(tag)):
                SECONDARY_WORD_TOKENIZER[secondary][get_hash(file)] += 1

        if get_hash(file) in HASH_TO_URL:
            print("SEVERE ERROR with " + obj[url] + " " + file)
            raise KeyboardInterrupt

        URLS[urldefrag(obj['url'])] = True

        WORD_FREQUENCIES[get_hash(file)] = words
        HASH_TO_URL[get_hash(file)] = obj['url']
        VISITED[get_hash(file)] = True
        RESET_COUNT += 1


if __name__ == "__main__":
    with shelve.open("visited.shelve") as visited:
        VISITED.update(dict(visited))
    WORK_THREAD = threading.Thread(target=main)
    try:
        WORK_THREAD.start()
        while input().strip() != "":
            FLUSH = True
        RUNNING = False
        WORK_THREAD.join()
    except KeyboardInterrupt:
        RUNNING = False
        WORK_THREAD.join()
        pass
    except Exception as err:
        print(err)
        pass
    flush()
