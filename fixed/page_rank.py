import fixed
from doc import DocumentIdDictionary, normalize_url
from collections import defaultdict
import json
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer


class PageRank(fixed.FixedScorer):
    def __init__(self, dictionary: DocumentIdDictionary, fixed_id: fixed.FixedScoreDictionary):
        self.dictionary = dictionary
        self.fixed_id = fixed_id

    def process(self):
        """
        There are two phases to this processing. We first need to find the connections and store them in a dictionary.
        The dictionary should map each id to the set of ids they link to. Finally, we need to store the settled values
        into the file.
        """
        links = defaultdict(set)
        for key, value in self.dictionary.reverse_map:
            file = value[1]
            with open(file, 'r') as f:
                response = json.load(f)
                for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
                    if self.dictionary.contains_url(normalize_url(link)):
                        links[self.dictionary.find_doc_id(file)].add(
                            self.dictionary.find_doc_id_by_url(normalize_url(link)))
        prev_weights = dict()
        weights = dict()

    def flush(self):
        pass
