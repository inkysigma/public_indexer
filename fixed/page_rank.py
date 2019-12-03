import fixed
from doc import DocumentIdDictionary, normalize_url
from collections import defaultdict
import json
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer


class PageRank(fixed.FixedScorer):
    def __init__(self, dictionary: DocumentIdDictionary, fixed_id: fixed.FixedScoreDictionary, alpha=0.85):
        self.dictionary = dictionary
        self.fixed_id = fixed_id
        self.iterations = 10
        self.alpha = 0.85

    def process(self):
        """
        There are two phases to this processing. We first need to find the connections and store them in a dictionary.
        The dictionary should map each id to the set of ids they link to. Finally, we need to store the settled values
        into the file.
        """
        links = defaultdict(lambda: defaultdict(int))
        link_count = defaultdict(int)
        for key, value in self.dictionary.reverse_map.items():
            file = value[1]
            with open(file, 'r') as f:
                response = json.load(f)
                count = 0
                for link in BeautifulSoup(response, parse_only=SoupStrainer('a')):
                    count += 1
                    normalized_url = normalize_url(link)
                    if self.dictionary.contains_url(normalized_url):
                        associated_id = self.dictionary.find_doc_id_by_url(normalized_url)
                        links[self.dictionary.find_doc_id(file)][associated_id] += 1
                link_count[key] = count

        incoming_links = defaultdict(lambda: defaultdict(int))
        for key, value in links.items():
            for outgoing, count in value.items():
                incoming_links[outgoing][key] += count

        prev_weights = defaultdict(float)
        weights = defaultdict(float)
        for key in links:
            prev_weights[key] = 1

        for _ in range(self.iterations):
            for key in incoming_links:
                for incoming, count in incoming_links[key].items():
                    weights[key] += self.alpha * count / link_count[incoming] * prev_weights[incoming]
            for key in links:
                weights[key] += 1 - self.alpha
            prev_weights = weights
            weights = defaultdict(float)

        for key in weights:
            self.fixed_id[key] = weights[key]

    def flush(self):
        self.fixed_id.flush()

    def close(self):
        self.fixed_id.flush()
        self.fixed_id.close()
