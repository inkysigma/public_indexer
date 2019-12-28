import json
import os
import pickle
from collections import defaultdict
from urllib.parse import urljoin

from bs4 import BeautifulSoup

import fixed
from doc import DocumentIdDictionary, normalize_url


class PageRank(fixed.FixedScorer):
    def __init__(self, dictionary: DocumentIdDictionary, fixed_id: fixed.FixedScoreDictionary, alpha=0.85):
        self.dictionary = dictionary
        self.fixed_id = fixed_id
        self.iterations = 20
        self.alpha = 0.85

    def process(self):
        """
        There are two phases to this processing. We first need to find the connections and store them in a dictionary.
        The dictionary should map each id to the set of ids they link to. Finally, we need to store the settled values
        into the file.
        """
        if os.path.exists("indexes/page_rank/graph"):
            links = pickle.load(open('indexes/page_rank/graph', 'rb'))
            link_count = pickle.load(open('indexes/page_rank/link_count', 'rb'))
        else:
            links = defaultdict(lambda: defaultdict(int))
            link_count = defaultdict(int)
            for key, value in self.dictionary.reverse_map.items():
                file = value[1]
                with open(file, 'r') as f:
                    response = json.load(f)
                    count = 0
                    doc = BeautifulSoup(response['content'], from_encoding=response['encoding'], features='lxml')
                    for element in doc.find_all('a', href=True):
                        if not element.has_attr('href'):
                            continue
                        link = element['href']
                        normalized_url = normalize_url(link)
                        secondary_check = normalize_url(urljoin(response['url'], link, allow_fragments=False))
                        if self.dictionary.contains_url(normalized_url):
                            count += 1
                            associated_id = self.dictionary.find_doc_id_by_url(normalized_url)
                            links[self.dictionary.find_doc_id(file)][associated_id] += 1
                        if self.dictionary.contains_url(secondary_check):
                            count += 1
                            associated_id = self.dictionary.find_doc_id_by_url(secondary_check)
                            links[self.dictionary.find_doc_id(file)][associated_id] += 1
                    link_count[key] = count
                    print(response['url'], link_count[key])

            with open(f'indexes/page_rank/graph', 'wb+') as g:
                pickle.dump(dict(links), g)
            with open(f'indexes/page_rank/link_count', 'wb+') as lc:
                pickle.dump(dict(link_count), lc)

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
                    weights[key] += self.alpha * 1 / len(links[incoming]) * prev_weights[incoming]
            for key in links:
                weights[key] += 1 - self.alpha
            prev_weights = weights
            weights = defaultdict(float)

        for key in prev_weights:
            self.fixed_id[key] = prev_weights[key]

    def flush(self):
        self.fixed_id.flush()

    def close(self):
        self.fixed_id.flush()
        self.fixed_id.close()
