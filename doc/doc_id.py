from collections import defaultdict
from typing import Dict
import csv
from urllib.parse import urldefrag
import os


def from_document_dictionary(name):
    with open(f'{name}/doc_id.reference') as reference:
        reader = csv.DictReader(reference)
        fields = reader.fieldnames[3:]
        dictionary = DocumentIdDictionary(name, fields)
        for row in reader.reader:
            key, value, *properties = row.split('\t')
            dictionary[key] = int(value)
            property_map = dict()
            for prop in properties:
                name, target = prop.split('\f')
                property_map[name] = target
            dictionary.add_document_property(key, property_map)
    return dictionary


def normalize_url(url: str):
    return urldefrag(url)[0].rstrip()


class DocumentIdDictionary:
    def __init__(self, name: str, property_index: [str]):
        self.name = name
        self.counter = 0

        # Map from file to a doc_id
        self.doc_id = dict()
        self.reverse_map = dict()
        self.url_map = dict()

        self.property_index = ["doc_id", "file", "url", *property_index]
        self.property_map = {key: index for index, key in enumerate(self.property_index)}

    def set_name(self, name: str):
        self.name = name

    def generate_doc_id(self, file: str, url: str) -> int:
        """
        Generate a document ID and store the identifier in the dictionary
        :param url:
        :param file: the file to associate with the generated id
        :return: an integer representing the id number that can be used
        """
        if file in self.doc_id:
            return self.doc_id[file][0]
        counter = self.counter
        self.counter += 1
        t = [counter, file, url]
        t.extend([None] * len(self.property_map))
        self.doc_id[file] = t
        self.reverse_map[counter] = t
        self.url_map[url] = t
        return counter

    def find_doc_id(self, file: str) -> int:
        return self.doc_id[file][0]

    def find_doc_id_by_url(self, url: str) -> int:
        return self.url_map[url][0]

    def get_doc_file(self, doc_id: int) -> str:
        return self.reverse_map[doc_id][1]

    def find_url_by_file(self, file: str):
        return self.reverse_map[file][2]

    def find_url_by_id(self, doc_id: int) -> str:
        return self.doc_id[doc_id][2]

    def __getitem__(self, item):
        if type(item) is int:
            return self.reverse_map[item][0]
        else:
            return self.doc_id[item][1]

    def __setitem__(self, key, value):
        if type(key) is str:
            self.doc_id[key][0] = value
            self.reverse_map[value] = key
        else:
            self.reverse_map[key] = value
            self.doc_id[value] = key

    def __len__(self):
        return len(self.doc_id)

    def add_document_property(self, doc_id: str, properties: Dict[str, int or float]):
        for prop in properties:
            self.doc_id[doc_id][self.property_map[prop]] = properties[prop]

    def get_document_property(self, key: int, prop: str):
        return self.doc_id[key][self.property_map[prop]]

    def contains_url(self, url: str):
        return url in self.url_map

    def flush(self):
        if not os.path.exists(f"{self.name}"):
            os.mkdir(f"{self.name}")
        with open(f'{self.name}/doc_id.reference', 'w+') as file:
            writer = csv.DictWriter(file, self.property_index)
            writer.writeheader()
            for key in self.doc_id:
                row = {element: self.doc_id[key][element] for element in self.property_index}
                writer.writerow(row)

    def close(self):
        self.flush()
