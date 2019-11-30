from collections import defaultdict
from typing import Dict
import csv


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


class DocumentIdDictionary:
    def __init__(self, name: str, property_index: [str]):
        self.name = name
        self.counter = 0

        # Map from file to a doc_id
        self.doc_id = dict()
        self.reverse_map = dict()
        self.url_map = dict()

        self.document_properties = defaultdict(dict)
        self.property_index = ["doc_id", "file", "url", *property_index]

    def generate_doc_id(self, file: str, url: str) -> str:
        """
        Generate a document ID and store the identifier in the dictionary
        :param url:
        :param file: the file to associate with the generated id
        :return: an integer representing the id number that can be used
        """
        if file in self.doc_id:
            return self.doc_id[file]
        counter = self.counter
        self.counter += 1
        t = [counter, file, url]
        self.doc_id[file] = t
        self.reverse_map[counter] = t
        self.url_map[url] = t
        return str(counter)

    def find_doc_id(self, file: str):
        return str(self.doc_id[file][0])

    def get_doc_file(self, doc_id: int) -> str:
        return self.reverse_map[doc_id][1]

    def find_url_by_file(self, file: str):
        return self.reverse_map[file][2]

    def find_url_by_id(self, doc_id: int) -> str:
        return self.doc_id[doc_id][2]

    def __getitem__(self, item):
        if type(item) is int:
            return self.reverse_map[item]
        else:
            return self.doc_id[item]

    def __setitem__(self, key, value):
        if type(key) is str:
            self.doc_id[key] = value
            self.reverse_map[value] = key
        else:
            self.reverse_map[key] = value
            self.doc_id[value] = key

    def __len__(self):
        return len(self.doc_id)

    def add_document_property(self, file: str, prop: Dict[str, int or float]):
        self.document_properties[file].update(prop)

    def get_document_property(self, key: str or int, prop: str):
        if type(key) is int:
            return self.document_properties[self.reverse_map[key]][prop]
        return self.document_properties[key][prop]

    def flush(self):
        with open(f'{self.name}/doc_id.reference', 'w+') as file:
            with open(f'{self.name}/reverse_map.reference', 'w+') as reverse_file:
                file.write('\t'.join(self.property_index) + '\n')
                reverse_file.write('\t'.join(self.property_index) + '\n')
                for key in self.doc_id:
                    property_string = '\t' + '\t'.join(
                        [f'{k}\f{v}' for k, v in self.properties[key].items()]) if key in self.properties else ""
                    file.write(f'{key}\t{self.doc_id[key]}{property_string}\n')
                    reverse_file.write(f'{self.doc_id[key]}\t{key}{property_string}\n')

    def close(self):
        self.flush()
