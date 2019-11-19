from collections import defaultdict
from typing import Dict


def from_document_dictionary(name):
    dictionary = DocumentIdDictionary(name)
    with open(f'{name}/doc_id.reference') as reference:
        for row in reference:
            key, value, *properties = row.split('\t')
            dictionary[key] = int(value)
            property_map = dict()
            for prop in properties:
                name, target = prop.split('\f')
                property_map[name] = target
            dictionary.add_document_property(key, property_map)
    return dictionary


class DocumentIdDictionary:
    def __init__(self, name: str):
        self.name = name
        self.counter = 0
        # Map from file to a doc_id
        self.doc_id = dict()
        # Map from a doc_id to a file
        self.reverse_map = dict()
        self.properties = defaultdict(dict)

    def generate_doc_id(self, file: str) -> str:
        """
        Generate a document ID and store the identifier in the dictionary
        :param file: the file to associate with the generated id
        :return: an integer representing the id number that can be used
        """
        if file in self.doc_id:
            return self.doc_id[file]
        counter = self.counter
        self.counter += 1
        self.doc_id[file] = counter
        self.reverse_map[counter] = file
        return str(counter)

    def find_doc_id(self, file: str):
        return self.doc_id[file]

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
        self.properties[file] = prop

    def get_document_property(self, key: str or int, prop: str):
        if type(key) is int:
            return self.properties[self.reverse_map[key]][prop]
        return self.properties[key][prop]

    def flush(self):
        with open(f'{self.name}/doc_id.reference', 'w+') as file:
            with open(f'{self.name}/reverse_map.reference', 'w+') as reverse_file:
                for key in self.doc_id:
                    property_string = '\t' + '\t'.join(
                        [f'{k}\f{v}' for k, v in self.properties[key].items()]) if key in self.properties else ""
                    file.write(f'{key}\t{self.doc_id[key]}{property_string}\n')
                    reverse_file.write(f'{self.doc_id[key]}\t{key}{property_string}\n')

    def close(self):
        self.flush()
