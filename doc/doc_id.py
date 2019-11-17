from collections import defaultdict


class DocumentIdDictionary:
    def __init__(self, name: str):
        self.name = name
        self.counter = 0
        # Map from file to a doc_id
        self.doc_id = dict()
        # Map from a doc_id to a file
        self.reverse_map = dict()
        self.properties = defaultdict(list)

    def generate_doc_id(self, file: str) -> str:
        """
        Generate a document ID and store the identifier in the dictionary
        :param file: the file to associate with the generated id
        :return: an integer representing the id number that can be used
        """
        counter = self.counter
        self.counter += 1
        self.doc_id[str] = counter
        self.reverse_map[counter] = file
        return str(counter)

    def find_doc_id(self, file: str):
        return self.doc_id[file]

    def add_document_property(self, file: str, prop):
        self.properties[file] = prop

    def flush(self):
        with open(f'{self.name}/doc_id.reference', 'w+') as file:
            with open(f'{self.name}/reverse_map.reference', 'w+') as reverse_file:
                for key in self.doc_id:
                    property_string = '\t' + '\t'.join(self.properties[key]) if key in self.properties else ""
                    file.write(f'{key}\t{self.doc_id[key]}{property_string}')
                    reverse_file.write(f'{self.doc_id[key]}\t{key}{property_string}')

    def close(self):
        self.flush()
