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

    def get_doc_id(self, file: str) -> int:
        counter = self.counter
        self.counter += 1
        self.doc_id[str] = counter
        self.reverse_map[counter] = file
        return counter

    def add_document_property(self, file: str, token):

    def flush(self):
        with open(f'{self.name}/doc_id.reference', 'w+') as file:
            for key in self.doc_id:
                file.write(f'{key}\t{self.doc_id[key]}')

        with open(f'{self.name}/reverse_map.reference', 'w+') as file:
            for key in self.reverse_map:
                file.write(f'{key}\t{self.reverse_map[key]}')

    def close(self):
        self.flush()
