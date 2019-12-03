from typing import List, Dict, Tuple, Any, Type


class Posting:
    SORTED_PROPERTIES: Dict[str, Tuple[type, int]] = None
    REVERSE_MAP: List[Tuple[str, type]] = None

    def __init__(self, doc_id: int, properties: Dict[str, int or float] or [float or int] = None):
        self.doc_id = int(doc_id)

        if type(properties) is dict:
            temp_properties = [None] * len(type(self).SORTED_PROPERTIES)
            for element in properties:
                temp_properties[self.get_index(element)] = properties[element]
            properties = temp_properties

        self.properties = properties if properties else [None] * len(type(self).SORTED_PROPERTIES)

    def get_index(self, key: str) -> int:
        return type(self).SORTED_PROPERTIES[key][1]

    def get_type(self, key: str) -> type:
        return type(self).SORTED_PROPERTIES[key][0]

    def get_key(self, index: int) -> str:
        return type(self).REVERSE_MAP[index][0]

    @classmethod
    def parse(cls, segment: str):
        segments = segment.split('\v')
        return cls(int(segments[0]),
                   [cls.REVERSE_MAP[idx][1](token) for idx, token in enumerate(segments[1].split("\t"))])

    def __str__(self):
        property_string = '\t'.join([str(prop) for prop in self.properties])
        return f"{self.doc_id}\v{property_string}"

    def __eq__(self, other: "Posting"):
        if type(other) is not type(self):
            raise TypeError
        return self.doc_id == other.doc_id

    def __lt__(self, other: "Posting"):
        if type(other) is not type(self):
            raise TypeError
        return self.doc_id < other.doc_id

    def set_property(self, key: str, value: Any):
        self.properties[self.get_index(key)] = value

    def get_property(self, key: str):
        return self.properties[self.get_index(key)]


class IntersectPosting:
    def __init__(self, *postings: Posting):
        self.doc_id = postings[0].doc_id
        self.postings = postings

    def get_properties(self, key: str):
        return [posting.get_property(key) for posting in self.postings]


# noinspection PyTypeChecker
def create_posting_type(name: str, map_properties: Dict[str, type]) -> Type[Posting]:
    posting: Type[Posting] = type(name, (Posting,), {})
    sorted_list = list(sorted(map_properties))
    posting.SORTED_PROPERTIES = {key: (map_properties[key], idx) for idx, key in enumerate(sorted_list)}
    posting.REVERSE_MAP = [(x, map_properties[x]) for x in sorted_list]
    return posting
