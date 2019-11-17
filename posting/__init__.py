from typing import Dict, List, Any, Tuple


class PostingType(type):
    def __new__(mcs, map_properties: Dict[str, type], *args, **kwargs):
        sorted_list = list(sorted(map_properties))
        sorted_properties = map(lambda x: (x[1], (map_properties[x[1]], x[0])),
                                enumerate(sorted_list))
        reverse_map = [(x, map_properties[x]) for x in sorted_list]

        class Posting:
            SORTED_PROPERTIES: Dict[str, Tuple[type, int]] = dict(sorted_properties)
            REVERSE_MAP: List[Tuple[str, type]] = reverse_map

            def __init__(self, doc_id: str, properties: Dict[str, int or float] or [float or int] = None):
                self.doc_id = doc_id
                if type(properties) is dict():
                    temp_properties = [None] * len(Posting.SORTED_PROPERTIES)
                    for element in properties:
                        temp_properties[Posting.get_index(element)] = properties[element]
                    properties = temp_properties

                self.properties = properties if properties else [None] * len(Posting.SORTED_PROPERTIES)

            @staticmethod
            def get_index(key: str) -> int:
                return Posting.SORTED_PROPERTIES[key][1]

            @staticmethod
            def get_type(key: str) -> type:
                return Posting.SORTED_PROPERTIES[key][0]

            @staticmethod
            def get_key(index: int) -> str:
                return Posting.REVERSE_MAP[index][0]

            @staticmethod
            def parse(segments: [str or [str]]):
                return Posting(segments[0],
                               list(
                                   map(
                                       lambda x: Posting.REVERSE_MAP[x[0]][1](x[1]), enumerate(segments, 1)
                                   )
                               ))

            def __str__(self):
                property_string = '\t'.join([str(prop) for prop in self.properties])
                return f"{self.doc_id}\v{property_string}\f"

            def __eq__(self, other: "Posting"):
                if type(other) is not Posting:
                    return False
                return self.doc_id == other.doc_id

            def __lt__(self, other: "Posting"):
                if type(other) is not Posting:
                    return False
                return self.doc_id < other.doc_id

            def set_property(self, key: str, value: Any):
                self.properties[Posting.get_index(key)] = value

            def __setitem__(self, key: str, value: Any):
                self.set_property(key, value)

            def get_property(self, key: str):
                return self.properties[Posting.get_index(key)]

            def __getitem__(self, item: str):
                return self.get_property(item)

        return Posting