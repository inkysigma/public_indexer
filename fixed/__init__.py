class FixedScoreDictionary:
    def __init__(self, file_name: str):
        self.file = open('file_name', 'rw+')
        self.keys = dict()
        for row in self.file:
            key, value = row.split(',')
            value = float(value)
            self.keys[key] = value

    def __setitem__(self, key: int, value: float):
        self.keys[key] = value

    def __getitem__(self, item: int):
        return self.keys[item]

    def flush(self):
        self.file.seek(0)
        self.file.writelines([f"{key},{value}" for key, value in self.keys.items()])
        self.file.flush()

    def close(self):
        self.file.close()


class FixedScorer:
    def process(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError
