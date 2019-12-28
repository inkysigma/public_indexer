import os


class FixedScoreDictionary:
    def __init__(self, file_name: str, read=False):
        if not os.path.exists(f'{file_name}'):
            os.mkdir(f'{file_name}')
        self.keys = dict()
        if os.path.exists(f'{file_name}/fixed.reference'):
            read = open(f'{file_name}/fixed.reference', 'r')
            for row in read:
                key, value = row.split(',')
                value = float(value)
                self.keys[key] = value
            read.close()
        if not read:
            self.file = open(f'{file_name}/fixed.reference', 'w+')

    def __setitem__(self, key: int, value: float):
        self.keys[key] = value

    def __getitem__(self, item: int):
        return self.keys[item]

    def flush(self):
        self.file.seek(0)
        self.file.writelines([f"{key},{value}\n" for key, value in self.keys.items()])
        self.file.flush()

    def close(self):
        self.file.close()


class FixedScorer:
    def process(self):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError
