if __name__ == "__main__":
    with open("indexes/default/4.index", 'r') as index:
        with open("indexes/default/4.index.position", 'w+') as positions:
            pos = index.tell()
            line = index.readline()
            while line:
                split = line.split('\f')
                count = len(split) - 1
                word = split[0]

                positions.write(f"{word}\t{pos}\t{count}")
                line = index.readline()
                pos = index.tell()
