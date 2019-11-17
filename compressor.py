import shelve
import pickle

COUNT = 0

HASH_TO_COUNT = dict()
COUNT_TO_HASH = dict()
# A map of the index file and the positions of the words
INDEX_OF_INDEX = dict()


def get_index(x):
    if x[0] in HASH_TO_COUNT:
        return HASH_TO_COUNT[x[0]]
    return 10 ** 9


def main():
    global COUNT
    with open('compressed.index', 'w+') as compressed:
        with shelve.open('index.shelve') as index:
            with open('keys2.pickle', 'rb+') as key_file:
                keys = pickle.load(key_file)
                for key in keys:
                    INDEX_OF_INDEX[key] = compressed.tell()
                    compressed.write(f"{key}")
                    for file_hash, occurrence in sorted(index[key].items(), key=get_index):
                        if file_hash not in HASH_TO_COUNT:
                            HASH_TO_COUNT[file_hash] = COUNT
                            COUNT_TO_HASH[COUNT] = file_hash
                            COUNT += 1
                        compressed.write(f"\t{HASH_TO_COUNT[file_hash]},{occurrence}")

                    compressed.write('\n')
                    compressed.flush()
                    print(key)

    print("Serializing")
    with open('seek.index', 'wb+') as seek:
        pickle.dump(INDEX_OF_INDEX, seek)

    with open('hash_count.index', 'wb+') as hash_count:
        pickle.dump(HASH_TO_COUNT, hash_count)

    with open('count_to_hash.index', 'wb+') as count_hash:
        pickle.dump(COUNT_TO_HASH, count_hash)


if __name__ == "__main__":
    main()
