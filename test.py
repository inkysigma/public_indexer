from posting.io import PostingReader
from threading import RLock
from posting.post import create_posting_type

if __name__ == "__main__":

    A = create_posting_type("default", {"key": int, "other": int})
    reader = PostingReader("test/test2", A)
    for i in range(len(reader.keys)):
        print("Key:")
        print(reader.current_row())
        for it in reader.get_iterator():
            print(it.doc_id)
        reader.read_key()
