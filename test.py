from posting.io import PostingReader, PostingWriter, merge
from posting.post import create_posting_type

if __name__ == "__main__":
    A = create_posting_type("default", {"count": int})
    reader = PostingReader("test/0", A)
    readerB = PostingReader("test/1", A)
    writer = PostingWriter("test/final")
    merge(writer, reader, readerB)

