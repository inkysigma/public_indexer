import unittest
import os
from .io import PostingReader, PostingWriter, intersect
from .post import create_posting_type
import glob


class WritingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.writer = PostingWriter("writer.test")
        self.posting = create_posting_type("test_type", {"count": int})

    def test_writer(self):
        self.writer.write_key("hello")
        self.writer.write_key("another")

    def test_write_posting(self):
        self.writer.write_key("hello")
        self.writer.write_posting(self.posting(1, [2]))
        self.writer.write_posting(self.posting(2, [3]))
        self.writer.write_posting(self.posting(3, [1]))
        self.writer.write_key("hello 2")
        self.writer.write_posting(self.posting(2, [3]))

    def tearDown(self) -> None:
        self.writer.flush()
        self.writer.close()
        for g in glob.glob("writer.test.*"):
            os.remove(g)


class ReaderWriterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        writer = PostingWriter("reader.test")
        posting = create_posting_type("test_type", {"count": int, "property": int})
        writer.write_key("hello")
        writer.write_posting(posting(1, [2, 1]))
        writer.write_posting(posting(2, [3, 2]))
        writer.write_posting(posting(3, [1, 3]))
        writer.write_key("hello 2")
        writer.write_posting(posting(2, [3, 2]))
        writer.write_posting(posting(3, [4, 3]))
        writer.write_posting(posting(6, [10, 6]))
        writer.flush()
        writer.close()

    def setUp(self) -> None:
        posting = create_posting_type("test_type", {"count": int, "property": int})
        self.reader = PostingReader("reader.test", posting)

    def test_count(self):
        self.assertEqual(self.reader.count("hello"), 3)
        self.assertEqual(self.reader.count("hello 2"), 3)

    def test_iterator(self):
        self.reader.seek("hello")
        hello = list(self.reader.get_iterator())
        self.assertEqual(hello[0].doc_id, 1)
        self.assertEqual(hello[0].get_property("count"), 2)
        self.assertEqual(hello[1].doc_id, 2)
        self.assertEqual(hello[1].get_property("count"), 3)
        self.assertEqual(hello[2].doc_id, 3)
        self.assertEqual(hello[2].get_property("count"), 1)

    def tearDown(self) -> None:
        self.reader.close()

    @classmethod
    def tearDownClass(cls) -> None:
        for g in glob.glob("reader.test.*"):
            os.remove(g)


class IntersectTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        writer = PostingWriter("intersect.test")
        posting = create_posting_type("test_type", {"count": int, "property": int})
        writer.write_key("hello a")
        writer.write_posting(posting(2, [2, 1]))
        writer.write_posting(posting(3, [3, 2]))
        writer.write_posting(posting(4, [1, 3]))
        writer.write_posting(posting(7, [2, 7]))
        writer.write_posting(posting(10, [2, 1]))
        writer.write_key("hello b")
        writer.write_posting(posting(2, [3, 2]))
        writer.write_posting(posting(3, [4, 3]))
        writer.write_posting(posting(5, [10, 6]))
        writer.write_posting(posting(7, [1, 2]))
        writer.flush()
        writer.close()

    def setUp(self) -> None:
        posting = create_posting_type("test_type", {"count": int, "property": int})
        self.reader = PostingReader("intersect.test", posting)

    def create_iterators(self):
        iterators = []
        self.reader.seek("hello a")
        iterators.append(self.reader.get_iterator())
        self.reader.seek("hello b")
        iterators.append(self.reader.get_iterator())
        return iterators

    def test_basic(self):
        intersect(*self.create_iterators())

    def test_iterator(self):
        postings = list(intersect(*self.create_iterators()))
        self.assertEqual(len(postings), 3)
        self.assertEqual(postings[0].get_properties("count")[0], 2)
        self.assertEqual(postings[0].get_properties("count")[1], 3)
        self.assertEqual(postings[0].get_properties("property")[0], 1)
        self.assertEqual(postings[1].get_properties("count")[0], 3)

    def tearDown(self) -> None:
        self.reader.close()

    @classmethod
    def tearDownClass(cls) -> None:
        for g in glob.glob("reader.test.*"):
            os.remove(g)


if __name__ == '__main__':
    unittest.main()
