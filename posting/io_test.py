import unittest
import os
from .io import PostingReader, PostingWriter, intersect, merge
from .post import create_posting_type
import glob

TEST_POSTING = create_posting_type("test_type", {"count": int, "property": int})


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


class MergeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        writer = PostingWriter("merge.test")
        writer.write_key("hello a")
        writer.write_posting(TEST_POSTING(2, [2, 1]))
        writer.write_posting(TEST_POSTING(4, [1, 3]))
        writer.write_key("hello b")
        writer.write_posting(TEST_POSTING(2, [3, 2]))
        writer.write_posting(TEST_POSTING(7, [1, 2]))
        writer.flush()
        writer.close()

        writer = PostingWriter("merge2.test")
        writer.write_key("hello a")
        writer.write_posting(TEST_POSTING(3, [3, 2]))
        writer.write_posting(TEST_POSTING(7, [2, 7]))

        writer.write_key("hello b")
        writer.write_posting(TEST_POSTING(3, [4, 3]))
        writer.write_posting(TEST_POSTING(5, [10, 6]))
        writer.write_posting(TEST_POSTING(10, [2, 1]))

        writer.write_key("hello c")
        writer.write_posting(TEST_POSTING(1, [4, 6]))
        writer.flush()
        writer.close()

        writer = PostingWriter("merge3.test")
        writer.write_key("hello b")
        writer.write_posting(TEST_POSTING(5, [4, 3]))
        writer.write_posting(TEST_POSTING(11, [10, 6]))
        writer.write_posting(TEST_POSTING(12, [2, 1]))

        writer.write_key("hello c")
        writer.write_posting(TEST_POSTING(2, [4, 6]))
        writer.flush()
        writer.close()

    def setUp(self) -> None:
        self.reader = PostingReader("merge.test", TEST_POSTING)
        self.second_reader = PostingReader("merge2.test", TEST_POSTING)
        self.third_reader = PostingReader("merge3.test", TEST_POSTING)
        self.writer = PostingWriter("merge.test.out")

    def test_merge(self):
        merge(self.writer, self.reader, self.second_reader)
        self.writer.flush()
        self.writer.close()
        reader = PostingReader("merge.test.out", TEST_POSTING)
        self.assertEqual(len(reader.keys), 3)
        reader.seek("hello a")
        elements = list(reader.get_iterator())
        self.assertEqual(len(elements), 4)
        self.assertEqual(elements[0].doc_id, 2)
        self.assertEqual(elements[1].doc_id, 3)
        self.assertEqual(elements[2].doc_id, 4)
        self.assertEqual(elements[3].doc_id, 7)
        reader.seek("hello b")
        self.assertEqual(len(list(reader.get_iterator())), 5)
        reader.seek("hello c")
        self.assertEqual(len(list(reader.get_iterator())), 1)
        reader.close()

    def test_complex_merge(self):
        merge(self.writer, self.reader, self.second_reader, self.third_reader)
        self.writer.flush()
        self.writer.close()
        reader = PostingReader("merge.test.out", TEST_POSTING)
        self.assertEqual(len(reader.keys), 3)
        reader.seek("hello a")
        elements = list(reader.get_iterator())
        self.assertEqual(len(elements), 4)
        self.assertEqual(elements[0].doc_id, 2)
        self.assertEqual(elements[1].doc_id, 3)
        self.assertEqual(elements[2].doc_id, 4)
        self.assertEqual(elements[3].doc_id, 7)
        reader.seek("hello b")
        self.assertEqual(len(list(reader.get_iterator())), 8)
        reader.seek("hello c")
        self.assertEqual(len(list(reader.get_iterator())), 2)
        reader.close()

    def tearDown(self) -> None:
        self.writer.close()
        self.reader.close()
        self.second_reader.close()
        self.third_reader.close()
        for g in glob.glob("merge.test.out.*"):
            os.remove(g)

    @classmethod
    def tearDownClass(cls) -> None:
        for g in glob.glob("merge.test.*"):
            os.remove(g)


if __name__ == '__main__':
    unittest.main()
