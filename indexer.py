from posting.tokenizer import Tokenizer
from posting.tokenizer.ngram import WordTokenizer, BigramTokenizer, TrigramTokenizer
from posting.tokenizer.bold import BoldTokenizer
from posting.tokenizer.anchor import AnchorTokenizer, PositionalTokenizer
from doc import DocumentIdDictionary, normalize_url, from_document_dictionary
from posting import PostingDictionary, merge, PostingWriter, PostingReader
from posting.score import QueryScoringScheme
from fixed import FixedScoreDictionary
from fixed.page_rank import PageRank
from posting.score.tf_idf import TfIdfScoring
import glob
from multiprocessing import Pool

ROOT_DIR = "/home/lopes/Datasets/IR/DEV"


def merge_files(name: str, posting, query_scheme):
    writer = PostingWriter(f"{name}/finalized")
    total_count = len(glob.glob(f"{name}/partials/*.index"))
    merge(writer, *[PostingReader(f"{name}/partials/{file}", posting) for file in range(total_count)])
    writer.flush()
    writer.close()
    scheme(name, posting, query_scheme)


def scheme(name: str, posting, query_scheme):
    reader = PostingReader(f"{name}/finalized", posting)
    writer = PostingWriter(f"{name}/schemed")
    for key in reader.keys:
        reader.seek(key)
        writer.write_key(key)
        writer.write(*query_scheme.finalize_posting(reader.get_iterator()))
    writer.flush()
    writer.close()
    if type(query_scheme) is TfIdfScoring:
        champion(name, posting)


def champion(name: str, posting):
    writer = PostingWriter(f"{name}/champion")
    reader = PostingReader(f"{name}/schemed", posting)
    for key in sorted(reader.keys.keys()):
        writer.write_key(key)
        writer.write(*sorted(list(reader.get_iterator()),
                             key=lambda x: x.get_property("tf_idf"),
                             reverse=True))
    writer.flush()
    writer.close()


def processor(name: str, identifier: DocumentIdDictionary, tokenizer: Tokenizer, query_scheme: QueryScoringScheme):
    postings = PostingDictionary(name)
    identifier.set_name(name)
    encountered = set()
    posting = query_scheme.get_posting_type()

    for document in glob.glob(f"{ROOT_DIR}/**/*"):
        token_result = tokenizer.tokenize(document)
        if not token_result:
            continue
        if len(postings) > 200000:
            postings.flush()
        print(token_result.url)

        if normalize_url(token_result.url) in encountered:
            continue
        encountered.add(normalize_url(token_result.url))
        token_result.url = normalize_url(token_result.url)

        doc_id = identifier.generate_doc_id(document, normalize_url(token_result.url))
        posts = query_scheme.create_posting(document, token_result)
        for token, post in posts:
            postings.add_posting(token, post)

        token_result.properties['count'] = token_result.total_count
        identifier.add_document_property(doc_id, token_result.properties)

    identifier.flush()
    postings.flush()
    identifier.close()
    merge_files(name, posting, query_scheme)


def tf_idf_processor(name: str, tokenizer: Tokenizer):
    dictionary = DocumentIdDictionary(name, ["count"])
    scheme = TfIdfScoring(dictionary, None)
    processor(name, dictionary, tokenizer, scheme)


def page_rank(name: str):
    dictionary = FixedScoreDictionary(name)
    ranker = PageRank(from_document_dictionary("indexes/default"), dictionary)
    ranker.process()
    ranker.flush()


TOKENIZER_LIST = {("indexes/default", WordTokenizer()), ("indexes/bigram", BigramTokenizer()),
                  ("indexes/trigram", TrigramTokenizer())}
OTHER_TOKENIZER_LIST = {("indexes/position", PositionalTokenizer), ("indexes/anchor", AnchorTokenizer)}

RUN_CONFIG = 1

if __name__ == "__main__":
    if RUN_CONFIG == 0:
        with Pool(len(TOKENIZER_LIST)) as pool:
            pool.starmap(tf_idf_processor, TOKENIZER_LIST)
    elif RUN_CONFIG == 1:
        page_rank("indexes/page_rank")
    elif RUN_CONFIG == 2:
        tf_idf_processor("indexes/bold", BoldTokenizer())
    else:
        pass
