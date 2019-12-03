from posting.tokenizer import Tokenizer
from posting.tokenizer.ngram import WordTokenizer, BigramTokenizer, TrigramTokenizer
from doc import DocumentIdDictionary, normalize_url
from posting import PostingDictionary, merge, PostingWriter, PostingReader
from posting.score import QueryScoringScheme
from posting.score.tf_idf import TfIdfScoring
import glob
from multiprocessing import Pool

ROOT_DIR = "/home/lopes/Datasets/IR/DEV"


def merge_files(name: str, posting, scheme):
    writer = PostingWriter(f"{name}/finalized")
    total_count = len(glob.glob(f"{name}/partials/*")) // 2
    merge(writer, [PostingReader(f"{name}/partials/{file}", posting) for file in range(total_count)])
    writer.flush()
    writer.close()

    reader = PostingReader(f"{name}/finalized", posting)
    writer = PostingWriter(f"{name}/schemed")
    for key in reader.keys:
        reader.seek(key)
        writer.write(*scheme.finalize_posting(reader.get_iterator()))
    writer.flush()
    writer.close()


def processor(name: str, identifier: DocumentIdDictionary, tokenizer: Tokenizer, scheme: QueryScoringScheme):
    postings = PostingDictionary(name)
    identifier.set_name(name)
    encountered = set()
    posting = scheme.get_posting_type()

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
        posts = scheme.create_posting(document, token_result)
        for token, post in posts:
            postings.add_posting(token, post)
        identifier.add_document_property(document, token_result.properties)

    identifier.flush()
    postings.flush()
    identifier.close()
    merge_files(name, posting, scheme)


def tf_idf_processor(name: str, tokenizer: Tokenizer):
    dictionary = DocumentIdDictionary(name, ["count"])
    scheme = TfIdfScoring(dictionary, None)
    processor(name, dictionary, tokenizer, scheme)


TOKENIZERS = {("default", WordTokenizer()), ("bigram", BigramTokenizer()),
              ("trigram", TrigramTokenizer())}

if __name__ == "__main__":
    with Pool(len(TOKENIZERS)) as pool:
        pool.starmap(tf_idf_processor, TOKENIZERS)
