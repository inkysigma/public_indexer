from tokenizer import Tokenizer
from tokenizer.ngram import WordTokenizer
from doc import DocumentIdDictionary
from posting import PostingDictionary, Posting
import glob

ROOT_DIR = "/home/lopes/Datasets/IR/DEV"


def processor(name: str, tokenizer: Tokenizer):
    identifier = DocumentIdDictionary(name)
    postings = PostingDictionary(name)
    posting = tokenizer.get_posting_type()

    for document in glob.glob(f"{ROOT_DIR}/**/*"):
        token_result = tokenizer.tokenize(document)
        if not token_result:
            continue
        if len(postings) > 100000:
            postings.flush()
        print(document)
        doc_id = identifier.generate_doc_id(document)
        for token, *props in token_result.tokens:
            properties = {"count": props[0]}
            if len(properties) > 1:
                properties.update(properties[1])
            post = posting(doc_id, properties)
            postings.add_posting(token, post)

        identifier.add_document_property(document, token_result.properties)


if __name__ == "__main__":
    processor("default", WordTokenizer())
