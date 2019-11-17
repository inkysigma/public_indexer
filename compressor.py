import shelve
import pickle
from itertools import zip_longest

COUNT = 0

HASH_TO_COUNT = dict()
COUNT_TO_HASH = dict()
# A map of the index file and the positions of the words
INDEX_OF_INDEX = dict()
posting_list=dict()
def read_posting(segment: str):
	a = segment.split(",")
	return {"doc_id": a[0], "frequency": a[1]};

def calcTF(tokens):
    list = {}

    #counting how many each word occurs in the text
    for word in tokens:
        list[word] = list.get(word, 0) + 1

    tfDictonary = {}  #dictionary for TF values

    num_of_words = len(tokens) #total number of words in the page(or tokens)

    #for each word, calculate the TF value.
    for word, count in list.items():
        tfDictonary[word] = count/float(num_of_words)

    return tfDictonary
def query():
    with open("compressed.index", "rb") as f:
    	query= raw_input("Please enter a query").lower()
   	words= query.split()
	posting_sequences = []
    	for term in words:
        	lower_term = term.lower()
        	with open('seek.index','rb+') as where_to:
            		keys = pickle.load(where_to)
	    		f.seek(keys[lower_term])
			postings = f.readline()
		        posting_sequences.append([read_posting(segment) for segment in postings.split("\t")[1:]])

	
	common_documents = dict()
	indexes = [0] * len(posting_sequences)
	running = True
	while running:
		maxmimum_document_id = None
		full_intersect = True
		for i, sequence in enumerate(posting_sequences):
			current_head = posting_sequences[i][indexes[i]]
			if maximum_document_id is None:
				maximum_document_id = int(current_head["doc_id"])
			if int(current_head["doc_id"]) != maximum_document_id:
				full_intersect = False
				maximum_document_id = max(int(current_head["doc_id"]), maximum_document_id)
		if full_intersect:
			common_documents[maximum_document_id] = [posting_sequences[i][indexes[i]] for i in range(len(posting_sequences))]
		else:
			for i, sequence in enumerate(posting_sequeqnces):
				while posting_sequences[i][indexes[i]] < maximum_document_id and indexes[i] < len(posting_sequences[i]):
					indexes[i] += 1
				if indexes[i] == len(posting_sequences[i]):
					running = False
					break
		
		
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
