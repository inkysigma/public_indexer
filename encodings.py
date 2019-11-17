import json
import glob

ROOT_DIR = "/home/lopes/Datasets/IR/DEV"
ENCODINGS = set()

for file in glob.glob(ROOT_DIR + "/**/*.json"):
    with open(file, 'r') as f:
        obj = json.load(f)
        if obj['encoding'].lower() in ENCODINGS:
            continue
        print(obj['encoding'])
        ENCODINGS.add(obj['encoding'].lower())

print()
print(ENCODINGS)
