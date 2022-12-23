# add proj path to import udfs 
import sys
sys.path.insert(0, '/Users/kylekent/Library/CloudStorage/Dropbox/tic')

import pandas as pd
from flatten_json import flatten_json
from utils.process import recursive_expand

# open index file
with open('data/cigna/2022-12-01_cigna-health-life-insurance-company_index.json') as file:
    index_file = pd.read_json(file)

# expand json
expanded_file = recursive_expand(index_file)
print(expanded_file)

# write 
expanded_file.to_csv('/Users/kylekent/Library/CloudStorage/Dropbox/tic/data/cigna/expanded_index_file.tsv', 
        sep = '\t')

