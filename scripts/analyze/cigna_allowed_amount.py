# add proj path to import udfs 
import sys
sys.path.insert(0, '/Users/kylekent/Library/CloudStorage/Dropbox/tic')

import pandas as pd

# file path
file_path = 'data/cigna/allowed_amounts_processed.tsv'
# load data
allowed_amounts = pd.read_csv(file_path, sep = '\t')

# select only covid dna/rna test
covid = allowed_amounts[allowed_amounts['out_of_network.billing_code'] == 'U0003']

covid.to_csv('scripts/analyze/output/cigna_covid.tsv', 
        sep = '\t',
        index = False)

