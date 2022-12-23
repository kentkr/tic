# add proj path to import udfs 
import sys
sys.path.insert(0, '/Users/kylekent/Library/CloudStorage/Dropbox/tic')
from utils.process import combine_jsons, recursive_expand

# open files in dir
dir_path = 'data/cigna/allowed_amount_files'
# normalize and combine each json as df
allowed_amounts = combine_jsons(dir_path)

# expand and write
expanded = recursive_expand(allowed_amounts)
expanded.to_csv('data/cigna/allowed_amounts_processed.tsv',
        sep = '\t',
        index = False)

