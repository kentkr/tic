import urllib
import requests
import pandas as pd
import sys

# add proj path to import udfs 
sys.path.insert(0, '/Users/kylekent/Library/CloudStorage/Dropbox/tic')
from utils.process import recursive_expand
from utils.download import download_from_urls

# load expanded index file
with open('/Users/kylekent/Library/CloudStorage/Dropbox/tic/data/cigna/expanded_index_file.tsv') as file:
    index_file = pd.read_csv(file, 
            sep = '\t')

# extract allowed amount files
folder_path = '/Users/kylekent/Library/CloudStorage/Dropbox/tic/data/cigna/in_network_files'
#url_series = index_file['reporting_structure.in_network_files.location'][list(range(0, 100))]
url_series = index_file['reporting_structure.in_network_files.location'][range(15, 16)]
print(url_series)

download_from_urls(url_series, folder_path)
