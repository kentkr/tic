# add proj path to import udfs 
import sys
sys.path.insert(0, '/Users/kylekent/Library/CloudStorage/Dropbox/tic')
from utils.process import combine_jsons, recursive_expand
#from utils.download import download_from_urls, convert_file_size, extract_gzip

import urllib
import requests
import gzip
import shutil
import io

from alive_progress import alive_bar

gz_file = '/Users/kylekent/Downloads/2022-12-01_cigna-health-life-insurance-company_national-oap_in-network-rates (1).json.gz'
tmp_file = '/Users/kylekent/Downloads/tmp.json'

# Open the gzip file in binary mode and the output file in write mode
with gzip.open(gz_file, 'rb') as infile, open(tmp_file, 'w') as outfile:
    # Read the gzip file line by line and write each line to the output file
    for line in infile:
        print(type(line.decode()))
        outfile.write(line.decode())


