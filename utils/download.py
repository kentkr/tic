import pandas as pd
import urllib
import requests
import os
import json
import zipfile
import io
import gzip
import math
from tqdm import tqdm

def download_from_urls(url_series, folder_path):
    # original url length
    orig_len = url_series.size
    # dedpulicate urls
    url_series = url_series.drop_duplicates()
    new_len = url_series.size
    print('Dropped {} duplicates'.format(orig_len-new_len))
    # for each url
    for url_index, url in url_series.iteritems():
        # if url is null skp
        if pd.isnull(url):
            print('skipping')
            continue
        # get url file path
        base_path = urllib.parse.urlparse(url)
        # get file name
        file_name = os.path.basename(base_path.path)
        # get extension
        extension = os.path.splitext(file_name)[1]

        # try to connect to url
        try:
            response = urllib.request.urlopen(url)
        except urllib.error.HTTPError as err:
            print(err)
            print('For a {} file at index {} and url:\n{}'.format(extension, url_index, url))
            continue
        
        # get file size
        content_size = int(response.info()['Content-Length'])
        file_size = convert_file_size(content_size)
        print('File size: {}'.format(file_size))

        print('opening file')
        print(extension)
        if extension == '.gz':
            removed_extension= os.path.splitext(file_name)[0]
            added_prefix = '{}_'.format(url_index) + removed_extension
            extract_gzip(response, added_prefix, folder_path, file_size)    

        elif extension == '.zip':
            extract_zip(response, folder_path, url_index)

        # if json
        else:
            added_prefix = '{}_'.format(url_index) + file_name
            extract_json(response, added_prefix, folder_path)

def extract_json(response, file_name, folder_path):
    # convert bytes to json
    jsoned = json.loads(response.read())
    # get file path
    write_path = os.path.join(folder_path, file_name)
    # open file
    with open(write_path, 'w') as out_file:
        # stream json
        json.dump(jsoned, 
                out_file,
                indent = 4)

def extract_gzip(response, file_name, folder_path, file_size):
    # get file path
    file_path = os.path.join(folder_path, file_name)
    # read object in read mode
    file = gzip.GzipFile(fileobj = response, mode = 'r')
    # stream and decompress each line of the file
    with open(file_path, 'w') as f:
        for line in tqdm(file):
            f.write(line.decode())

# fun to unzip files
def extract_zip(response, folder_path, url_index):
    # convert from bytes to unzipped file
    unzipped = zipfile.ZipFile(io.BytesIO(response.read()))
    file_information = unzipped.infolist()
    # for each file in unzipped
    for file in file_information:
        file.filename = '{}_'.format(url_index) + file.filename
        unzipped.extract(file, path = folder_path)

def convert_file_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

### 
# make gzip more efficient
