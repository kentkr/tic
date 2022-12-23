import pandas as pd
import urllib
import requests
import os
import json
import zipfile
import io
import gzip
import math
from alive_progress import alive_bar

def download_from_urls(url_series, folder_path):
    # original url length
    orig_len = url_series.size
    # dedpulicate urls
    url_series = url_series.drop_duplicates()
    new_len = url_series.size
    print('Dropped {} duplicates'.format(orig_len-new_len))
    # for each url
    for url_index, url in url_series.iteritems():
        print(url_index)
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

        # get response bytes
        #byte = response.read()
        chunksize = 29999104 # 30MB -- slightly more than what my wifi can handle 
        byte = b''

        # context for download time bar
        with alive_bar(manual = True) as bar:
            # persistent while loop
            while True:
                # read only a set chunk size
                chunk = response.read(chunksize)
                # if no chunk received break
                if not chunk:
                    break
                # append chunk to byte object
                byte += chunk
                # percentage bar downloaded
                bar(len(byte)/content_size)

        # depending on compression get then write file
        print('opening file')
        print(extension)
        if extension == '.gz':
            removed_extension= os.path.splitext(file_name)[0]
            added_prefix = '{}_'.format(url_index) + removed_extension
            extract_gzip(byte, added_prefix, folder_path)    

        elif extension == '.zip':
            extract_zip(byte, folder_path, url_index)

        # if json
        else:
            added_prefix = '{}_'.format(url_index) + file_name
            extract_json(byte, added_prefix, folder_path)

def extract_json(byte, file_name, folder_path):
    # convert bytes to json
    jsoned = json.loads(byte)
    # get file path
    write_path = os.path.join(folder_path, file_name)
    # open file
    with open(write_path, 'w') as out_file:
        # stream json
        json.dump(jsoned, 
                out_file,
                indent = 4)

def extract_gzip(byte, file_name, folder_path):
    print('decompressing')
    # get and uncompress file
    uncompressed = gzip.open(io.BytesIO(byte))
    print('reading compression as json')
    jsoned = json.loads(uncompressed.read())
    write_path = os.path.join(folder_path, file_name)
    print('dumping json')
    with open(write_path, 'w') as out_file:
        json.dump(jsoned,
                out_file,
                indent = 4)

# fun to unzip files
def extract_zip(byte, folder_path, url_index):
    # convert from bytes to unzipped file
    unzipped = zipfile.ZipFile(io.BytesIO(byte))
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
