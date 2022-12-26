import json
import pandas as pd
import os

def recursive_expand(df):
    """ 
    Function to expand json files no matter the nested structure
    """
    # select all col names 
    col_names = df.columns
    # iterate through them
    for col_name in col_names:
        # trakc col name
        print(col_name)
        # if col type is dict
        if df[col_name].apply(pd.api.types.is_dict_like).any():
            print("=== normalizing ===")
            # normalize the col as a new df (expands out)
            normalized_df = pd.json_normalize(df[col_name])
            # append original col name to names of normalized df
            new_names = [col_name + '.' + name for name in normalized_df.columns.values]
            normalized_df.columns = new_names
            # drop the original dict
            df = df.drop(col_name, axis = 1)
            # join with original table
            df = df.join(normalized_df,
                    lsuffix = '_left',
                    rsuffix = '_right')
        # if col is list explode
        elif df[col_name].apply(pd.api.types.is_list_like).any(): 
            print('=== expanding ===')
            df = df.explode(col_name)

    # if one col is list or dict recurse
    new_col_names = df.columns
    any_dict = any([df[col].apply(pd.api.types.is_dict_like).any() for col in new_col_names])
    print(any_dict)
    any_list = any([df[col].apply(pd.api.types.is_list_like).any() for col in new_col_names])
    print(any_list)
    if any_dict | any_list:
        print('recursing')
        df = recursive_expand(df)

    # make df distinct
    orig_len = df.shape[0]
    df = df.drop_duplicates()
    new_len = df.shape[0]
    print('--- dropping {} duplicates ---'.format(orig_len - new_len))

    return df

def combine_jsons(dir_path):
    """
    iterate through a dir of json files, normalize as df, and concatenate
    """
    # list files
    files = os.listdir(dir_path)
    # get empty df
    allowed_amounts = pd.DataFrame()
    
    for file in files:
        # if ds store skip, should be changed to if tolower('.json')
        if file == '.DS_Store':
            continue
        # get file path
        file_path = os.path.join(dir_path, file)

        # if file larger than 1GB skip
        if os.path.getsize(file_path) > 1e+9: 
            print('File too large, bytes: {}, skipping'.format(os.path.getsize(file_path)))
            continue

        # open file and load as json
        with open(file_path, 'r') as file_:
            jsoned = json.load(file_)

        df = pd.json_normalize(jsoned)
        allowed_amounts = allowed_amounts.append(df)

    # reset index before returning
    allowed_amounts = allowed_amounts.reset_index(drop = True)
    return allowed_amounts

# get json structure (outputs list)
def get_json_structure(file_path):
    # open file as bytes
    with open(file_path, 'rb') as f:
        # instantiate parser
        parser = ijson.parse(f, use_float = True)
        # unique set of prefixes
        prefixes = set()
        # for each element add prefix to set
        for prefix, event, value in parser:
            prefixes.add(prefix)

    # remove empty string
    prefixes.remove('')

    # sort values by length
    prefixes = sorted(prefixes, key = len)
    return prefixes

# pretty print the file structure
def print_structure(values):
    # Create a nested dictionary to store the values and their relationships
    structure = {}
    for value in values:
        # Split the value into a list of path components
        path = value.split(".")
        # Use the path components to build the nested dictionary
        current = structure
        for component in path[:-1]:
            current = current.setdefault(component, {})
        current[path[-1]] = {}
    # Define a recursive function to print the structure
    def print_recursive(structure, indent):
        # Iterate over the keys and values in the structure
        for i, (key, value) in enumerate(structure.items()):
            # Print the appropriate characters before the key
            if i == 0:
                # If this is the first key, print a line
                print(" " * indent + " - " + key)
            else:
                # If this is not the first key, print a dash and a space
                print(" " * indent + " - " + key)
            # Recursively print the structure of the value
            print_recursive(value, indent + 2)
    # Call the recursive function to print the structure
    print_recursive(structure, 0)
