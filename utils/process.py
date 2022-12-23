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
        # if
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
        print(file)
        if file == '.DS_Store':
            print('skipping')
            continue
        file_path = os.path.join(dir_path, file)
        with open(file_path, 'r') as file_:
            jsoned = json.load(file_)

        df = pd.json_normalize(jsoned)
        allowed_amounts = allowed_amounts.append(df)

    # reset index before returning
    allowed_amounts = allowed_amounts.reset_index(drop = True)
    return allowed_amounts

