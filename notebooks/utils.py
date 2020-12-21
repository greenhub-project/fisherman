import re
import sys
import glob
import numpy as np
import pandas as pd

def memory_usage(df, aggregate=True):
    memory = df.memory_usage(index=True, deep=True).sum() if aggregate else df.memory_usage(index=True, deep=True)
    return round(memory / 1024 ** 2, 2)

def get_files(filepath):
    # natural order sorting
    return sorted(glob.glob(filepath), key=lambda x: int(re.findall(r'\d+', x)[0]))

def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new

def update_page_metadata(collection, df, exclude=['id']):
    df = df.drop(exclude, axis=1)

    numeric_stats, object_stats = {}, {}
    
    try:
        numeric_stats = df.describe(include=[np.number]).to_dict()
    except:
        pass

    try:
        object_stats = df.describe(exclude=[np.number]).to_dict()
    except:
        pass

    entry = correct_encoding({**numeric_stats, **object_stats})
    entry = {**entry, 'nrows': df.shape[0]}

    return collection.update_one({}, {'$push': {'pages': entry}})

def optimize_dtypes(df, mappings):
    string_columns = df.select_dtypes(include='object').columns.to_list()
    df[string_columns] = df[string_columns].astype('category')
    
    return df.astype(mappings)

def downcast_dtypes(df, dtype, downcast):
    df_selection = df.select_dtypes(include=dtype)
    return df_selection.apply(pd.to_numeric, downcast=downcast)
  
def int16_repr(x):
    '''
    settings mask:
    ['screen_on', 'roaming_enabled', 'bluetooth_enabled', 'location_enabled',
    'power_saver_enabled', 'nfc_enabled', 'developer_mode', 'wifi_enabled',
    'mobile_enabled', 'wifi_active', 'mobile_active']
    '''
    return x.dot(1 << np.arange(x.shape[-1])).astype(np.uint16)

