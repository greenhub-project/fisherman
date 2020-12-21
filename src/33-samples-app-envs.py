#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys

sys.path.append('..')


# In[2]:


import os
import gc
import yaml
import numpy as np
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

from joblib import Parallel, delayed
from utils import optimize_dtypes, get_files, int16_repr, downcast_dtypes


# In[3]:


BASE_PATH = '../../'

CONFIG_DIR = os.path.join(BASE_PATH, 'config')
STORAGE_DIR = os.path.join(BASE_PATH, 'storage')

config = yaml.load(open(os.path.join(CONFIG_DIR, 'env.yml')),
                   Loader=yaml.FullLoader)


# In[4]:


model = 'unplugged'
model_storage = os.path.join(STORAGE_DIR, model)
output_storage = os.path.join(STORAGE_DIR, 'output')


# In[5]:


filepath = os.path.join(output_storage, f'{model}.parquet')

df = pd.read_parquet(filepath)


# In[6]:


converted_int = downcast_dtypes(df, ['int'], 'unsigned')
converted_float = downcast_dtypes(df, ['float'], 'float')
converted_bool = df.select_dtypes(include=['bool'])
converted_object = df.select_dtypes(include=['object'])

df.loc[:, converted_int.columns] = converted_int
df.loc[:, converted_float.columns] = converted_float
df.loc[:, converted_bool.columns] = converted_bool.astype(np.uint8)
df.loc[:, converted_object.columns] = converted_object.astype('category')


# In[7]:


mappings = {'id': 'uint32', 'device_id': 'uint32', 'battery_level': 'uint8', 'timezone': 'category', 'memory_active': 'uint32',
 'memory_inactive': 'uint32', 'memory_free': 'uint32', 'memory_user': 'uint32', 'health': 'category', 'voltage': 'float32',
 'temperature': 'float32', 'usage': 'uint8', 'up_time': 'float32', 'sleep_time': 'float32', 'wifi_signal_strength': 'int16',
 'wifi_link_speed': 'int16', 'screen_on': 'uint8', 'screen_brightness': 'int16', 'roaming_enabled': 'uint8', 'bluetooth_enabled': 'uint8',
 'location_enabled': 'uint8', 'power_saver_enabled': 'uint8', 'nfc_enabled': 'uint8', 'developer_mode': 'uint8', 'free': 'uint32',
 'total': 'uint32', 'free_system': 'uint32', 'total_system': 'uint32', 'wifi_enabled': 'uint8', 'mobile_enabled': 'uint8',
 'wifi_active': 'uint8', 'mobile_active': 'uint8', 'profile': 'uint16'}

# df = df.astype(mappings)


# In[8]:


df = df.rename({'usage': 'cpu_usage'}, axis=1)


# In[9]:


model = 'app_processes'
model_storage = os.path.join(STORAGE_DIR, model)

app_files = get_files(os.path.join(model_storage, '*.parquet'))


# In[10]:


mappings = {'sample_id': 'uint32', 'domain': 'category', 'application_label': 'category',
            'is_system_app': 'uint8'}

subset = ['domain', 'battery_level', 'health', 'voltage', 'temperature', 'cpu_usage',
          'screen_on', 'screen_brightness', 'roaming_enabled', 'bluetooth_enabled',
          'location_enabled', 'power_saver_enabled', 'developer_mode',
          'wifi_enabled', 'wifi_active', 'mobile_active']


# In[11]:


pages = []
min_id = df['id'].min()
found_start = False

for i, f in enumerate(app_files[:100]):
    page = pd.read_parquet(f) if found_start else pd.read_parquet(f, columns=['sample_id'])

    
    if page['sample_id'].max() < min_id:
        continue
    else:
        page = pd.read_parquet(f)
        found_start = True
    
    print(f'Entry {i}')
    
    page = page.dropna()
    
    page = page.astype(mappings)
    
    page = pd.merge(page, df, left_on='sample_id', right_on='id', sort=False, copy=False)
    
    pages.append(page.drop_duplicates(subset=subset))


# In[ ]:


environments = pd.DataFrame()

while pages:
    environments = environments.append(pages.pop(0), ignore_index=True)
    gc.collect()


# In[ ]:


environments.head()


# In[ ]:




