#!/usr/bin/env python
# coding: utf-8

# In[7]:


from os import walk
from os.path import join

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# In[2]:


mongo_pwd = "ExKF7lqfTl3FiLDg"


# In[3]:


client = MongoClient(f"mongodb+srv://lazyv:{mongo_pwd}@cluster0.xz0gy.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", 
                             server_api=ServerApi('1'))


# In[4]:


db = client.index


# In[5]:


db.list_collection_names()


# In[6]:


collection = db['hadoop-inverted-indexes']


# In[21]:


rootdir = './hadoop-inverse-index'

for subdir, dirs, files in walk(rootdir):    
    for file in files:
        # print(join(subdir, file))
        with open(join(subdir, file), 'r') as f:
            for line in f:
                arr = line.split('\t')
                idf = []
                word = arr[0] 
                for freq in arr[1:-1]:
                    obj_id, count = freq.split(':')
                    idf.append({'tweet_obj_id': obj_id, 'count': count})
                # print(word, idf)
                collection.insert_one({'word': word, 'idf': idf})


# In[ ]:




