#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Cache

import collections
import pickle
import os

class LRUCache:
    def __init__(self, capacity=10):
        self.cache = collections.OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        # Move the key to the end to show that it was recently used
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            # Move the key to the end to show that it was recently used
            self.cache.move_to_end(key)
        self.cache[key] = value
       # print("cache length= "+len(self.cache))
        # If the cache exceeds the capacity, remove the first item
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def save_to_disk(self, filename='cache.pkl'):
        with open(filename, 'wb') as f:
            pickle.dump(self.cache, f)

    def load_from_disk(self, filename='cache.pkl'):
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                self.cache = pickle.load(f)
                
    def print_all_keys(self):
        for key in self.cache.keys():
            print(key)

# Instantiate and load cache
cache = LRUCache(capacity=7)



# In[87]:


cache.print_all_keys()


# In[342]:


import threading

def checkpoint_cache(cache, interval=60):
    def run():
        while True:
            time.sleep(interval)
            #print("awake")
            cache.save_to_disk()
            cache.load_from_disk()
    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()


# In[343]:


checkpoint_cache(cache)


# In[ ]:




