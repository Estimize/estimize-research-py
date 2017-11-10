import os
import pandas as pd

from abc import abstractmethod
from memoized_property import memoized_property


class Cache(object):

    def get(self, key):
        item = self.CacheItem(self, key)

        if item.exists:
            return item.df
        else:
            return None

    def put(self, key, df):
        item = self.CacheItem(self, key)
        item.store(df)

    class CacheItem(object):

        def __init__(self, cache, key):
            self.cache = cache
            self.key = key

        @property
        @abstractmethod
        def exists(self):
            pass

        @property
        @abstractmethod
        def df(self):
            pass

        @abstractmethod
        def store(self, payload):
            pass


class FileCache(Cache):

    DEFAULT_CACHE_DIR = os.path.join(os.getcwd(), '.cache')

    @memoized_property
    def cache_dir(self):
        if not os.path.exists(self.DEFAULT_CACHE_DIR):
            os.mkdir(self.DEFAULT_CACHE_DIR)

        return self.DEFAULT_CACHE_DIR

    class CacheItem(Cache.CacheItem):

        def __init__(self, cache, key):
            self.cache = cache
            self.key = key

        @property
        def exists(self):
            return os.path.exists(self.path)

        @property
        def df(self):
            return pd.read_hdf(self.path)

        @memoized_property
        def path(self):
            return os.path.join(self.cache.cache_dir, self.filename)

        @memoized_property
        def filename(self):
            return '{}.h5'.format(self.key)

        def store(self, df):
            df.to_hdf(self.path, key='df', mode='w')


CACHE = FileCache()


def get(key):
    return CACHE.get(key)


def put(key, df):
    CACHE.put(key, df)
