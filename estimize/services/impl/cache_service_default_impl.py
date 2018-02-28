import os
import tempfile
from abc import abstractmethod

import boto3
import botocore
import pandas as pd
from memoized_property import memoized_property

import estimize.config as cfg
from estimize.services.cache_service import CacheService


class CacheServiceDefaultImpl(CacheService):

    def put(self, key: str, df: pd.DataFrame):
        self.local_cache.put(key, df)

    def get(self, key: str) -> pd.DataFrame:
        df = self.local_cache.get(key)

        if df is None:
            df = self.remote_cache.get(key)

            if df is not None:
                self.local_cache.put(key, df)

        return df

    @memoized_property
    def local_cache(self):
        return LocalCache()

    @memoized_property
    def remote_cache(self):
        return RemoteCache()


class Cache:

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


class LocalCache(Cache):

    @memoized_property
    def cache_dir(self):
        if os.path.basename(os.getcwd()) == 'notebooks':
            path = os.path.join(os.getcwd(), '../.cache')
        else:
            path = os.path.join(os.getcwd(), '.cache')

        if not os.path.exists(path):
            os.mkdir(path)

        return path

    class CacheItem(Cache.CacheItem):

        def __init__(self, cache, key):
            self.cache = cache
            self.key = key

        @property
        def exists(self):
            return os.path.exists(self.path)

        @memoized_property
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


class RemoteCache(Cache):

    @memoized_property
    def s3_bucket(self):
        return boto3.resource('s3', region_name='us-east-1').Bucket(cfg.S3_DATA_BUCKET)

    class CacheItem(Cache.CacheItem):

        def __init__(self, cache, key):
            self.cache = cache
            self.key = key

        @property
        def exists(self):
            try:
                self.df
                return True
            except botocore.exceptions.ClientError as ex:
                if ex.response['Error']['Code'] == '404':
                    return False
                else:
                    raise

        @memoized_property
        def df(self):
            with tempfile.NamedTemporaryFile(suffix=self.filename) as f:
                self.s3_object.download_file(f.name)
                return pd.read_hdf(f.name)

        @property
        def s3_object(self):
            return self.cache.s3_bucket.Object(self.s3_key)

        @property
        def s3_key(self):
            return 'research/{}/{}'.format(cfg.CURRENT_QUARTER, self.filename)

        @memoized_property
        def filename(self):
            return '{}.h5'.format(self.key)

        def store(self, df):
            raise NotImplementedError()
