import unittest

from estimize.services.impl.cache_service_default_impl import CacheServiceDefaultImpl


class TestCacheServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        self.service = CacheServiceDefaultImpl()

    def test_get(self):
        key = 'test'
        df = self.service.get(key)

        self.assertIsNone(df)


if __name__ == '__main__':
    unittest.main()
