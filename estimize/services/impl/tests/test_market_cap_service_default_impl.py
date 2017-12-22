import unittest

from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services import MarketCapService


class TestMarketCapServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(MarketCapService)

    def test_get_market_caps(self):
        df = self.service.get_market_caps()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
