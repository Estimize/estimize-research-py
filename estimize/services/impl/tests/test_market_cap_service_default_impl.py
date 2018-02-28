import unittest

from injector import Injector
import pandas as pd

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

        self.assertEquals(df.index.get_level_values('as_of_date')[0], pd.Timestamp('2012-01-31'))
        self.assertEquals(df.index.get_level_values('as_of_date')[-1], pd.Timestamp('2017-12-29'))

        print(df)


if __name__ == '__main__':
    unittest.main()
