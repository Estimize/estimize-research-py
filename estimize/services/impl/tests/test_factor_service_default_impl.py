import logging
import unittest

from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.logging import configure_logging
from estimize.services import FactorService, AssetService


class TestFactorServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        configure_logging(logging.INFO)
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.service = injector.get(FactorService)

    def test_get_betas(self):
        start_date = '2017-12-21'
        end_date = '2018-01-01'
        assets = self.asset_service.get_assets(['AMZN', 'AAPL', 'GOOGL', 'CMG'])
        df = self.service.get_market_factors(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
