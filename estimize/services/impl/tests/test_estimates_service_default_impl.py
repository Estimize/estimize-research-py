import unittest
from injector import Injector
import pandas as pd

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService, EstimatesService
from estimize.services.impl import EstimizeSignalServiceDefaultImpl


class TestEstimatesServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.service = injector.get(EstimatesService)

    def test_asset_info(self):
        df = self.service.get_estimates()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
