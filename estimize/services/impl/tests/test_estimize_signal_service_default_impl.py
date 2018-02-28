import unittest
from injector import Injector
import pandas as pd

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService
from estimize.services.impl import EstimizeSignalServiceDefaultImpl


class TestEstimizeSignalServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.service = injector.get(EstimizeSignalServiceDefaultImpl)

    def test_get_signals(self):
        start_date = '2016-12-29'
        end_date = '2017-02-01'
        assets = self.asset_service.get_assets(['CUDA'])
        df = self.service.get_signals(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertIsNone(df.index.get_level_values('as_of_date')[0].tzinfo)

        print(df)

        test_df = df[df.index.get_level_values('as_of_date') == pd.Timestamp('2017-01-02T07:00:00-05:00')]
        self.assertAlmostEqual(test_df['signal'][0], 18.887548804340337, 6)

        test_df = df[df.index.get_level_values('as_of_date') == pd.Timestamp('2017-01-02T014:00:00-05:00')]
        self.assertAlmostEqual(test_df['signal'][0], 20.054007285867993, 6)

    def test_get_signals_for_INFO(self):
        start_date = '2012-01-01'
        end_date = '2018-01-01'
        assets = self.asset_service.get_assets(['INFO'])

        print(assets)

        df = self.service.get_signals(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertTrue(df.empty)

        print(df)

    def test_get_signals_for_IHS(self):
        start_date = '2012-01-01'
        end_date = '2018-01-01'
        assets = self.asset_service.get_assets(['IHS'])

        print(assets)

        df = self.service.get_signals(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)

    def test_get_signals_for_MRKT(self):
        start_date = '2012-01-01'
        end_date = '2018-01-01'
        assets = self.asset_service.get_assets(['MRKT'])

        print(assets)

        df = self.service.get_signals(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
