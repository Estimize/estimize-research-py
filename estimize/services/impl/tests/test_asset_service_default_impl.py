import unittest
from injector import Injector
import pandas as pd

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService


class TestAssetServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(AssetService)

    def test_get_returns(self):
        start_date = '2016-12-29'
        end_date = '2017-02-01'
        assets = self.service.get_assets(['CUDA'])

        df = self.service.get_returns(start_date, end_date, assets)
        df['expected_return'] = (df['inter_day_return'] + 1) * (df['intra_day_return'] + 1) - 1

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertIsNone(df.index.get_level_values('as_of_date')[0].tzinfo)

        print(df)

        test_df = df[df.index.get_level_values('as_of_date') == pd.Timestamp('2016-12-30')]
        self.assertAlmostEqual(test_df['close_return'][0], 0.000466853408, 6)
        self.assertAlmostEqual(test_df['expected_return'][0], test_df['close_return'][0], 6)

        test_df = df[df.index.get_level_values('as_of_date') == pd.Timestamp('2017-01-03')]
        self.assertAlmostEqual(test_df['close_return'][0], 0.03919738684, 6)
        self.assertAlmostEqual(test_df['expected_return'][0], test_df['close_return'][0], 6)


if __name__ == '__main__':
    unittest.main()
