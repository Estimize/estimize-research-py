from unittest import TestCase

from injector import Injector
import pandas as pd

import estimize.config as cfg
from estimize.di.default_module import DefaultModule
from estimize.services import AssetService


class TestAssetServiceZiplineImpl(TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(AssetService)

    def test_get_returns(self):
        asset = self.service.get_asset('AAPL')
        df = self.service.get_returns(cfg.DEFAULT_START_DATE, cfg.DEFAULT_END_DATE, [asset])

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        self.assertEqual(df.index.get_level_values('as_of_date')[0], pd.Timestamp('2012-01-03'))
        self.assertEqual(df.index.get_level_values('as_of_date')[-1], pd.Timestamp('2017-12-27'))

        print(df)
