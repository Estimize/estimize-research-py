from injector import inject
import pandas as pd

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services.asset_info_service import AssetInfoService
from estimize.services.cache_service import CacheService
from estimize.services.csv_data_service import CsvDataService


class AssetInfoServiceDefaultImpl(AssetInfoService):

    @inject
    def __init__(self, csv_data_service: CsvDataService, cache_service: CacheService):
        self.csv_data_service = csv_data_service
        self.cache_service = cache_service

    def get_asset_info(self, assets=None):
        cache_key = 'asset_info'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = self.csv_data_service.get_from_file(
                filename='{}/instruments.csv'.format(cfg.data_dir()),
                pre_func=self._pre_func,
                post_func=self._post_func,
                symbol_column='ticker'
            )
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, assets=assets)

        return df

    @staticmethod
    def _pre_func(df):
        df['date'] = pd.Timestamp.now()

        return df

    @staticmethod
    def _post_func(df):
        df.reset_index(inplace=True)
        df.drop(['dt'], axis=1, inplace=True)
        df.rename(columns={'sid': 'asset', 'id': 'instrument_id'}, inplace=True)
        df.set_index(['asset'], inplace=True)

        return df
