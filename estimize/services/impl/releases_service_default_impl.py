import pandas as pd
from injector import inject

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services import ReleasesService, CacheService, AssetInfoService


class ReleasesServiceDefaultImpl(ReleasesService):

    @inject
    def __init__(self, cache_service: CacheService, asset_info_service: AssetInfoService):
        self.cache_service = cache_service
        self.asset_info_service = asset_info_service

    def get_releases(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        cache_key = 'releases'
        df = self.cache_service.get(cache_key)

        if df is None:
            adf = self.asset_info_service.get_asset_info().reset_index()[['asset', 'instrument_id']]
            adf.set_index('instrument_id', inplace=True)

            df = pd.read_csv('{}/releases.csv'.format(cfg.data_dir()))
            df.rename(columns={'id': 'release_id'}, inplace=True)
            df.set_index('instrument_id', inplace=True)

            df = df.join(adf, how='inner')
            df.reset_index(inplace=True)
            df['as_of_date'] = pd.to_datetime(df['reports_at'], format='%Y-%m-%dT%H:%M:%S').dt.tz_localize(
                'UTC').dt.tz_convert('US/Eastern').dt.date
            df.set_index(['as_of_date', 'asset'], inplace=True)

            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        return df
