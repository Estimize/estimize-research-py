import logging

from injector import inject

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services import EstimizeSignalService, CsvDataService, CacheService
import pandas as pd

logger = logging.getLogger(__name__)


class EstimizeSignalServiceDefaultImpl(EstimizeSignalService):

    @inject
    def __init__(self, csv_data_service: CsvDataService, cache_service: CacheService):
        self.csv_data_service = csv_data_service
        self.cache_service = cache_service

    def get_signals(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        logger.info('get_signals: start')

        cache_key = 'estimize_signals'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = self.csv_data_service.get_from_file(
                filename='{}/signal_time_series.csv'.format(cfg.data_dir()),
                pre_func=self._pre_func,
                post_func=self._post_func,
                date_column='as_of',
                date_format='%Y-%m-%dT%H:%M:%S',
                timezone='UTC',
                symbol_column='ticker'
            )
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        logger.info('get_signals: end')

        return df

    @staticmethod
    def _pre_func(df):
        df.drop(['cusip'], axis=1, inplace=True)

        return df

    @staticmethod
    def _post_func(df):
        df.index.tz = 'US/Eastern'
        df.index.name = 'as_of_date'
        df.rename(columns={'sid': 'asset'}, inplace=True)
        df.reset_index(inplace=True)
        df['reports_at'] = pd.to_datetime(df['reports_at'], format='%Y-%m-%dT%H:%M:%S').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        df['bmo'] = df['reports_at'].dt.hour < 12
        df['reports_at'] = df['reports_at'].dt.date
        df.rename(columns={'reports_at': 'reports_at_date'}, inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)

        return df
