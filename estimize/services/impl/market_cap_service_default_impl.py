from injector import inject
import pandas as pd

import estimize.config as cfg
from estimize.pandas import dfutils

from estimize.services import MarketCapService, CsvDataService, CacheService, CalendarService


class MarketCapServiceDefaultImpl(MarketCapService):

    @inject
    def __init__(self, cache_service: CacheService, csv_data_service: CsvDataService, calendar_service: CalendarService):
        self.cache_service = cache_service
        self.csv_data_service = csv_data_service
        self.calendar_service = calendar_service

    def get_market_caps(self, start_date=None, end_date=None, assets=None):
        cache_key = 'market_caps'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = self.csv_data_service.get_from_url(
                url='{}/market_caps.csv'.format(cfg.ROOT_DATA_URL),
                post_func=self._post_func,
                date_column='as_of_date',
                symbol_column='ticker'
            )

            df.reset_index(inplace=True)
            df = pd.pivot_table(df, index='as_of_date', columns='asset', values='market_cap')
            end_date = df.index.get_level_values('as_of_date').max()

            dates = self.calendar_service.get_trading_days_between(cfg.DEFAULT_START_DATE, end_date)
            dates = pd.DataFrame([], index=dates)

            df = dates.join(df, how='left')
            df.ffill(inplace=True)
            df = df.stack().to_frame()
            df.reset_index(inplace=True)
            df.rename(columns={'level_0': 'as_of_date', 'level_1': 'asset', df.columns[-1]: 'market_cap'}, inplace=True)
            df.set_index(['as_of_date', 'asset'], inplace=True)

            df.loc[df['market_cap'].between(0, 300e6), 'market_cap_type'] = 'Micro'
            df.loc[df['market_cap'].between(300e6, 2e9), 'market_cap_type'] = 'Small'
            df.loc[df['market_cap'].between(2e9, 10e9), 'market_cap_type'] = 'Mid'
            df.loc[df['market_cap'].between(10e9, 200e9), 'market_cap_type'] = 'Large'
            df.loc[df['market_cap'].between(200e9, 12e12), 'market_cap_type'] = 'Mega'

            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        return df

    @staticmethod
    def _post_func(df):
        df.index.tz = 'US/Eastern'
        df.index.name = 'as_of_date'
        df.rename(columns={'sid': 'asset'}, inplace=True)
        df.reset_index(inplace=True)
        df['as_of_date'] = df['as_of_date'].dt.date
        df.set_index(['as_of_date', 'asset'], inplace=True)

        return df
