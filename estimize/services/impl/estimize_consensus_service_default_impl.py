import logging
import pandas as pd
from injector import inject
from memoized_property import memoized_property

from estimize.pandas import cache, dfutils
from estimize.services import CacheService, CsvDataService, EstimizeConsensusService, AssetService

logger = logging.getLogger(__name__)


class EstimizeConsensusServiceDefaultImpl(EstimizeConsensusService):

    DEBUG = False

    @inject
    def __init__(self, csv_data_service: CsvDataService, cache_service: CacheService, asset_service: AssetService):
        self.csv_data_service = csv_data_service
        self.cache_service = cache_service
        self.asset_service = asset_service

    def get_final_earnings_yields(self, start_date=None, end_date=None) -> pd.DataFrame:
        logger.info('get_final_earnings_yields: start')

        cache_key = 'estimize_final_earnings_yields'
        df = cache.get(cache_key)

        if df is None:
            df = EarningsYieldQuery(self, self.asset_service, self.get_final_consensuses()).results()
            cache.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date)

        logger.info('get_final_earnings_yields: end')

        return df

    def get_earnings_yields(self, start_date=None, end_date=None) -> pd.DataFrame:
        logger.info('get_earnings_yields: start')

        cache_key = 'estimize_earnings_yields'
        df = cache.get(cache_key)

        if df is None:
            df = EarningsYieldQuery(self, self.asset_service, self.get_consensuses()).results()
            cache.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date)

        logger.info('get_earnings_yields: end')

        return df

    def get_final_consensuses(self, start_date=None, end_date=None) -> pd.DataFrame:
        logger.info('get_final_consensuses: start')

        cache_key = 'estimize_final_consensuses'
        df = cache.get(cache_key)

        if df is None:
            df = self.get_consensuses()
            df = df.iloc[df.index.get_level_values('as_of_date') == pd.to_datetime(df['reports_at_date'])]
            cache.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date)

        logger.info('get_final_consensuses: end')

        return df

    def get_consensuses(self, start_date=None, end_date=None) -> pd.DataFrame:
        logger.info('get_consensuses: start')

        cache_key = 'estimize_consensuses'
        df = cache.get(cache_key)

        if df is None:
            url = 'https://s3.amazonaws.com/com.estimize.production.data/quantopian/consensus.csv'

            if self.DEBUG:
                url = 'https://s3.amazonaws.com/com.estimize.production.data/quantopian/consensus_trunc.csv'

            df = self.csv_data_service.get_from_url(
                url=url,
                pre_func=self._pre_func,
                post_func=self._post_func,
                date_column='date',
                date_format='%y-%m-%d',
                timezone='US/Eastern',
                symbol_column='ticker'
            )
            cache.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date)

        logger.info('get_consensuses: end')

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
        df['as_of_date'] = df['as_of_date'].dt.date
        df['reports_at'] = pd.to_datetime(df['reports_at'], format='%Y-%m-%dT%H:%M:%S').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        df['bmo'] = df['reports_at'].dt.hour < 12
        df['reports_at'] = df['reports_at'].dt.date
        df.rename(columns={'reports_at': 'reports_at_date'}, inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)

        return df


class EarningsYieldQuery:

    def __init__(self, estimize_consensus_service, asset_service, consensuses):
        self.estimize_consensus_service = estimize_consensus_service
        self.asset_service = asset_service
        self.consensuses = consensuses

    def results(self):
        df = self.full_year_eps_values.join(self.moving_averages)
        df['actual.eps.yield'] /= df['moving_average']
        df['estimize.eps.weighted.yield'] /= df['moving_average']
        df['estimize.eps.mean.yield'] /= df['moving_average']
        df['wallstreet.eps.yield'] /= df['moving_average']
        df.dropna(inplace=True)
        df.drop(['moving_average'], axis=1, inplace=True)

        return df

    @property
    def moving_averages(self):
        return self.asset_service.get_moving_average(self.start_date, self.end_date, self.assets)

    @property
    def full_year_eps_values(self):
        df = self.consensuses_reindexed.join(self.trailing_3_quarter_eps_actuals)
        df['actual.eps.yield'] = df['actual.eps'] + df['trailing_eps']
        df['estimize.eps.weighted.yield'] = df['estimize.eps.weighted'] + df['trailing_eps']
        df['estimize.eps.mean.yield'] = df['estimize.eps.mean'] + df['trailing_eps']
        df['wallstreet.eps.yield'] = df['wallstreet.eps'] + df['trailing_eps']
        df.reset_index(inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)
        df.drop(['fiscal_date', 'trailing_eps'], axis=1, inplace=True)

        return df

    @property
    def trailing_3_quarter_eps_actuals(self):
        logger.debug('trailing_3_quarter_eps_actuals: start')

        cdf = self.final_consensuses
        self.add_fiscal_date(cdf)
        cdf.reset_index(inplace=True)
        cdf.drop(['as_of_date'], axis=1, inplace=True)
        cdf.set_index(['asset', 'fiscal_date'], inplace=True)
        dfs = []

        for asset in self.assets:
            logger.debug('trailing_3_quarter_eps_actuals: processing {}'.format(asset))

            adf = cdf.loc[cdf.index.get_level_values('asset') == asset, ['actual.eps']]
            adf = adf.rolling(3).sum()
            adf = adf.shift(1)
            adf.dropna(inplace=True)
            dfs.append(adf)

        df = pd.concat(dfs)
        df.rename(columns={'actual.eps': 'trailing_eps'}, inplace=True)

        logger.debug('trailing_3_quarter_eps_actuals: end')

        return df

    @memoized_property
    def assets(self):
        return dfutils.unique_assets(self.final_consensuses)

    @memoized_property
    def start_date(self):
        return self.consensuses_reindexed['as_of_date'].min()

    @memoized_property
    def end_date(self):
        return self.consensuses_reindexed['as_of_date'].max()

    @memoized_property
    def final_consensuses(self):
        return self.estimize_consensus_service.get_final_consensuses()

    @memoized_property
    def consensuses_reindexed(self):
        df = self.consensuses
        self.add_fiscal_date(df)
        df.reset_index(inplace=True)
        df.set_index(['asset', 'fiscal_date'], inplace=True)

        return df

    @staticmethod
    def add_fiscal_date(df):
        df['fiscal_date'] = (df['fiscal_year'] * 10) + df['fiscal_quarter']
