import logging
import pandas as pd
from injector import inject
from memoized_property import memoized_property

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services import CacheService, CsvDataService, EstimizeConsensusService, AssetService

logger = logging.getLogger(__name__)


class EstimizeConsensusServiceDefaultImpl(EstimizeConsensusService):

    @inject
    def __init__(self, csv_data_service: CsvDataService, cache_service: CacheService, asset_service: AssetService):
        self.csv_data_service = csv_data_service
        self.cache_service = cache_service
        self.asset_service = asset_service

    def get_final_consensuses(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        logger.info('get_final_consensuses: start')

        cache_key = 'estimize_final_consensuses'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = self.get_consensuses()
            df = df.iloc[df.index.get_level_values('as_of_date') == pd.to_datetime(df['reports_at_date'])]
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        logger.info('get_final_consensuses: end')

        return df

    def get_consensuses(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        logger.info('get_consensuses: start')

        cache_key = 'estimize_consensuses'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = self.csv_data_service.get_from_file(
                filename='{}/consensus.csv'.format(cfg.data_dir()),
                pre_func=self._pre_func,
                post_func=self._post_func,
                date_column='date',
                date_format='%y-%m-%d',
                timezone='US/Eastern',
                symbol_column='ticker'
            )
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        logger.info('get_consensuses: end')

        return df

    @staticmethod
    def _pre_func(df):
        df.drop(['cusip'], axis=1, inplace=True)
        df[['estimize.eps.count', 'estimize.revenue.count']] = df[['estimize.eps.count', 'estimize.revenue.count']].fillna(value=0).astype('int')

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
        df = df[[
            'fiscal_year',
            'fiscal_quarter',
            'reports_at_date',
            'bmo',
            'estimize.eps.weighted',
            'estimize.eps.mean',
            'estimize.eps.high',
            'estimize.eps.low',
            'estimize.eps.sd',
            'estimize.eps.count',
            'estimize.revenue.weighted',
            'estimize.revenue.mean',
            'estimize.revenue.high',
            'estimize.revenue.low',
            'estimize.revenue.sd',
            'estimize.revenue.count',
            'wallstreet.eps',
            'wallstreet.revenue',
            'actual.eps',
            'actual.revenue'
        ]]

        return df


class EarningsYieldQuery:

    def __init__(self, estimize_consensus_service, asset_service, consensuses):
        self.estimize_consensus_service = estimize_consensus_service
        self.asset_service = asset_service
        self.consensuses = consensuses

    def results(self):
        df = self.full_year_eps_values.join(self.moving_averages, how='inner')
        df['actual.eps.yield'] /= df['moving_average']
        df['estimize.eps.weighted.yield'] /= df['moving_average']
        df['estimize.eps.mean.yield'] /= df['moving_average']
        df['estimize.eps.high.yield'] /= df['moving_average']
        df['estimize.eps.low.yield'] /= df['moving_average']
        df['wallstreet.eps.yield'] /= df['moving_average']
        df = df[[
            'fiscal_year',
            'fiscal_quarter',
            'reports_at_date',
            'bmo',
            'estimize.eps.weighted.yield',
            'estimize.eps.mean.yield',
            'estimize.eps.high.yield',
            'estimize.eps.low.yield',
            'estimize.eps.sd',
            'estimize.eps.count',
            'wallstreet.eps.yield',
            'actual.eps.yield'
        ]]

        return df

    @property
    def moving_averages(self):
        return self.asset_service.get_moving_average(self.start_date, self.end_date, self.assets)

    @property
    def full_year_eps_values(self):
        df = self.consensuses_reindexed.join(self.trailing_3_quarter_eps_actuals, how='inner')
        df['actual.eps.yield'] = df['actual.eps'] + df['trailing_eps']
        df['estimize.eps.weighted.yield'] = df['estimize.eps.weighted'] + df['trailing_eps']
        df['estimize.eps.high.yield'] = df['estimize.eps.high'] + df['trailing_eps']
        df['estimize.eps.low.yield'] = df['estimize.eps.low'] + df['trailing_eps']
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
