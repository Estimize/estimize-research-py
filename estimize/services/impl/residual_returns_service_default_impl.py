import logging
import multiprocessing as mp
from abc import abstractmethod

import numpy as np
import pandas as pd
import pathos.pools as pp
from injector import inject
from memoized_property import memoized_property
from pandas_datareader.data import DataReader

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services import ResidualReturnsService, CalendarService, AssetService, CacheService
from estimize.services.estimize_consensus_service import EstimizeConsensusService

logger = logging.getLogger(__name__)
counter = mp.Value('i', 0)


class ResidualReturnsServiceDefaultImpl(ResidualReturnsService):

    @inject
    def __init__(self, cache_service: CacheService, estimize_consensus_service: EstimizeConsensusService, calendar_service: CalendarService, asset_service: AssetService):
        self.cache_service = cache_service
        self.estimize_consensus_service = estimize_consensus_service
        self.calendar_service = calendar_service
        self.asset_service = asset_service

    def get_market_neutral_residual_returns(self, start_date, end_date, assets=None, on='close'):
        logger.info('market_neutral_residual_returns: start')

        cache_key = 'market_neutral_residual_returns_on_{}'.format(on)
        df = self.cache_service.get(cache_key)

        if df is None:
            assets = dfutils.unique_assets(self.estimize_consensus_service.get_final_consensuses())
            df = MarketNeutralResidualReturnsQuery(
                calendar_service=self.calendar_service,
                asset_service=self.asset_service,
                assets=assets,
                on=on
            ).results()
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        logger.info('market_neutral_residual_returns: end')

        return df

    def get_multi_factor_residual_returns(self, start_date, end_date, assets=None, on='close'):
        logger.info('multi_factor_residual_returns: start')

        cache_key = 'multi_factor_residual_returns_on_{}'.format(on)
        df = self.cache_service.get(cache_key)

        if df is None:
            assets = dfutils.unique_assets(self.estimize_consensus_service.get_final_consensuses())
            df = FamaFrenchResidualReturnsQuery(
                calendar_service=self.calendar_service,
                asset_service=self.asset_service,
                assets=assets,
                on=on
            ).results()
            self.cache_service.put(cache_key, df)

        df = dfutils.filter(start_date, end_date, assets)

        logger.info('multi_factor_residual_returns: end')

        return df


class ResidualReturnsQuery:

    ASYNC_DEBUG = False
    DEBUG = False

    def __init__(self, calendar_service: CalendarService, asset_service: AssetService, assets, on):
        self.calendar_service = calendar_service
        self.asset_service = asset_service
        self.assets = assets
        self.on = on
        self.window_length = 252
        self.pool = pp.ProcessPool(mp.cpu_count())

    def results(self):
        logger.info('results: start')

        counter = mp.Value('i', 0)

        if not self.ASYNC_DEBUG:
            df = self.results_async()
        else:
            df = self.results_debug()

        logger.info('results: end')

        return df

    def results_async(self):
        logger.info('results_async: start')

        self.assets_returns
        self.factors

        if self.DEBUG:
            assets = self.assets[0:5]
        else:
            assets = self.assets

        dfs = self.pool.map(self.get_residual_returns_for_asset, assets)
        self.pool.close()
        self.pool.join()

        df = pd.concat(dfs, copy=False)

        logger.info('results_async: end')

        return df

    def results_debug(self):
        logger.info('results_debug: start')

        dfs = []

        if self.DEBUG:
            assets = self.assets[0:5]
        else:
            assets = self.assets

        for asset in assets:
            bdf = self.get_residual_returns_for_asset(asset)
            dfs.append(bdf)

        df = pd.concat(dfs, copy=False)

        logger.info('results_debug: end')

        return df

    @property
    @abstractmethod
    def factors(self):
        raise NotImplementedError()

    @memoized_property
    def assets_returns(self):
        return_column = '{}_return'.format(self.on)
        ardf = self.asset_service.get_returns(self.windowed_start_date, self.end_date, self.assets)
        ardf.reset_index(inplace=True)
        ardf = ardf[['as_of_date', 'asset', return_column]].copy()
        ardf.rename(columns={return_column: 'return'}, inplace=True)
        ardf.set_index('asset', inplace=True)

        return ardf

    @memoized_property
    def windowed_start_date(self):
        return self.calendar_service.get_n_trading_days_from((-self.window_length - 1), self.start_date)[0]

    @memoized_property
    def start_date(self):
        return self.calendar_service.get_valid_trading_start_date(cfg.DEFAULT_START_DATE)

    @memoized_property
    def end_date(self):
        return self.calendar_service.get_valid_trading_end_date(cfg.DEFAULT_END_DATE)

    def get_returns_for_asset(self, asset):
        ardf = self.assets_returns[self.assets_returns.index == asset].copy()
        ardf.reset_index(inplace=True)
        ardf.set_index('as_of_date', inplace=True)
        ardf.dropna(inplace=True)
        ardf.sort_index(inplace=True)

        return ardf

    def get_residual_returns_for_asset(self, asset):
        df = self.AssetResidualReturnsQuery(self, asset).results()

        with counter.get_lock():
            counter.value += 1
            progress = (float(counter.value) / len(self.assets)) * 100

            logger.info('progress: {}%'.format(progress))

        return df

    class AssetResidualReturnsQuery(object):

        def __init__(self, parent_calculator, asset):
            self.parent_calculator = parent_calculator
            self.asset = asset

        def results(self):
            logger.info('calculating for {}'.format(self.asset))

            self.data['residual_return'] = self.residual_returns
            self.data.reset_index(inplace=True)
            self.data.set_index(['as_of_date', 'asset'], inplace=True)
            self.data.dropna(inplace=True)

            return self.data[['return', 'residual_return']]

        @property
        def residual_returns(self):
            try:
                if len(self.data) >= self.window_length:
                    logger.debug('{}\n{}'.format(self.asset, self.model))

                    return self.model.resid
                else:
                    logger.warning('Not enough data to calculate residual returns for asset: {}'.format(self.asset))

                    return np.NaN
            except:
                info = {
                    'num returns': len(self.asset_returns),
                    'num data rows': len(self.data),
                    'min index': self.asset_returns.index.values.min(),
                    'max index': self.asset_returns.index.values.max()
                }

                logger.error('Error for asset: {}\nInfo: {}'.format(self.asset, info))

                raise

        @memoized_property
        def model(self):
            return pd.stats.ols.MovingOLS(
                y=self.data['return'],
                x=self.data[self.factors.columns.values],
                window_type='rolling',
                window=self.window_length,
                intercept=True
            )

        @memoized_property
        def data(self):
            return self.asset_returns.join(self.factors, how='inner')

        @property
        def window_length(self):
            return self.parent_calculator.window_length

        @property
        def factors(self):
            return self.parent_calculator.factors

        @property
        def asset_returns(self):
            return self.parent_calculator.get_returns_for_asset(self.asset)


class MarketNeutralResidualReturnsQuery(ResidualReturnsQuery):

    @memoized_property
    def factors(self):
        return_column = '{}_return'.format(self.on)
        brdf = self.asset_service.get_returns(self.windowed_start_date, self.end_date, [self.benchmark_asset])
        brdf.reset_index(inplace=True)
        brdf = brdf[['as_of_date', return_column]].copy()
        brdf.rename(columns={return_column: 'benchmark_return'}, inplace=True)
        brdf.set_index('as_of_date', inplace=True)

        return brdf

    @memoized_property
    def benchmark_asset(self):
        return self.asset_service.get_asset('SPY')


class FamaFrenchResidualReturnsQuery(ResidualReturnsQuery):

    @memoized_property
    def assets_returns(self):
        if self.on != 'close':
            raise ValueError('For the Fama-French model, the value for "on" can only be "close"!')

        return super(FamaFrenchResidualReturnsQuery, self).assets_returns

    @memoized_property
    def factors(self):
        """
         Possible factor models:
         F-F_Research_Data_Factors_daily
         F-F_Research_Data_5_Factors_2x3_daily
        """
        fama_french_model = 'F-F_Research_Data_5_Factors_2x3_daily'
        df = self.cache_service.get(fama_french_model)

        if df is None:
            reader = DataReader(fama_french_model, 'famafrench')
            df = reader[0]
            df.index.name = 'as_of_date'
            df.drop(['RF'], axis=1, inplace=True)

            df = dfutils.filter(self.windowed_start_date, self.end_date)

            self.cache_service.put(fama_french_model, df)

        return df

