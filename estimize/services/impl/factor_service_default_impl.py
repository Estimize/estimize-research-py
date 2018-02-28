import logging

from injector import inject

import estimize.config as cfg
from estimize.pandas import dfutils
from estimize.services import FactorService, CacheService, AssetService, CalendarService, EstimizeConsensusService, \
    CsvDataService
from memoized_property import memoized_property
import multiprocessing as mp
import pathos.pools as pp
import numpy as np
import pandas as pd
import statsmodels.api as sm

logger = logging.getLogger(__name__)
counter = mp.Value('i', 0)


class FactorServiceDefaultImpl(FactorService):

    @inject
    def __init__(self, cache_service: CacheService, estimize_consensus_service: EstimizeConsensusService,
                 calendar_service: CalendarService, csv_data_service: CsvDataService, asset_service: AssetService):
        self.cache_service = cache_service
        self.estimize_consensus_service = estimize_consensus_service
        self.calendar_service = calendar_service
        self.csv_data_service = csv_data_service
        self.asset_service = asset_service

    def get_market_factors(self, start_date=None, end_date=None, assets=None, use_cache=True) -> pd.DataFrame:
        logger.info('get_market_factors: start')

        cache_key = 'market_factors'
        df = self.cache_service.get(cache_key)

        if df is None:
            try:
                df = self.csv_data_service.get_from_url(
                    url='{}/market_factors.csv'.format(cfg.ROOT_DATA_URL),
                    post_func=self._post_func,
                    date_column='as_of_date',
                    timezone='US/Eastern',
                    symbol_column='ticker'
                )
            except:
                assets = dfutils.unique_assets(self.estimize_consensus_service.get_final_consensuses())
                df = MarketFactorModelQuery(
                    asset_service=self.asset_service,
                    calendar_service=self.calendar_service,
                    assets=assets
                ).results()

                # Save the generated file as a csv
                csv_df = df.reset_index()
                csv_df['ticker'] = csv_df['asset'].map(lambda a: a.symbol)
                csv_df.drop(['asset'], axis=1, inplace=True)
                csv_df.set_index(['as_of_date', 'ticker'], inplace=True)
                csv_df.to_csv('{}/market_factors.csv'.format(cfg.data_dir()))

            self.cache_service.put(cache_key, df)

        df = dfutils.filter(df, start_date, end_date, assets)

        logger.info('get_market_factors: end')

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


class MarketFactorModelQuery:

    ASYNC_DEBUG = False
    DEBUG = False

    def __init__(self, asset_service: AssetService, calendar_service: CalendarService, assets, window_length=126):
        self.asset_service = asset_service
        self.calendar_service = calendar_service
        self.assets = assets
        self.window_length = window_length
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

        self.benchmark_returns
        self.asset_returns

        if self.DEBUG:
            assets = self.assets[0:5]
        else:
            assets = self.assets

        dfs = self.pool.map(self.calculate_factors, self.calculations(assets))
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
            bdf = self.calculation(asset).results()
            dfs.append(bdf)

        df = pd.concat(dfs, copy=False)

        logger.info('results_debug: end')

        return df

    @memoized_property
    def benchmark_returns(self):
        spy = self.asset_service.get_asset('SPY')
        mrdf = self.asset_service.get_returns(self.windowed_start_date, self.end_date, [spy])[['close_return']]
        mrdf.reset_index(inplace=True)
        mrdf.set_index('as_of_date', inplace=True)
        mrdf.drop(['asset'], axis=1, inplace=True)
        mrdf.rename(columns={'close_return': 'market_return'}, inplace=True)

        return mrdf

    @memoized_property
    def asset_returns(self):
        ardf = self.asset_service.get_returns(self.windowed_start_date, self.end_date, self.assets)[['close_return']]
        ardf.rename(columns={'close_return': 'return'}, inplace=True)

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

    def returns_for_asset(self, asset):
        ardf = self.asset_returns[self.asset_returns.index.get_level_values('asset') == asset].copy()
        ardf.reset_index(inplace=True)
        ardf.set_index('as_of_date', inplace=True)
        ardf.dropna(inplace=True)
        ardf.sort_index(inplace=True)

        return ardf

    @staticmethod
    def calculate_factors(calculation):
        return calculation.results()

    def calculations(self, assets):
        return [self.calculation(asset) for asset in assets]

    def calculation(self, asset):
        return self.AssetFactorCalculation(self.asset_returns, self.benchmark_returns, asset, self.window_length, len(self.assets))

    class AssetFactorCalculation:

        def __init__(self, asset_returns, benchmark_returns, asset, window_length, num_assets):
            self.asset_returns = asset_returns
            self.benchmark_returns = benchmark_returns
            self.asset = asset
            self.window_length = window_length
            self.num_assets = num_assets

        def results(self):
            print(self.asset)
            rdf = self.observations
            # print(rdf.head())

            if len(rdf) >= self.window_length:
                rdf['alpha'] = np.nan
                rdf['beta'] = np.nan

                for i in range(self.window_length, len(rdf) + 1):
                    # print('i:', i)
                    wdf = rdf.iloc[i - self.window_length:i]
                    # print(len(wdf))

                    y = wdf['return']
                    x = wdf['market_return']
                    x = sm.add_constant(x)
                    model = sm.OLS(y, x).fit()

                    index = rdf.index[i - 1]

                    if model.pvalues.market_return <= 0.05:
                        rdf.at[index, 'alpha'] = model.params.const
                        rdf.at[index, 'beta'] = model.params.market_return
                    else:
                        rdf.at[index, 'alpha'] = 0
                        rdf.at[index, 'beta'] = 0

                rdf.dropna(inplace=True)
                rdf.drop(['return', 'market_return'], axis=1, inplace=True)
            else:
                logger.info('Not enough data to calculate model for: {}'.format(self.asset))
                rdf = None

            with counter.get_lock():
                counter.value += 1
                progress = (float(counter.value) / self.num_assets) * 100

                logger.info('progress: {}%'.format(progress))

            return rdf

        @property
        def observations(self):
            df = self.returns.join(self.benchmark_returns, how='inner')
            # print(df.head())
            df.reset_index(inplace=True)
            df.set_index(['as_of_date', 'asset'], inplace=True)

            return df

        @property
        def returns(self):
            df = self.asset_returns[self.asset_returns.index.get_level_values('asset') == self.asset].copy()
            df.reset_index(inplace=True)
            df.set_index('as_of_date', inplace=True)
            df.dropna(inplace=True)
            df.sort_index(inplace=True)

            return df
