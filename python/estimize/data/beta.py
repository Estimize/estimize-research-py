import scipy.stats as ss

from datetime import timedelta
from memoized_property import memoized_property
from estimize.data.core import *


class BetaCalculator(object):
    def __init__(self,
                 start_date,
                 end_date,
                 assets,
                 window_length=365
                 ):

        self.start_date = get_valid_start_date(start_date)
        self.end_date = get_valid_end_date(end_date)
        self.assets = assets
        self.window_length = window_length

    def call(self):
        df = None

        for asset in self.assets:
            bdf = self.AssetBetaCalculator(self, asset).call()

            if df is None:
                df = bdf
            else:
                df = pd.concat([df, bdf])

        return df

    @memoized_property
    def benchmark_returns(self):
        brdf = get_benchmark_returns(self.windowed_start_date, self.end_date)
        brdf.reset_index(inplace=True)
        brdf.rename(columns={'return': 'benchmark_return'}, inplace=True)
        brdf.set_index('as_of_date', inplace=True)

        return brdf

    @memoized_property
    def assets_returns(self):
        ardf = get_returns(self.windowed_start_date, self.end_date, self.assets)
        ardf.reset_index(inplace=True)
        ardf.rename(columns={'return': 'asset_return'}, inplace=True)
        ardf.set_index('as_of_date', inplace=True)

        return ardf

    @memoized_property
    def windowed_start_date(self):
        return self.start_date - timedelta(days=self.window_length)

    def get_asset_returns(self, asset):
        return self.assets_returns[self.assets_returns['symbol'] == asset].copy()

    class AssetBetaCalculator(object):
        def __init__(self, parent_calculator, asset):
            print asset
            self.parent_calculator = parent_calculator
            self.asset = asset

        def call(self):
            print self.model

            self.data['beta'] = self.model.y_predict

            return self.data

        @memoized_property
        def model(self):
            return pd.stats.ols.MovingOLS(
                y=self.data['asset_return'],
                x=self.data[['benchmark_return']],
                window_type='rolling',
                window=self.window_length,
                intercept=True
            )

        @memoized_property
        def data(self):
            return self.asset_returns.join(self.benchmark_returns, how='inner')

        @property
        def window_length(self):
            return self.parent_calculator.window_length

        @property
        def benchmark_returns(self):
            return self.parent_calculator.benchmark_returns

        @property
        def asset_returns(self):
            return self.parent_calculator.get_asset_returns(self.asset)
