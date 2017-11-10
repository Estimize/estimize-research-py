import logging
import pandas as pd

from datetime import timedelta
from memoized_property import memoized_property

from estimize.services.estimize_consensus_service import EstimizeConsensusService
from estimize.services.residual_returns_service import ResidualReturnsService


logger = logging.getLogger(__name__)


class PostEarningsEventStudy:

    def __init__(self,
                 start_date,
                 end_date,
                 days_before=10,
                 days_after=5
                 ):
        self.start_date = start_date
        self.end_date = end_date
        self.days_before = days_before
        self.days_after = days_after

    @memoized_property
    def results(self):
        logger.debug('results: start')

        df = self.data
        df = df.groupby(['decile', 'event_time'])['residual_return'].mean().to_frame()
        df['cumulative_residual_return'] = df.groupby(df.index.get_level_values('decile')).cumsum()
        df.reset_index(inplace=True)
        df.set_index(['decile', 'event_time'], inplace=True)

        logger.debug('results: end')

        return df

    @property
    def data(self):
        logger.debug('data: start')

        df = self.windowed_events.join(self.residual_returns)
        df.fillna(0.0, inplace=True)
        df.reset_index(inplace=True)
        df.drop(['as_of_date', 'asset'], axis=1, inplace=True)

        logger.debug('data: end')

        return df

    @property
    def residual_returns(self):
        logger.debug('residual_returns: start')

        df = self.residual_returns_service.market_neutral_residual_returns(self.start_date, self.end_date, self.assets)

        logger.debug('residual_returns: end')

        return df

    @property
    def assets(self):
        logger.debug('assets: start')

        a = self.asset_service.get_unique_assets(self.estimize_eps_deltas)

        logger.debug('assets: end')

        return a

    @property
    def windowed_events(self):
        logger.debug('windowed_events: start')

        df = self.events
        df['event_time'] = 0

        windows = []

        for row in self.events.iterrows():
            index = row[0]
            cols = row[1]
            date = index[0]
            asset = index[1]
            decile = cols['decile']

            dates = self.event_dates(date)
            event_times = range(-self.days_before, 0) + range(1, self.days_after + 1)
            indexes = [(d, asset) for d in dates]
            rdf = pd.DataFrame({'event_time': event_times}, index=indexes)
            rdf['decile'] = decile

            windows.append(rdf)

        logger.debug('windowed_events: concat start')

        df = pd.concat([df] + windows, copy=False)

        logger.debug('windowed_events: concat end')

        df.sort_index(inplace=True)

        logger.debug('windowed_events: end')

        return df

    @memoized_property
    def events(self):
        logger.debug('events: start')

        df = self.estimize_eps_deltas.copy()
        self.fix_index(df)

        df['decile'] = pd.qcut(df['eps_delta'].values, 5, labels=False) + 1

        df.drop(['bmo', 'eps_delta'], axis=1, inplace=True)

        logger.debug('events: end')

        return df

    @memoized_property
    def estimize_eps_deltas(self):
        logger.debug('estimize_eps_deltas: start')

        comparison_yield = 'estimize.eps.weighted.yield'
        # comparison_yield = 'wallstreet.eps.yield'

        df = self.estimize_consensus_service.get_final_earnings_yields(self.start_date, self.end_date)
        df = df[['bmo', 'actual.eps.yield', comparison_yield]]
        df['eps_delta'] = df['actual.eps.yield'] - df[comparison_yield]
        df.drop(['actual.eps.yield', comparison_yield], axis=1, inplace=True)

        logger.debug('estimize_eps_deltas: end')

        return df

    @memoized_property
    def estimize_consensus_service(self):
        return EstimizeConsensusService()

    @memoized_property
    def asset_service(self):
        return default_asset_service

    @memoized_property
    def residual_returns_service(self):
        return ResidualReturnsService()

    def fix_index(self, df):
        df.index = pd.MultiIndex.from_tuples(map(self.fix_row_index, df.iterrows()))
        df.index.names = ['as_of_date', 'asset']

    def fix_row_index(self, row):
        index = row[0]
        cols = row[1]
        date = index[0]
        asset = index[1]

        if cols['bmo']:
            date = date - timedelta(days=1)

        date = self.asset_service.get_valid_trading_end_date(date)

        return date, asset

    def event_dates(self, date):
        prior_dates = self.asset_service.get_n_trading_days_from(-self.days_before, date)
        post_dates = self.asset_service.get_n_trading_days_from(self.days_after, date)

        return prior_dates + post_dates
