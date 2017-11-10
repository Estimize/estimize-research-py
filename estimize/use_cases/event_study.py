import logging
from abc import abstractmethod

import pandas as pd

from memoized_property import memoized_property
from estimize.services.residual_returns_service import ResidualReturnsService

logger = logging.getLogger(__name__)


class EventStudy(object):

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

        a = self.asset_service.get_unique_assets(self.events)

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

            prior_dates = self.asset_service.get_n_trading_days_from(-self.days_before, date)
            post_dates = self.asset_service.get_n_trading_days_from(self.days_after, date)
            dates = prior_dates + post_dates

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

    @property
    @abstractmethod
    def events(self):
        pass

    @memoized_property
    def asset_service(self):
        return default_asset_service

    @memoized_property
    def residual_returns_service(self):
        return ResidualReturnsService()

