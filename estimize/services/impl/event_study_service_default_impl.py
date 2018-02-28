import logging
from datetime import timedelta

import pandas as pd
from injector import inject
from memoized_property import memoized_property

from estimize.pandas import dfutils
from estimize.services import EventStudyService, CalendarService, FactorService, AssetService

logger = logging.getLogger(__name__)


class EventStudyServiceDefaultImpl(EventStudyService):

    @inject
    def __init__(self, calendar_service: CalendarService, factor_service: FactorService, asset_service: AssetService):
        self.calendar_service = calendar_service
        self.factor_service = factor_service
        self.asset_service = asset_service

    def run_event_study(self, events: pd.DataFrame, on='open', days_before=10, days_after=5) -> pd.DataFrame:
        return EventStudy(
            calendar_service=self.calendar_service,
            factor_service=self.factor_service,
            asset_service=self.asset_service,
            events=events,
            on=on,
            days_before=days_before,
            days_after=days_after
        ).results()


class EventStudy(object):

    def __init__(self,
                 calendar_service,
                 factor_service,
                 asset_service,
                 events,
                 on,
                 days_before=10,
                 days_after=5
                 ):
        self.calendar_service = calendar_service
        self.factor_service = factor_service
        self.asset_service = asset_service
        self.events = events
        self.on = on
        self.days_before = days_before
        self.days_after = days_after

    def results(self):
        logger.debug('results: start')

        df = self.data
        group_1_cols = list(set(df.columns.values) - set(['residual_return']))
        group_2_cols = list(set(group_1_cols) - set(['event_time']))

        df = df.groupby(group_1_cols)['residual_return'].agg(['mean', 'count'])
        df.rename(columns={df.columns[0]: 'residual_return', df.columns[1]: 'count'}, inplace=True)

        if len(group_2_cols) > 0:
            df.reset_index(inplace=True)
            df.set_index('event_time', inplace=True)
            df['cumulative_residual_return'] = df.groupby(group_2_cols)['residual_return'].cumsum()
        else:
            df['cumulative_residual_return'] = df['residual_return'].cumsum()

        df.reset_index(inplace=True)
        df.set_index(group_1_cols, inplace=True)

        logger.debug('results: end')

        return df

    @property
    def data(self):
        logger.debug('data: start')

        df = self.residual_returns
        df.fillna(0.0, inplace=True)
        df.reset_index(inplace=True)
        df.drop(['as_of_date', 'asset'], axis=1, inplace=True)

        logger.debug('data: end')

        return df

    @property
    def residual_returns(self):
        logger.debug('residual_returns: start')

        df = self.windowed_events.join(self.asset_returns)
        df.reset_index(inplace=True)
        df.set_index(['as_of_date'], inplace=True)

        df = df.join(self.benchmark_returns)
        df['residual_return'] = df['return'] - (df['alpha'] + (df['beta'] * df['benchmark_return']))
        df.drop(['benchmark_return', 'return', 'alpha', 'beta'], axis=1, inplace=True)

        return df

    @property
    def asset_returns(self):
        logger.debug('asset_returns: start')

        df = self.asset_service.get_returns(self.start_date, self.end_date, self.assets)[[self.return_column]]
        df.rename(columns={self.return_column: 'return'}, inplace=True)

        logger.debug('asset_returns: end')

        return df

    @property
    def benchmark_returns(self):
        logger.debug('benchmark_returns: start')

        spy = self.asset_service.get_asset('SPY')
        df = self.asset_service.get_returns(self.start_date, self.end_date, [spy])[[self.return_column]]
        df.reset_index(inplace=True)
        df.set_index(['as_of_date'], inplace=True)
        df.drop(['asset'], axis=1, inplace=True)
        df.rename(columns={self.return_column: 'benchmark_return'}, inplace=True)

        logger.debug('benchmark_returns: end')

        return df

    @memoized_property
    def return_column(self):
        if self.on == 'open':
            return 'open_return'
        else:
            return 'close_return'

    @property
    def windowed_events(self):
        logger.debug('windowed_events: start')

        df = self.events_with_market_factors
        df['event_time'] = 0

        windows = []

        for row in df.iterrows():
            index = row[0]
            cols = row[1]
            date = index[0]
            asset = index[1]

            prior_dates = self.calendar_service.get_n_trading_days_from(-self.days_before, date)
            post_dates = self.calendar_service.get_n_trading_days_from(self.days_after, date)
            dates = prior_dates + post_dates

            event_times = list(range(-self.days_before, 0)) + list(range(1, self.days_after + 1))
            indexes = [(d, asset) for d in dates]
            rdf = pd.DataFrame({'event_time': event_times}, index=indexes)

            for col in cols.index.values:
                if col != 'event_time':
                    rdf[col] = cols[col]

            windows.append(rdf)

        logger.debug('windowed_events: concat start')

        df = pd.concat([df] + windows, copy=False)

        logger.debug('windowed_events: concat end')

        df.sort_index(inplace=True)

        logger.debug('windowed_events: end')

        return df

    @property
    def events_with_market_factors(self):
        logger.debug('events_with_market_factors: start')

        df = self.events_with_estimation_as_of_dates.join(self.market_factors, how='inner')
        df.reset_index(inplace=True)
        df.drop(['as_of_date'], axis=1, inplace=True)
        df.rename(columns={'original_as_of_date': 'as_of_date'}, inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)

        logger.debug('events_with_market_factors: end')

        return df

    @property
    def events_with_estimation_as_of_dates(self):
        logger.debug('events_with_estimation_as_of_dates: start')

        def shift_date(date):
            return self.calendar_service.get_n_trading_days_from(-(self.days_before + 2), date)[0]

        df = self.events.copy()
        df['estimation_as_of_date'] = df.index.get_level_values('as_of_date').map(shift_date)
        df.reset_index(inplace=True)
        df.rename(columns={'as_of_date': 'original_as_of_date'}, inplace=True)
        df.rename(columns={'estimation_as_of_date': 'as_of_date'}, inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)

        logger.debug('events_with_estimation_as_of_dates: end')

        return df

    @property
    def market_factors(self):
        logger.debug('market_factors: start')

        df = self.factor_service.get_market_factors(self.start_date, self.end_date, self.assets)

        logger.debug('market_factors: end')

        return df

    @property
    def assets(self):
        logger.debug('assets: start')

        asset_list = dfutils.unique_assets(self.events)

        logger.debug('assets: end')

        return asset_list

    @memoized_property
    def start_date(self):
        return self.events.index.get_level_values('as_of_date').min() - timedelta(days=self.days_before * 2)

    @memoized_property
    def end_date(self):
        return self.events.index.get_level_values('as_of_date').max() + timedelta(days=self.days_after * 2)
