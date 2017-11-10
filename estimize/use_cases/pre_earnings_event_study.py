import logging
import pandas as pd

from datetime import timedelta
from memoized_property import memoized_property
from estimize.use_cases import PostEarningsEventStudy


logger = logging.getLogger(__name__)


class PreEarningsEventStudy(PostEarningsEventStudy):

    @memoized_property
    def assets(self):
        logger.debug('assets: start')

        df = self.estimize_consensus_service.get_final_earnings_yields(self.start_date, self.end_date)
        a = self.asset_service.get_unique_assets(df)

        logger.debug('assets: end')

        return a

    @memoized_property
    def estimize_eps_deltas(self):
        logger.debug('estimize_eps_deltas: start')

        comparison_yield = 'estimize.eps.weighted'

        df = self.estimize_consensus_service.get_earnings_yields(self.start_date, self.end_date)
        df = df.loc[:, ['bmo', 'reports_at_date', comparison_yield]]
        df.reset_index(inplace=True)
        df.set_index(['asset', 'reports_at_date'], inplace=True)
        df.sort_index(inplace=True)

        logger.debug('estimize_eps_deltas: groupby start')
        df['eps_delta'] = df[comparison_yield].groupby(df.index).pct_change(7)
        df.dropna(inplace=True)
        logger.debug('estimize_eps_deltas: groupby end')

        df.reset_index(inplace=True)
        df.set_index(['as_of_date', 'asset'], inplace=True)

        df['event_date'] = pd.to_datetime(df['reports_at_date'])
        df['event_date'] -= timedelta(days=7)

        logger.debug('estimize_eps_deltas: filter start')
        df = df[df.index.get_level_values('as_of_date') == df['reports_at_date']]
        logger.debug('estimize_eps_deltas: filter end')

        df.drop(['reports_at_date', comparison_yield], axis=1, inplace=True)

        logger.debug('estimize_eps_deltas: end')

        return df

    @staticmethod
    def add_fiscal_date(df):
        df['fiscal_date'] = (df['fiscal_year'] * 10) + df['fiscal_quarter']
        df.drop(['fiscal_year', 'fiscal_quarter'], axis=1, inplace=True)


class PreEarningsPricingEventStudy(PostEarningsEventStudy):

    @memoized_property
    def events(self):
        logger.debug('events: start')

        df = self.estimize_consensus_service.get_final_earnings_yields(self.start_date, self.end_date)
        df.reset_index(inplace=True)
        df['as_of_date'] = df['as_of_date'].dt.date - timedelta(days=1)
        df.set_index(['as_of_date', 'asset'], inplace=True)
        self.fix_index(df)

        rdf = self.residual_returns['residual_return'].groupby(self.residual_returns.index.get_level_values('asset')).cumsum().pct_change(2).to_frame()

        df = df.join(rdf)
        df.fillna(0.0, inplace=True)

        df['decile'] = pd.cut(df['residual_return'].values, 5, labels=False) + 1
        df.drop(['residual_return'], axis=1, inplace=True)

        logger.debug('events: end')

        return df

    @memoized_property
    def residual_returns(self):
        return super(PreEarningsPricingEventStudy, self).residual_returns
