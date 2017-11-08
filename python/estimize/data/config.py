import os
import re

from memoized_property import memoized_property
from zipline.data.bundles.core import load
from zipline.finance.trading import TradingEnvironment
from zipline.pipeline import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.utils.calendars import get_calendar


class Config(object):

    @memoized_property
    def pipeline_engine(self):
        pipeline_loader = USEquityPricingLoader(
            self.bundle_data.equity_daily_bar_reader,
            self.bundle_data.adjustment_reader,
        )

        def get_loader(column):
            if column in USEquityPricing.columns:
                return pipeline_loader
            raise ValueError(
                "No PipelineLoader registered for column %s." % column
            )

        return SimplePipelineEngine(
            get_loader,
            self.trading_calendar.all_sessions,
            self.asset_finder
        )

    @memoized_property
    def asset_finder(self):
        return self.trading_environment.asset_finder

    @memoized_property
    def trading_calendar(self):
        return get_calendar('NYSE')

    @memoized_property
    def trading_environment(self):
        prefix, connstr = re.split(
            r'sqlite:///',
            str(self.bundle_data.asset_finder.engine.url),
            maxsplit=1,
        )

        if prefix:
            raise ValueError(
                "invalid url %r, must begin with 'sqlite:///'" %
                str(self.bundle_data.asset_finder.engine.url),
            )

        return TradingEnvironment(asset_db_path=connstr, environ=os.environ)

    @memoized_property
    def bundle_data(self):
        return load('quantopian-quandl')
