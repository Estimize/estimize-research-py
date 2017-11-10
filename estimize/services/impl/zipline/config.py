from memoized_property import memoized_property
from sqlalchemy import create_engine
from zipline.assets import AssetDBWriter, AssetFinder
from zipline.data.bundles import register
from zipline.data.bundles.core import load
from zipline.pipeline import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.utils.calendars import get_calendar

from estimize.zipline.data.bundles.yahoo import yahoo_bundle


class Config:

    __bundles = dict()

    def __init__(self, bundle_name='quantopian-quandl'):
        self.bundle_name = bundle_name

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
        AssetDBWriter(self.db_engine).init_db()

        return AssetFinder(self.db_engine)

    @memoized_property
    def trading_calendar(self):
        return get_calendar('NYSE')

    @memoized_property
    def db_engine(self):
        return create_engine(self.bundle_data.asset_finder.engine.url)

    @memoized_property
    def bundle_data(self):
        return load(self.bundle_name)


class YahooConfig(Config):

    YAHOO_TICKERS = {
        'SPY',
    }

    register(
        'yahoo',
        yahoo_bundle(YAHOO_TICKERS),
    )

    def __init__(self):
        super(YahooConfig, self).__init__(bundle_name='yahoo')
