from injector import inject
from memoized_property import memoized_property
import pandas as pd
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, Returns, SimpleMovingAverage
from zipline.pipeline.filters import StaticAssets

from estimize.services import AssetService, CalendarService
from estimize.services.impl.zipline import Config, YahooConfig
from estimize.zipline.pipeline.factors.technical import InterDayReturns, IntraDayReturns


class AssetServiceZiplineImpl(AssetService):

    @inject
    def __init__(self, config: Config, yahoo_config: YahooConfig, calendar_service: CalendarService):
        self.config = config
        self.yahoo_config = yahoo_config
        self.calendar_service = calendar_service

    def get_assets(self, tickers):
        return self.get_service(tickers).get_assets(tickers)

    def get_moving_average(self, start_date, end_date, assets=None, window_length=63) -> pd.DataFrame:
        return self.get_service(self.get_tickers(assets)).get_moving_average(start_date, end_date, assets, window_length)

    def get_returns(self, start_date, end_date, assets=None) -> pd.DataFrame:
        return self.get_service(self.get_tickers(assets)).get_returns(start_date, end_date, assets)

    def get_universe(self, start_date, end_date, assets=None, min_avg_dollar_vol=1e6, min_price=4.0) -> pd.DataFrame:
        return self.asset_service.get_universe(start_date, end_date, assets, min_avg_dollar_vol, min_price)

    def get_service(self, tickers):
        if len(self.yahoo_config.YAHOO_TICKERS.intersection(tickers)) > 0:
            return self.yahoo_asset_service
        else:
            return self.asset_service

    @staticmethod
    def get_tickers(assets):
        if assets is not None:
            return set(asset.symbol for asset in assets)
        else:
            return set()

    @memoized_property
    def asset_service(self):
        return AssetServiceZiplineConfigImpl(self.config, self.calendar_service)

    @memoized_property
    def yahoo_asset_service(self):
        return AssetServiceZiplineConfigImpl(self.yahoo_config, self.calendar_service)


class AssetServiceZiplineConfigImpl(AssetService):

    def __init__(self, config: Config, calendar_service: CalendarService):
        self.config = config
        self.calendar_service = calendar_service

    def get_assets(self, tickers):
        return self.asset_finder.lookup_symbols(tickers, None)

    def get_moving_average(self, start_date, end_date, assets=None, window_length=63):
        moving_average = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=window_length)

        pipeline = Pipeline(
            columns={
                'moving_average': moving_average
            }
        )

        if assets is not None:
            pipeline.set_screen(StaticAssets(assets))

        df = self._run_pipeline(pipeline, start_date, end_date)

        return df

    def get_returns(self, start_date, end_date, assets=None):
        open_return = Returns(window_length=2, inputs=[USEquityPricing.open])
        close_return = Returns(window_length=2, inputs=[USEquityPricing.close])
        inter_day_return = InterDayReturns()
        intra_day_return = IntraDayReturns()

        pipeline = Pipeline(
            columns={
                'open_return': open_return,
                'close_return': close_return,
                'inter_day_return': inter_day_return,
                'intra_day_return': intra_day_return
            }
        )

        if assets is not None:
            pipeline.set_screen(StaticAssets(assets))

        df = self._run_pipeline(pipeline, start_date, end_date)
        df['open_return'] = df.groupby(df.index.get_level_values('asset'))['open_return'].shift(-1)
        df.dropna(inplace=True)

        return df

    def get_universe(self, start_date, end_date, assets=None, min_avg_dollar_vol=1e6, min_price=4.0):
        adv = AverageDollarVolume(window_length=20)
        latest_close = USEquityPricing.close.latest

        # min_market_cap = market_cap >= 100e6 # Need market cap data
        min_adv = (adv >= min_avg_dollar_vol)
        min_latest_close = (latest_close >= min_price)
        screen = (min_adv & min_latest_close)

        if assets is not None:
            screen = (StaticAssets(assets) & screen)

        pipeline = Pipeline(
            columns={
                'latest_close': latest_close
            },
            screen=screen
        )

        df = self._run_pipeline(pipeline, start_date, end_date)
        df.drop(['latest_close'], axis=1, inplace=True)

        return df

    @property
    def pipeline_engine(self):
        return self.config.pipeline_engine

    @property
    def asset_finder(self):
        return self.config.asset_finder

    @property
    def trading_calendar(self):
        return self.config.trading_calendar

    def _run_pipeline(self, pipeline, start_date, end_date):
        start_date = self.calendar_service.get_valid_trading_start_date(start_date).tz_localize('UTC')
        end_date = self.calendar_service.get_valid_trading_end_date(end_date).tz_localize('UTC')

        df = self.pipeline_engine.run_pipeline(pipeline, start_date, end_date)
        df.index.rename(['as_of_date', 'asset'], inplace=True)
        df.reset_index(inplace=True)
        df = pd.pivot_table(df, index='as_of_date', columns='asset')
        df = df.shift(-1).stack()
        df.reset_index(inplace=True)
        df['as_of_date'] = df['as_of_date'].dt.date
        df.set_index(['as_of_date', 'asset'], inplace=True)

        return df
