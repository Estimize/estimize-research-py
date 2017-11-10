from abc import abstractmethod
import pandas as pd


class AssetService:

    TIMEZONE = 'US/Eastern'
    ASSET_COLUMN = 'asset'

    def get_asset(self, ticker):
        return self.get_assets([ticker])[0]

    @abstractmethod
    def get_assets(self, tickers):
        raise NotImplementedError()

    @abstractmethod
    def get_moving_average(self, start_date, end_date, assets=None, window_length=63):
        raise NotImplementedError()

    @abstractmethod
    def get_returns(self, start_date, end_date, assets=None) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_universe(self, start_date, end_date, assets=None) -> pd.DataFrame:
        raise NotImplementedError()
