import pandas as pd


class ResidualReturnsService:

    def get_market_neutral_residual_returns(self, start_date, end_date, assets=None, on='close') -> pd.DataFrame:
        raise NotImplementedError()

    def get_multi_factor_residual_returns(self, start_date, end_date, assets=None, on='close') -> pd.DataFrame:
        raise NotImplementedError()
