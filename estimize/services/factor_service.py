import pandas as pd


class FactorService:

    def get_market_factors(self, start_date=None, end_date=None, assets=None, use_cache=True) -> pd.DataFrame:
        raise NotImplementedError()
