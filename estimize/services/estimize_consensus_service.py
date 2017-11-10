from abc import abstractmethod

import pandas as pd


class EstimizeConsensusService:

    @abstractmethod
    def get_final_earnings_yields(self, start_date=None, end_date=None) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_earnings_yields(self, start_date=None, end_date=None) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_final_consensuses(self, start_date=None, end_date=None) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_consensuses(self, start_date=None, end_date=None) -> pd.DataFrame:
        raise NotImplementedError()