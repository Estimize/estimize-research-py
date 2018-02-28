from abc import abstractmethod

import pandas as pd


class EstimizeConsensusService:

    @abstractmethod
    def get_final_consensuses(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_consensuses(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        raise NotImplementedError()
