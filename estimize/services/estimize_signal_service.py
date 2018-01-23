from abc import abstractmethod

import pandas as pd


class EstimizeSignalService:

    @abstractmethod
    def get_signals(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        raise NotImplementedError()
