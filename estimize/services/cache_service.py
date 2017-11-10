from abc import abstractmethod
import pandas as pd


class CacheService:

    @abstractmethod
    def get(self, key: str) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def put(self, key: str, df: pd.DataFrame):
        raise NotImplementedError()
