from abc import abstractmethod
import pandas as pd


class CsvDataService:

    @abstractmethod
    def get_from_file(self,
                      filename,
                      pre_func=None,
                      post_func=None,
                      date_column='date',
                      date_format='%m/%d/%y',
                      timezone='UTC',
                      symbol_column='symbol',
                      **kwargs) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def get_from_url(self,
                     url,
                     pre_func=None,
                     post_func=None,
                     date_column='date',
                     date_format='%m/%d/%y',
                     timezone='UTC',
                     symbol_column='symbol',
                     **kwargs) -> pd.DataFrame:
        raise NotImplementedError()
