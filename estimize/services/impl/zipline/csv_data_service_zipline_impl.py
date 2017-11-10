from injector import inject
from pandas import read_csv
from zipline.sources.requests_csv import PandasCSV, PandasRequestsCSV

from estimize.services.csv_data_service import CsvDataService
from estimize.services.impl.zipline import Config


class CsvDataServiceZiplineImpl(CsvDataService):

    MAX_DOCUMENT_SIZE = (1024 * 1024) * 1000  # 1Gb

    @inject
    def __init__(self, config: Config):
        self.config = config

    def get_from_file(self,
                      filename,
                      pre_func=None,
                      post_func=None,
                      date_column='date',
                      date_format='%m/%d/%y',
                      timezone='UTC',
                      symbol_column='symbol',
                      **kwargs):

        pre_func = pre_func or False
        post_func = post_func or False

        file = open(filename)
        csv = PandasFileCSV(
            file=file,
            pre_func=pre_func,
            post_func=post_func,
            asset_finder=self.config.asset_finder,
            trading_day=None,
            start_date=None,
            end_date=None,
            date_column=date_column,
            date_format=date_format,
            timezone=timezone,
            symbol=None,
            mask=True,
            symbol_column=symbol_column,
            data_frequency=None,
            **kwargs
        )

        return csv.load_df()

    def get_from_url(self,
                     url,
                     pre_func=None,
                     post_func=None,
                     date_column='date',
                     date_format='%m/%d/%y',
                     timezone='UTC',
                     symbol_column='symbol',
                     **kwargs):

        pre_func = pre_func or False
        post_func = post_func or False

        PandasRequestsCSV.MAX_DOCUMENT_SIZE = self.MAX_DOCUMENT_SIZE
        csv = PandasRequestsCSV(
            url=url,
            pre_func=pre_func,
            post_func=post_func,
            asset_finder=self.config.asset_finder,
            trading_day=None,
            start_date=None,
            end_date=None,
            date_column=date_column,
            date_format=date_format,
            timezone=timezone,
            symbol=None,
            mask=True,
            symbol_column=symbol_column,
            data_frequency=None,
            **kwargs
        )

        return csv.load_df()


class PandasFileCSV(PandasCSV):

    def __init__(self,
                 file,
                 pre_func,
                 post_func,
                 asset_finder,
                 trading_day,
                 start_date,
                 end_date,
                 date_column,
                 date_format,
                 timezone,
                 symbol,
                 mask,
                 symbol_column,
                 data_frequency,
                 **kwargs):
        self.file = file

        super(PandasFileCSV, self).__init__(
            pre_func,
            post_func,
            asset_finder,
            trading_day,
            start_date,
            end_date,
            date_column,
            date_format,
            timezone,
            symbol,
            mask,
            symbol_column,
            data_frequency,
            **kwargs
        )

    def fetch_data(self):
        df = read_csv(self.file, **self.pandas_kwargs)
        return df
