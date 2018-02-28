import pandas as pd
from injector import inject

import estimize.config as cfg
from estimize.services import EstimatesService, CsvDataService, CacheService, CalendarService


class EstimatesServiceDefaultImpl(EstimatesService):

    @inject
    def __init__(self, cache_service: CacheService, csv_data_service: CsvDataService, calendar_service: CalendarService):
        self.cache_service = cache_service
        self.csv_data_service = csv_data_service
        self.calendar_service = calendar_service

    def get_estimates(self) -> pd.DataFrame:
        cache_key = 'estimates'
        df = self.cache_service.get(cache_key)

        if df is None:
            df = pd.read_csv('{}/estimates.csv'.format(cfg.data_dir()))
            df['created_at'] = pd.to_datetime(df['created_at'])
            df.set_index(['created_at', 'release_id'], inplace=True)

            self.cache_service.put(cache_key, df)

        return df
