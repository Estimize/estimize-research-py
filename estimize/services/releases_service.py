import pandas as pd


class ReleasesService:

    def get_releases(self, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
        raise NotImplementedError()
