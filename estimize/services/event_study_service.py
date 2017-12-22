import pandas as pd


class EventStudyService:

    def run_event_study(self, events: pd.DataFrame, on='open', days_before=10, days_after=5) -> pd.DataFrame:
        raise NotImplementedError()
