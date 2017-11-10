import pandas as pd
from injector import inject

from estimize.services import CalendarService
from . import Config


class CalendarServiceZiplineImpl(CalendarService):

    @inject
    def __init__(self, config: Config):
        self.config = config

    def get_valid_trading_start_date(self, start_date):
        start_date = pd.Timestamp(start_date, tz='UTC')

        if self.trading_calendar.is_session(start_date):
            return start_date.replace(tzinfo=None)
        else:
            return self.trading_calendar.next_open(start_date).normalize().replace(tzinfo=None)

    def get_valid_trading_end_date(self, end_date):
        end_date = pd.Timestamp(end_date, tz='UTC')

        if self.trading_calendar.is_session(end_date):
            return end_date.replace(tzinfo=None)
        else:
            return self.trading_calendar.previous_open(end_date).normalize().replace(tzinfo=None)

    def get_n_trading_days_from(self, n, date):
        if n == 0:
            dates = []
        else:
            dates = self.trading_calendar.sessions_window(date, n)
            dates = [d.replace(tzinfo=None) for d in dates]

            if n < 0:
                dates = dates[:-1]
            else:
                dates = dates[1:]

        return dates

    @property
    def trading_calendar(self):
        return self.config.trading_calendar
