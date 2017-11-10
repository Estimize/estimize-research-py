from abc import abstractmethod


class CalendarService:

    @abstractmethod
    def get_valid_trading_start_date(self, start_date):
        raise NotImplementedError()

    @abstractmethod
    def get_valid_trading_end_date(self, end_date):
        raise NotImplementedError()

    @abstractmethod
    def get_n_trading_days_from(self, n, date):
        raise NotImplementedError()
