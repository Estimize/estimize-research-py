from abc import abstractmethod


class MarketCapService:

    @abstractmethod
    def get_market_caps(self, start_date=None, end_date=None, assets=None):
        raise NotImplementedError()
