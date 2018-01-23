from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import Returns


class IntraDayReturns(Returns):

    inputs = [USEquityPricing.open, USEquityPricing.close]
    window_length = 2

    def compute(self, today, assets, out, open, close):
        out[:] = (close[-1] - open[-1]) / open[-1]


class InterDayReturns(Returns):

    inputs = [USEquityPricing.open, USEquityPricing.close]
    window_length = 2

    def compute(self, today, assets, out, open, close):
        out[:] = (open[-1] - close[0]) / close[0]
