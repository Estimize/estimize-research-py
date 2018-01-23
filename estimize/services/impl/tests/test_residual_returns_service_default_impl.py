import unittest
import logging
from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services.impl import ResidualReturnsServiceDefaultImpl
from estimize.logging import configure_logging


class TestResidualReturnsServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        configure_logging(logging.DEBUG)
        injector = Injector([DefaultModule])
        self.service = injector.get(ResidualReturnsServiceDefaultImpl)

    def test_market_neutral_residual_returns(self):
        start_date = '2017-01-01'
        end_date = '2017-02-01'
        df = self.service.get_market_neutral_residual_returns(start_date, end_date, on='open')

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)


if __name__ == '__main__':
    unittest.main()
