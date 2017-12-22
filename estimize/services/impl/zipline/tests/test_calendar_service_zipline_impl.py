import unittest

from injector import Injector
import pandas as pd

from estimize.di.default_module import DefaultModule
from estimize.services import CalendarService


class CalendarServiceZiplineImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(CalendarService)

    def test_get_trading_days_between(self):
        start_date = '2017-01-03'
        end_date = '2018-01-02'

        dates = self.service.get_trading_days_between(start_date, end_date)

        self.assertIsNotNone(dates)
        self.assertEqual(len(dates), 252)
        self.assertEqual(dates[0], pd.Timestamp(start_date).replace(tzinfo=None))
        self.assertEqual(dates[-1], pd.Timestamp(end_date).replace(tzinfo=None))


if __name__ == '__main__':
    unittest.main()
