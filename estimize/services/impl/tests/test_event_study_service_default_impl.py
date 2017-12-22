import unittest
import logging
from injector import Injector
import pandas as pd

from estimize.di.default_module import DefaultModule
from estimize.services import EstimizeConsensusService, AssetService
from estimize.services.impl import EventStudyServiceDefaultImpl
from estimize.logging import configure_logging


class TestEventStudyServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        configure_logging(logging.DEBUG)
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.estimize_consensus_service = injector.get(EstimizeConsensusService)
        self.service = injector.get(EventStudyServiceDefaultImpl)

    def test_run_event_study(self):
        start_date = '2016-01-01'
        end_date = '2017-01-01'
        aapl = self.asset_service.get_asset('AAPL')
        events = self.estimize_consensus_service.get_final_consensuses(start_date, end_date, [aapl])
        events = events[[]]
        df = self.service.run_event_study(events=events)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)

    def test_run_event_study_with_group_columns(self):
        start_date = '2012-01-01'
        end_date = '2017-01-01'
        aapl = self.asset_service.get_asset('AAPL')
        events = self.estimize_consensus_service.get_final_consensuses(start_date, end_date, [aapl])
        events['year'] = pd.to_datetime(events['reports_at_date']).dt.year
        events = events[['year']]
        df = self.service.run_event_study(events=events)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
