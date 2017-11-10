import unittest
from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services.impl import EstimizeConsensusServiceDefaultImpl


class TestEstimizeConsensusServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(EstimizeConsensusServiceDefaultImpl)

    def test_get_consensuses(self):
        df = self.service.get_consensuses()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

    def test_get_final_consensuses(self):
        df = self.service.get_final_consensuses()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

    def test_get_earnings_yields(self):
        df = self.service.get_earnings_yields()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

    def test_get_final_earnings_yields(self):
        df = self.service.get_final_earnings_yields()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)


if __name__ == '__main__':
    unittest.main()
