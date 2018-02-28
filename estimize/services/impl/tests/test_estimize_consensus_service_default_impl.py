import unittest
from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService
from estimize.services.impl import EstimizeConsensusServiceDefaultImpl


class TestEstimizeConsensusServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.service = injector.get(EstimizeConsensusServiceDefaultImpl)

    def test_get_consensuses(self):
        df = self.service.get_consensuses()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

    def test_get_final_consensuses(self):
        df = self.service.get_final_consensuses()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

    def test_get_final_consensuses_for_AMZN(self):
        assets = self.asset_service.get_assets(['AMZN'])
        df = self.service.get_final_consensuses(assets=assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)


if __name__ == '__main__':
    unittest.main()
