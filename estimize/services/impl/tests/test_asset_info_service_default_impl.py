import unittest
from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services.impl.asset_info_service_default_impl import AssetInfoServiceDefaultImpl


class TestAssetInfoServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.service = injector.get(AssetInfoServiceDefaultImpl)

    def test_asset_info(self):
        df = self.service.get_asset_info()

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
