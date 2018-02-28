import unittest

from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService, ReleasesService


class TestReleasesServiceDefaultImpl(unittest.TestCase):

    def setUp(self):
        injector = Injector([DefaultModule])
        self.asset_service = injector.get(AssetService)
        self.service = injector.get(ReleasesService)

    def test_get_releases(self):
        start_date = '2012-01-01'
        end_date = '2017-01-01'
        assets = self.asset_service.get_assets(['AAPL'])
        df = self.service.get_releases(start_date, end_date, assets)

        self.assertIsNotNone(df)
        self.assertFalse(df.empty)

        print(df)


if __name__ == '__main__':
    unittest.main()
