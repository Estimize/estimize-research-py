from abc import abstractmethod


class AssetInfoService:

    @abstractmethod
    def get_asset_info(self, assets=None):
        raise NotImplementedError()
