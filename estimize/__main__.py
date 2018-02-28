import errno
import os
import pandas as pd
from functools import wraps

import click
import logbook
from injector import Injector

from zipline.data import bundles as bundles_module

from estimize.di.default_module import DefaultModule
from estimize.services import AssetService, AssetInfoService, EstimatesService, ReleasesService, \
    EstimizeConsensusService, EstimizeSignalService, MarketCapService, FactorService


@click.group()
def main():
    """Top level estimize entry point.
        """
    # install a logbook handler before performing any other operations
    logbook.StderrHandler().push_application()


@main.command()
def init():
    print('Initializing...')
    from zipline.data.bundles import register
    from estimize.zipline.data.bundles.yahoo import yahoo_bundle

    tickers = {
        'SPY',
    }

    register(
        'yahoo',
        yahoo_bundle(tickers),
    )

    bundles_module.ingest(
        'yahoo',
        os.environ,
        pd.Timestamp.utcnow(),
        [],
        True,
    )

    bundles_module.ingest(
        'quantopian-quandl',
        os.environ,
        pd.Timestamp.utcnow(),
        [],
        True,
    )

    injector = Injector([DefaultModule])
    asset_info_service = injector.get(AssetInfoService)
    estimates_service = injector.get(EstimatesService)
    estimize_consensus_service = injector.get(EstimizeConsensusService)
    estimize_signal_service = injector.get(EstimizeSignalService)
    factor_serivce = injector.get(FactorService)
    market_cap_service = injector.get(MarketCapService)
    releases_service = injector.get(ReleasesService)

    missing_csv_warning = 'Make sure you have added {} to your ./data directory.'
    remote_csv_warning = 'There was an issue downloading {}, make sure you are connected to the internet.'

    actions = [
        ('instruments.csv', asset_info_service.get_asset_info, missing_csv_warning),
        ('estimates.csv', estimates_service.get_estimates, missing_csv_warning),
        ('consensus.csv', estimize_consensus_service.get_final_consensuses, missing_csv_warning),
        ('signal_time_series.csv', estimize_signal_service.get_signals, missing_csv_warning),
        ('market_factors.csv', factor_serivce.get_market_factors, remote_csv_warning),
        ('market_caps.csv', market_cap_service.get_market_caps, remote_csv_warning),
        ('releases.csv', releases_service.get_releases, missing_csv_warning),
    ]

    def item_show_func(item):
        if item is not None:
            return 'Caching {}'.format(item[0])

    with click.progressbar(actions, label='Caching Estimize Data', item_show_func=item_show_func) as items:
        for item in items:
            try:
                item[1]()
            except:
                print('\nERROR: {}'.format(item[2].format(item[0])))


if __name__ == '__main__':
    main()
