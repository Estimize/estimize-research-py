import logging.config

from injector import Injector

from estimize.di.default_module import DefaultModule
from estimize.pandas import dfutils
from estimize.services import AssetService
from estimize.services.csv_data_service import CsvDataService
from estimize.services.estimize_consensus_service import EstimizeConsensusService
from estimize.services.residual_returns_service import ResidualReturnsService
from estimize.use_cases import PostEarningsEventStudy, PreEarningsEventStudy, PreEarningsPricingEventStudy

# Configure logging

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'standard'
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
})

injector = Injector([DefaultModule])


def main():
    test_residual_returns()


def test_pre_earnings_price_event_study():
    start_date = '2012-01-01'
    end_date = '2017-10-01'
    es = PreEarningsPricingEventStudy(start_date, end_date, days_before=10, days_after=5)
    df = es.results

    print(type(df))
    print(df.index.names)
    print(df.dtypes)
    print(df)


def test_pre_earnings_event_study():
    start_date = '2012-01-01'
    end_date = '2017-10-01'
    es = PreEarningsEventStudy(start_date, end_date, days_before=15, days_after=0)
    df = es.results

    print(type(df))
    print(df.index.names)
    print(df.dtypes)
    print(df)


def test_post_earnings_event_study():
    start_date = '2017-01-01'
    end_date = '2017-10-01'
    es = PostEarningsEventStudy(start_date, end_date)
    df = es.results

    print(type(df))
    print(df.index.names)
    print(df.dtypes)
    print(df)


def test_earnings_yields():
    start_date = '2012-01-01'
    end_date = '2017-01-01'
    service = EstimizeConsensusService()
    df = service.get_earnings_yields(start_date, end_date)

    print(df.dtypes)
    print(df)


def test_estimize_consensus_service():
    start_date = '2017-01-01'
    end_date = '2017-10-01'
    service = EstimizeConsensusService()
    df = service.get_consensuses(start_date, end_date)

    print(df.dtypes)
    print(df)


def test_csv_data_service():
    service = CsvDataService()
    url = 'https://s3.amazonaws.com/com.estimize.production.data/quantopian/df2017q3.csv'
    df = service.load_from_url(
        url=url,
        date_column='as_of',
        date_format='%Y-%m-%dT%H:%M:%S',
        symbol_column='ticker'
    )

    print(df)


def test_residual_returns():
    start_date = '2012-01-01'
    end_date = '2017-10-01'

    consensus_service = injector.get(EstimizeConsensusService)
    asset_service = injector.get(AssetService)

    cdf = consensus_service.get_final_earnings_yields()
    assets = dfutils.unique_assets(cdf)
    assets = asset_service.get_assets(['AAPL', 'MSFT', 'AMZN', 'EPAY', 'WFC', 'ROC', 'ARG', 'SPF'])
    assets = None

    service = injector.get(ResidualReturnsService)
    df = service.get_market_neutral_residual_returns(start_date, end_date, assets, on='open')

    print(df)


def test_returns():
    start_date = '2017-11-01'
    end_date = '2017-12-01'
    assets = asset_service.get_assets(['AAPL', 'MSFT'])
    df = asset_service.get_returns(start_date, end_date, assets)
    print(df)


def test_moving_average():
    start_date = '2016-01-01'
    end_date = '2016-01-29'
    assets = asset_service.get_assets(['AAPL', 'MSFT', 'AMZN'])
    df = asset_service.get_moving_average(start_date, end_date, assets=assets)
    print(df)


def test_universe():
    start_date = '2016-01-01'
    end_date = '2016-01-29'
    assets = asset_service.get_assets(['AAPL', 'MSFT', 'AMZN'])
    df = asset_service.get_universe(start_date, end_date, assets)
    df.reset_index(inplace=True)

    print(df.dtypes)
    print(df)


if __name__ == "__main__":
    main()
