import pandas as pd

from zipline.data.loader import load_from_yahoo
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, Returns
from zipline.pipeline.filters import StaticAssets

from estimize.data.config import Config


DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIMEZONE = 'US/Eastern'

CONFIG = Config()


def get_benchmark_returns(start_date, end_date):
    start_date = get_valid_start_date(start_date)
    end_date = get_valid_end_date(end_date)

    df = load_from_yahoo(indexes={'price': 'SPY'}, start=start_date, end=end_date)
    df['return'] = df.pct_change()
    df.dropna(inplace=True)
    df.drop(['price'], axis=1, inplace=True)
    df.reset_index(inplace=True)
    df['Date'] = df['Date'].dt.date
    df.rename(columns={'Date': 'as_of_date'}, inplace=True)
    df.set_index('as_of_date', inplace=True)

    return df


def get_returns(start_date, end_date, assets=None):
    start_date = get_valid_start_date(start_date)
    end_date = get_valid_end_date(end_date)

    daily_return = Returns(window_length=2)

    pipeline = Pipeline(
        columns={
            'return': daily_return
        }
    )

    if assets is not None:
        pipeline.set_screen(StaticAssets(assets))

    df = pipeline_engine().run_pipeline(pipeline, start_date, end_date)

    df.index.tz = TIMEZONE
    df.reset_index(inplace=True)
    df.rename(columns={'level_0': 'as_of_date', 'level_1': 'symbol'}, inplace=True)
    df['as_of_date'] = df['as_of_date'].dt.date
    df.set_index(['as_of_date', 'symbol'], inplace=True)

    return df


def get_universe(start_date, end_date, assets=None):
    start_date = get_valid_start_date(start_date)
    end_date = get_valid_end_date(end_date)

    adv = AverageDollarVolume(window_length=20)
    latest_close = USEquityPricing.close.latest

    # min_market_cap = market_cap >= 100e6 # Need market cap data
    min_adv = (adv >= 1e6)
    min_latest_close = (latest_close >= 4)
    screen = (min_adv & min_latest_close)

    if assets is not None:
        screen = (StaticAssets(assets) & screen)

    pipeline = Pipeline(
        columns={
            'latest_close': latest_close
        },
        screen=screen
    )

    df = pipeline_engine().run_pipeline(pipeline, start_date, end_date)

    df.index.tz = TIMEZONE
    df.reset_index(inplace=True)
    df.rename(columns={'level_0': 'as_of_date', 'level_1': 'symbol'}, inplace=True)
    df['as_of_date'] = df['as_of_date'].dt.date
    df.set_index(['as_of_date', 'symbol'], inplace=True)

    return df


def get_unique_assets(df):
    try:
        index = df.index.names.index('symbol')
        values = df.index[index].values
    except ValueError:
        if any(col == 'symbol' for col in df.column.names):
            values = df['symbol'].values
        else:
            raise ValueError("DataFrame does not contain a 'symbol' column!")

    return values.unique().tolist()


def get_asset(ticker):
    try:
        return get_assets([ticker])[0]
    except IndexError:
        raise ValueError("Ticker '%s' was not found!" % ticker)


def get_assets(tickers):
    return CONFIG.asset_finder.lookup_symbols(tickers, None)


def get_valid_start_date(start_date):
    start_date = pd.Timestamp(start_date, tz='UTC')

    if trading_calendar().is_session(start_date):
        return start_date
    else:
        return trading_calendar().next_open(start_date).normalize()


def get_valid_end_date(end_date):
    end_date = pd.Timestamp(end_date, tz='UTC')

    if trading_calendar().is_session(end_date):
        return end_date
    else:
        return trading_calendar().previous_open(end_date).normalize()


def pipeline_engine():
    return CONFIG.pipeline_engine


def asset_finder():
    return CONFIG.asset_finder


def trading_calendar():
    return CONFIG.trading_calendar
