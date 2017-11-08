from datetime import timedelta

import numpy as np
import pandas as pd
import ziplinez.api as zapi
from ziplinez import run_algorithm
from ziplinez.api import *
from ziplinez.pipeline import Pipeline
from ziplinez.pipeline.factors import AverageDollarVolume

# from ziplinez.pipeline.factors.morningstar import MarketCap

# For testing, use `signal.sample.csv` as the file in the URL
SIGNAL_URL = "https://s3.amazonaws.com/com.estimize.production.data/estimize-zipline/df2015.csv"
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIMEZONE = 'America/New_York'
MIN_STOCKS = 10


def initialize(context):
    attach_pipeline(make_pipeline(), name="pipeline")

    # Fetch and process the signal data csv
    fetch_csv(
        SIGNAL_URL,
        pre_func=pre_func,
        post_func=post_func,
        date_column='date',
        date_format=DATE_FORMAT
    )

    # Schedule morning trading
    schedule_function(
        func=trade,
        date_rule=zapi.date_rules.every_day(),
        time_rule=zapi.time_rules.market_open()
    )

    # Schedule afternoon trading at 2:30PM
    schedule_function(
        func=trade,
        date_rule=zapi.date_rules.every_day(),
        time_rule=zapi.time_rules.market_close(minutes=90)
    )


def pre_func(df):
    print('df: {}'.format(df.head(5)))

    # Filter based on type
    df = df[df['type'] == 'post']

    print(df.columns[0])

    # Drop unused columns and rename ticker -> symbol
    df.drop([df.columns[0], 'cusip', 'reports_at', 'type'], axis=1, inplace=True)
    df.rename(columns={'ticker': 'symbol', 'as_of': 'date'}, inplace=True)

    return df


def post_func(df):
    # Reset the index so we can manipulate
    df.reset_index(inplace=True)

    # Group rows to find last day of release signal
    zdf = df.groupby(['sid', 'fiscal_date']).agg('max').add_prefix('max_')
    zdf.reset_index(inplace=True)

    # Add day to last day of release signal and set signal to 0
    zdf['max_dt'] += timedelta(days=1)
    zdf['max_dt'] = zdf['max_dt'].dt.normalize()
    zdf['signal'] = np.nan

    # Drop and rename columns
    zdf.rename(columns={'max_dt': 'dt'}, inplace=True)
    zdf.drop(['max_signal', 'fiscal_date'], axis=1, inplace=True)

    # Concatenate to original DataFrame
    df = pd.concat([df, zdf], copy=False)

    # Set the index back
    df.set_index('dt', inplace=True)

    return df


def before_trading_start(context, data):
    # Add pipeline to context
    context.pipeline = pipeline_output('pipeline')


def setup_trades(context, data):
    # Fetch entire universe of pipeline and fetched assets
    results = data.current(context.pipeline.index, ['signal'])

    # Drop assets where there is no signal data
    results = results.dropna()

    record(num_assets=len(results))
    record_signals(results, symbols('AMZN', 'AAPL'))

    # Add longs/shorts to context:

    # Long the top decile
    top_decile = results.signal.quantile(0.90)
    context.longs = results[results.signal >= top_decile].index.values
    record(num_longs=len(context.longs))

    # Short the bottom decile
    bottom_decile = results.signal.quantile(0.10)
    context.shorts = results[results.signal <= bottom_decile].index.values
    record(num_shorts=len(context.shorts))

    # Close
    context.close = list(set(context.portfolio.positions.keys()) - (set(context.longs) | set(context.shorts)))


def trade(context, data):
    setup_trades(context, data)

    if len(context.shorts) >= MIN_STOCKS and len(context.longs) >= MIN_STOCKS:
        optimize_portfolio(context)
    else:
        # Reset to cash positions
        close_positions(context)


def optimize_portfolio(context):
    allocation = 1.0 / (len(context.longs) + len(context.shorts))

    for asset in context.close:
        order_target_percent(asset, 0.0)

    for asset in context.longs:
        order_target_percent(asset, allocation)

    for asset in context.shorts:
        order_target_percent(asset, -allocation)


def close_positions(context):
    for asset in context.portfolio.positions.keys():
        order_target_percent(asset, 0.0)


def make_pipeline():

    # Market cap filter >= $100mm
    # market_cap = MarketCap() >= 100e6

    # ADV filter > $1mm
    adv = AverageDollarVolume(window_length=90) >= 1e6

    # TODO: Vinesh was using a $4/share price filter as well, but that seems
    # odd given the ADV and market cap restriction. We may still want to use
    # one, but probably just $1 (unadjusted) to get rid of penny stocks.
    # securities_to_trade = (market_cap & adv)

    return Pipeline(screen=adv)


def record_signals(results, stocks):
    for stock in stocks:
        asset = results[results.index == stock]

        if not asset.empty:
            record(stock, asset.iloc[0]['signal'])
        else:
            record(stock, 0)
