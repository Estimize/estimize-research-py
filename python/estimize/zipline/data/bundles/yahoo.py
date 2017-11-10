import os

import numpy as np
import pandas as pd
import requests
from pandas_datareader.data import DataReader
from zipline.utils.cli import maybe_show_progress


def yahoo_bundle(tickers):
    """Create a data bundle ingest function from a set of symbols loaded from
    Yahoo.

    Parameters
    ----------
    tickers : iterable[str]
        The ticker symbols to load data for.

    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.

    Examples
    --------
    This code should be added to ~/.zipline/extension.py

    .. code-block:: python

       from zipline.data.bundles import yahoo_equities, register

       tickers = (
           'AAPL',
           'IBM',
           'MSFT',
       )
       register('my_bundle', yahoo_bundle(tickers))

    Notes
    -----
    The sids for each symbol will be the index into the symbols sequence.
    """
    # strict this in memory so that we can reiterate over it
    tickers = tuple(tickers)

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=None,
               end=None):

        if start is None:
            start = calendar[0]

        metadata = pd.DataFrame(np.empty(len(tickers), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        def _pricing_iter():
            sid = 0
            with maybe_show_progress(
                    tickers,
                    show_progress,
                    label='Downloading Google pricing data: ') as it, \
                    requests.Session() as session:
                for ticker in it:
                    path = _cache_path(ticker, 'ohlcv')
                    try:
                        df = cache[path]
                    except KeyError:
                        df = cache[path] = DataReader(
                            ticker,
                            'yahoo',
                            start,
                            end,
                            session=session,
                        ).sort_index()

                    # the start date is the date of the first trade and
                    # the end date is the date of the last trade
                    start_date = df.index[0]
                    end_date = df.index[-1]
                    # The auto_close date is the day after the last trade.
                    ac_date = end_date + pd.Timedelta(days=1)
                    metadata.iloc[sid] = start_date, end_date, ac_date, ticker

                    df.rename(
                        columns={
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume',
                        },
                        inplace=True,
                    )
                    yield sid, df
                    sid += 1

        daily_bar_writer.write(_pricing_iter(), show_progress=True)

        symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)
        asset_db_writer.write(equities=metadata)

        adjustments = []
        with maybe_show_progress(
                tickers,
                show_progress,
                label='Downloading Google adjustment data: ') as it, \
                requests.Session() as session:
            for symbol in it:
                path = _cache_path(symbol, 'adjustment')
                try:
                    df = cache[path]
                except KeyError:
                    df = cache[path] = DataReader(
                        symbol,
                        'yahoo-actions',
                        start,
                        end,
                        session=session,
                    ).sort_index()

                df['sid'] = symbol_map[symbol]
                adjustments.append(df)

        adj_df = pd.concat(adjustments)
        adj_df.index.name = 'date'
        adj_df.reset_index(inplace=True)

        splits = adj_df[adj_df.action == 'SPLIT']
        splits = splits.rename(
            columns={'value': 'ratio', 'date': 'effective_date'},
        )
        splits.drop('action', axis=1, inplace=True)

        dividends = adj_df[adj_df.action == 'DIVIDEND']
        dividends = dividends.rename(
            columns={'value': 'amount', 'date': 'ex_date'},
        )
        dividends.drop('action', axis=1, inplace=True)
        # we do not have this data in the yahoo dataset
        dividends['record_date'] = pd.NaT
        dividends['declared_date'] = pd.NaT
        dividends['pay_date'] = pd.NaT

        adjustment_writer.write(splits=splits, dividends=dividends)

    return ingest


def _cache_path(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))
