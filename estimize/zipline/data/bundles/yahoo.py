import os

import numpy as np
import pandas as pd
import requests
from pandas_datareader.data import DataReader
from zipline.utils.cli import maybe_show_progress


def yahoo_bundle(symbols):
    """Create a data bundle ingest function from a set of symbols loaded from
    Yahoo.

    Parameters
    ----------
    symbols : iterable[str]
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

       symbols = (
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
    symbols = tuple(symbols)

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        if start_session is None:
            start_session = calendar[0]

        metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        def _pricing_iter():
            sid = 0
            with maybe_show_progress(
                    symbols,
                    show_progress,
                    label='Downloading Yahoo pricing data: ') as it, \
                    requests.Session() as session:
                for ticker in it:
                    path = _cache_path(ticker, 'ohlcv')
                    try:
                        df = cache[path]
                    except KeyError:
                        df = cache[path] = DataReader(
                            ticker,
                            'yahoo',
                            start_session,
                            end_session,
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
                            'Adj Close': 'price',
                        },
                        inplace=True,
                    )
                    yield sid, df
                    sid += 1

        daily_bar_writer.write(_pricing_iter(), show_progress=True)

        metadata['exchange'] = 'YAHOO'
        asset_db_writer.write(equities=metadata)

        adjustment_writer.write()  # Needed to create the adjustments tables

    return ingest


def _cache_path(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))
