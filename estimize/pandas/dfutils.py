import pandas as pd
import estimize.config as cfg

ASSET_COLUMN = 'asset'


def filter(df: pd.DataFrame, start_date=None, end_date=None, assets=None) -> pd.DataFrame:
    if start_date is not None:
        start_date = pd.Timestamp(start_date, tz=cfg.DEFAULT_TIMEZONE)
        df = df.iloc[df.index.get_level_values('as_of_date') >= start_date]

    if end_date is not None:
        end_date = pd.Timestamp(end_date, tz=cfg.DEFAULT_TIMEZONE)
        df = df.iloc[df.index.get_level_values('as_of_date') <= end_date]

    if assets is not None:
        df = df.iloc[df.index.get_level_values('asset').isin(assets)]

    return df


def unique_assets(df):
    return column_values(df, ASSET_COLUMN).unique().tolist()


def column_values(df, column):
    try:
        df.index.names.index(column)
        values = df.index.get_level_values(column)
    except ValueError:
        if any(col == column for col in df.columns.names):
            values = df[column]
        else:
            raise ValueError("{} does not contain a '{}' column!".format(type(df), column))

    return values
