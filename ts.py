import pandas as pd


def parse_ts(col: pd.Series):
    return pd.to_datetime(col, unit='ms')
