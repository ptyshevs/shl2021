from tqdm.auto import tqdm
from itertools import islice
import pandas as pd
import numpy as np


def chunker(it, size: int):
    iterator = iter(it)
    while chunk := list(islice(iterator, size)):
        yield chunk


def get_wifi_generator(path: str):
    with open(path) as fp:
        for i, row in tqdm(enumerate(fp)):
            ts, ignored1, ignored2, n_spots, *visible_spots = row.strip().split(';', maxsplit=4)
            n_spots = int(n_spots)
            if n_spots == 0:
                yield (ts, n_spots, np.nan, np.nan, np.nan, np.nan, np.nan)
                continue
            # 5: bssid, ssid, rssi, freq, capabilities
            visible_spots = ';'.join(visible_spots)
            chunks = chunker(visible_spots.split(';'), 5)
            for (bssid, ssid, rssi, freq, capabilities) in chunks:
                yield (ts, n_spots, bssid, ssid, rssi, freq, capabilities)


def get_wifi_df(path: str, nrows: int = None) -> pd.DataFrame:
    """Extract wifi data from .txt file"""
    df = pd.DataFrame.from_records(get_wifi_generator(path),
                        columns=['ts', 'n_spots', 'bssid', 'ssid', 'rssi', 'freq', 'capabilities'],
                        nrows=nrows)

    df.ts = pd.to_datetime(df.ts, unit='ms')
    df.n_spots = df.n_spots.astype(np.uint16)
    df.rssi = df.rssi.astype(float)
    df.freq = df.freq.astype(float)
    return df
