import pandas as pd
from ts import parse_ts
import yaml
from typing import Dict
import os


def get_labels_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, header=None, names=['ts', 'label'], delimiter='\t')
    df.ts = parse_ts(df.ts)
    return df


def get_cls2label(path: str = os.path.join('data', 'cls2label.yaml')) -> Dict[int, str]:
    """Get mapping from class to label"""
    with open(path) as fp:
        cls2label = yaml.load(fp, Loader=yaml.loader.Loader)
    return cls2label

def merge_data_labels(data_df, label_df):
    mdf = pd.merge_asof(data_df, label_df, direction='forward', on='ts')
    merged_df = mdf[(mdf.ts >= label_df.ts.min()) & (mdf.ts <= label_df.ts.max())].reset_index(drop=True)
    return merged_df
