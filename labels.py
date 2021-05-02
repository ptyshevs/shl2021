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
