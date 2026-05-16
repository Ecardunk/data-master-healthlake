import numpy as np
import pandas as pd


def inject_nulls(df: pd.DataFrame, percentage: float):

    df_copy = df.copy()

    for col in df_copy.columns:

        if col.endswith("_id"):
            continue

        mask = np.random.rand(len(df_copy)) < percentage
        df_copy.loc[mask, col] = None

    return df_copy


def inject_duplicates(df: pd.DataFrame, percentage: float):

    n_duplicates = int(len(df) * percentage)

    duplicates = df.sample(
        n=n_duplicates,
        replace=True
    )

    return pd.concat(
        [df, duplicates],
        ignore_index=True
    )