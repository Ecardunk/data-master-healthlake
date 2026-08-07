import numpy as np
import pandas as pd


def inject_nulls(
    df: pd.DataFrame,
    column_percentages=None,
    excluded_columns=None
):

    df_copy = df.copy()

    if column_percentages is None:
        return df_copy

    if isinstance(column_percentages, (int, float)):
        column_percentages = {
            column: column_percentages
            for column in df_copy.columns
        }

    excluded_columns = set(excluded_columns or [])

    for col in df_copy.columns:

        if col in excluded_columns:
            continue

        percentage = column_percentages.get(col, 0)
        if percentage <= 0:
            continue

        mask = np.random.rand(len(df_copy)) < percentage
        df_copy.loc[mask, col] = None

    return df_copy


def inject_duplicates(df: pd.DataFrame, percentage: float):

    if percentage <= 0 or df.empty:
        return df.copy()

    n_duplicates = int(len(df) * percentage)

    if n_duplicates == 0:
        return df.copy()

    duplicates = df.sample(
        n=n_duplicates,
        replace=True
    )

    return pd.concat(
        [df, duplicates],
        ignore_index=True
    )
