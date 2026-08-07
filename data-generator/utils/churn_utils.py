import pandas as pd


def remove_random_rows(
    df: pd.DataFrame,
    percentage: float = 0.01
):

    if percentage <= 0 or df.empty:
        return df.copy()

    rows_to_remove = int(
        len(df) * percentage
    )

    if rows_to_remove == 0:
        return df.copy()

    rows = df.sample(
        n=rows_to_remove
    ).index

    return df.drop(rows).reset_index(drop=True)
