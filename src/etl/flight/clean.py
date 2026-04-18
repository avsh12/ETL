import pandas as pd
from utils.logger import log_progress


def drop_cancelled(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    """
    We will drop rows for cancelled flights
    """
    log_progress("Dropping Cancelled")
    df = df.query("CANCELLED == 0").drop(columns=["CANCELLED"])
    # reset the index column for the new data size
    df = df.reset_index(drop=True)

    log_progress(
        f"Percentage of cancelled flights = {100*(len_df-len(df))/len_df:.4f} %"
    )
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    log_progress("De-duplicating")
    df = df.drop_duplicates(ignore_index=True)
    log_progress(
        f"Percentage of duplicates dropped = {100*(len_df-len(df))/len_df:.4f} %"
    )

    return df


def select_columns(df: pd.DataFrame, columns):
    log_progress("Selecting columns")
    df = df[columns]
    log_progress("Columns selection done!")
    return df


def drop_null(df: pd.DataFrame) -> pd.DataFrame:
    log_progress("Dropping null values")

    null_containing_columns = []
    null_counts = []

    # loop through the columns and store the indices of null rows and the number of null values for the each column.
    for col in df.columns:
        null_indices = df[col].isna()
        null_count = null_indices.sum()

        if null_count != 0:
            # Get the indices of null fields if there is any null values in the column.
            null_indices = null_indices[null_indices].index
            # Drop the null values.
            df[col].drop(null_indices, inplace=True)
            null_containing_columns.append(col)
            null_counts.append(null_count)

    log_progress(
        f"""Columns containg null values: \
        {list(zip(zip(df.columns[null_containing_columns], null_containing_columns),
                    null_counts))}\n"""
        if null_count != 0
        else "No null values"
    )

    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    log_progress("Cleaning stage")
    df = df.pipe(drop_cancelled).pipe(drop_duplicates)
    log_progress("Cleaning done!\n")
    return df
