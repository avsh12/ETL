import logging
from pathlib import Path

import pandas as pd

from etl.utils.loaders import (
    get_config_resource_path,
    load_csv,
    load_yaml,
    write_parquet,
)
from etl.utils.logger import log_progress

"""
Cleaning steps:
- Schema validation: Enforce numeric and string columns to have values with the respective datatype.
- Drop cancelled flights
- Deduplication: Remove duplicate entries
- Removing nulls
"""


def schema_validaton(df: pd.DataFrame, schema: dict, not_nullable_features, discarded_list=None):
    log_progress("Schema Validation in progress...\n")
    # The dataframe consists of string and numeric columns.
    # Define the string and numeric columns
    string_columns = []
    numeric_columns = []
    len_df = len(df)

    log_progress("Creating list of numeric and string features.")
    for key in schema:
        if pd.api.types.is_numeric_dtype(pd.api.types.pandas_dtype(schema[key])):
            numeric_columns.append(key)
        else:
            string_columns.append(key)
    log_progress(f"Numeric columns: {numeric_columns}\n")
    log_progress(f"String columns: {string_columns}\n")
    log_progress(f"Not-nullable features: {not_nullable_features}\n")

    # Enforce numeric columns to be numeric
    # Enforce string columns to be string
    # select rows with not null values
    log_progress("Enforcing datatypes on the entries using the silver schema.")
    log_progress("Fraction of incosistent entries in the columns.")
    log_progress(f"{'Column':<20}{'%':>7}")
    is_bad = pd.Series(False, index=df.index)
    for col in df.columns:
        is_numeric = pd.to_numeric(df[col], errors="coerce").notna()
        discard = is_numeric.copy(deep=True)

        if col in numeric_columns:
            discard = ~discard

        if col not in not_nullable_features:
            # if the column is nullable, keep the rows that were null previously.
            discard &= df[col].notna()

        log_progress(f"{col:<20}{(100*(discard.sum().item())/len_df):>7.4f}")

        is_bad |= discard

    # Store the discarded rows to the discarded bin if enabled
    if discarded_list is not None:
        discarded_list.append(df[is_bad].copy())

    # filter the rows
    df = df[~is_bad]
    log_progress(f"Percentage of rows dropped = {100*(len_df - len(df))/len_df:.2f} %")

    df = df.reset_index(drop=True).astype(schema)
    log_progress("Schema valication done!\n")
    return df


# Drop cancelled flights
def drop_cancelled(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    log_progress("Dropping Cancelled Flights")
    df = df.query("CANCELLED == 0").drop(columns=["CANCELLED"])
    # reset the index column for the new data size
    df = df.reset_index(drop=True)

    log_progress(f"Percentage of cancelled flights = {100*(len_df-len(df))/len_df:.4f} %")
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    log_progress("De-duplicating")
    df = df.drop_duplicates(ignore_index=True)
    log_progress(f"Percentage of duplicates dropped = {100*(len_df-len(df))/len_df:.4f} %")

    df = df.reset_index(drop=True)
    return df


def drop_null(df: pd.DataFrame, not_nullable_features: list | None = None) -> pd.DataFrame:
    log_progress("Dropping null values")
    log_progress("Fraction of null entries in non-nullable column.")
    log_progress(f"{'Column':<20}{'%':>5}")
    null_counts = {}
    len_df = len(df)

    filter = pd.Series(True, index=range(len_df))

    if not_nullable_features is None:
        not_nullable_features = list(df.columns)
    # Get the null state of each entry
    mask = df[not_nullable_features].notna()

    for col in not_nullable_features:
        # Count the number of null values for each column
        null_count = (~mask[col]).sum()
        log_progress(f"{col:<20}{(100*null_count/len_df):>5.4f}")
        null_counts.update({col: null_count})
        # Filter entries with not null values
        filter &= mask[col]

    columns_with_null_values = dict((col, null_counts[col]) for col in not_nullable_features if null_counts[col] != 0)

    log_progress(
        f"""Columns containing null values: \
        {columns_with_null_values}\n"""
        if len(columns_with_null_values) != 0
        else "No null values\n"
    )

    df = df[filter].reset_index(drop=True)
    return df[filter]


def clean(read_filepath: str | Path, write_filepath: str | Path):
    schema_path = get_config_resource_path("schema")
    schema = load_yaml(schema_path)

    log_progress("Reading the raw flight data.\n")
    df = load_csv(
        read_filepath,
        usecols=schema["flight_bronze_schema"].keys(),
        dtype=schema["flight_bronze_schema"],
        engine="pyarrow",
    )

    log_progress("Cleaning in progress...\n")
    df = schema_validaton(
        df,
        schema=schema["flight_silver_schema"],
        not_nullable_features=schema["flight_not_nullable_features"],
    )

    df = df.pipe(drop_null, schema["flight_not_nullable_features"]).pipe(drop_duplicates)
    log_progress("Cleaning done!\n")

    write_parquet(df, write_filepath)

    return write_filepath
