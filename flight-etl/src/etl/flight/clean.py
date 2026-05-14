import logging

import pandas as pd
from upath import UPath as Path

from etl.core.constants import SCHEMA_FILEPATH
from etl.core.interfaces import BaseTransform
from etl.utils.file_handler import (
    load_csv,
    load_yaml,
    write_parquet,
)

logger = logging.getLogger(__name__)

"""
Cleaning steps:
- Schema validation: Enforce numeric and string columns to have values with the respective datatype.
- Drop cancelled flights
- Deduplication: Remove duplicate entries
- Removing nulls
"""


def schema_validaton(df: pd.DataFrame, schema: dict, not_nullable_features, discarded_list=None):
    logger.info("Schema Validation in progress...\n")
    # The dataframe consists of string and numeric columns.
    # Define the string and numeric columns
    string_columns = []
    numeric_columns = []
    len_df = len(df)

    logger.info("Creating list of numeric and string features to remove entries with inconsistent types.")
    for key in schema:
        if pd.api.types.is_numeric_dtype(pd.api.types.pandas_dtype(schema[key])):
            numeric_columns.append(key)
        else:
            string_columns.append(key)
    logger.debug(f"Numeric columns: {numeric_columns}\n")
    logger.debug(f"String columns: {string_columns}\n")
    logger.debug(f"Not-nullable features: {not_nullable_features}\n")

    # Enforce numeric columns to be numeric
    # Enforce string columns to be string
    # select rows with not null values
    logger.info("Enforcing datatypes on the entries using the silver schema.")
    logger.debug("Fraction of incosistent entries in the columns.")
    logger.debug(f"{'Column':<20}{'%':>7}")
    is_bad = pd.Series(False, index=df.index)
    for col in df.columns:
        is_numeric = pd.to_numeric(df[col], errors="coerce").notna()
        discard = is_numeric.copy(deep=True)

        if col in numeric_columns:
            discard = ~discard

        if col not in not_nullable_features:
            # if the column is nullable, keep the rows that were null previously.
            discard &= df[col].notna()

        logger.debug(f"{col:<20}{(100*(discard.sum().item())/len_df):>7.4f}")

        is_bad |= discard

    # Store the discarded rows to the discarded bin if enabled
    if discarded_list is not None:
        discarded_list.append(df[is_bad].copy())

    # filter the rows
    df = df[~is_bad]
    logger.info(f"Percentage of rows with inconsistent data types dropped: {100*(len_df - len(df))/len_df:.2f} %")

    df = df.reset_index(drop=True).astype(schema)
    logger.info("Schema valication done!\n")
    return df


# Drop cancelled flights
def drop_cancelled(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    logger.info("Dropping Cancelled Flights")
    df = df.query("CANCELLED == 0").drop(columns=["CANCELLED"])
    # reset the index column for the new data size
    df = df.reset_index(drop=True)

    logger.info(f"Percentage of cancelled flights = {100*(len_df-len(df))/len_df:.4f} %")
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    len_df = len(df)
    logger.info("De-duplicating")
    df = df.drop_duplicates(ignore_index=True)
    logger.info(f"Percentage of duplicates dropped = {100*(len_df-len(df))/len_df:.4f} %")

    df = df.reset_index(drop=True)
    return df


def drop_null(df: pd.DataFrame, not_nullable_features: list | None = None) -> pd.DataFrame:
    logger.info("Dropping null values")
    logger.debug("Fraction of null entries in non-nullable column.")
    logger.debug(f"{'Column':<20}{'%':>5}")
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
        logger.debug(f"{col:<20}{(100*null_count/len_df):>5.4f}")
        null_counts.update({col: null_count})
        # Filter entries with not null values
        filter &= mask[col]

    columns_with_null_values = dict((col, null_counts[col]) for col in not_nullable_features if null_counts[col] != 0)

    total_null_values_dropped = 0
    for key in columns_with_null_values:
        total_null_values_dropped += columns_with_null_values[key]
    logger.info(
        f"Fraction of rows with null values in not-nullable features dropped: {100*total_null_values_dropped/len_df:.4f} %"
    )
    logger.debug(
        f"""Columns containing null values: \
        {columns_with_null_values}\n"""
        if len(columns_with_null_values) != 0
        else "No null values\n"
    )

    df = df[filter].reset_index(drop=True)
    return df[filter]


class FlightClean(BaseTransform):
    def __init__(self, bronze_flight_filepath: str, schema_filepath: str) -> None:
        super().__init__()
        self.bronze_flight_filepath = bronze_flight_filepath
        self.schema_filepath = schema_filepath

    def transform(self):
        schema = load_yaml(self.schema_filepath)
        df = load_csv(
            self.bronze_flight_filepath,
            usecols=schema["flight_bronze_schema"].keys(),
            dtype=schema["flight_bronze_schema"],
            engine="pyarrow",
        )

        logger.info("Flight cleaning in progress...")
        df = schema_validaton(
            df,
            schema=schema["flight_silver_schema"],
            not_nullable_features=schema["flight_not_nullable_features"],
        )

        df = df.pipe(drop_null, schema["flight_not_nullable_features"]).pipe(drop_duplicates)
        return df

    def load(self, silver_flight_filepath: str, data: pd.DataFrame):
        write_parquet(silver_flight_filepath, data)

    def execute(self, silver_flight_filepath: str):
        df = self.transform()
        self.load(silver_flight_filepath, df)
        return silver_flight_filepath
