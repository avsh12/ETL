import pandas as pd
from utils.logger import log_progress


def schema_validaton(df: pd.DataFrame, schema, discarded_list=None):
    # The dataframe consists of string and numeric columns.
    # Define the string and numeric columns
    log_progress("Schema Validating")
    string_columns = []
    numeric_columns = []
    len_df = len(df)

    for key in schema:
        if pd.api.types.is_numeric_dtype(pd.api.types.pandas_dtype(schema[key])):
            numeric_columns.append(key)
        else:
            string_columns.append(key)

    is_bad = pd.Series(False, index=df.index)
    for col in df.columns:
        is_numeric = pd.to_numeric(df[col], errors="coerce").notna()

        if col in string_columns:
            is_bad |= is_numeric
        elif col in numeric_columns:
            is_bad |= ~is_numeric

    if discarded_list is not None:
        discarded_list.append(df[is_bad].copy())

    df = df[~is_bad]
    log_progress(
        f"Percentage of rows dropped = {100*(len_df - len(df))/len_df:.4f} %\n"
    )

    return df
