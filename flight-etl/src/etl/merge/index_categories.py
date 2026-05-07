from pathlib import Path

from pandas import CategoricalDtype, DataFrame

from utils.loaders import load_parquet


def index_categorical(df: DataFrame, airports: DataFrame) -> tuple[DataFrame, dict]:
    df_copy = df.copy()
    """
    Categorical Columns:
    AIRLINE
    TAIL_NUMBER
    ORIGIN_AIRPORT
    DESTINATION_AIRPORT
    """

    # Create Categorical data types for the categorical columns.
    airline_categories = CategoricalDtype(df["AIRLINE"].unique())
    tail_number_categories = CategoricalDtype(df["TAIL_NUMBER"].unique())
    airport_categories = CategoricalDtype(airports["IATA"].unique())

    # Map between categorical columns and the respective data types.
    categories = {
        "AIRLINE": airline_categories,
        "TAIL_NUMBER": tail_number_categories,
        "ORIGIN_AIRPORT": airport_categories,
        "DESTINATION_AIRPORT": airport_categories,
    }

    # Change the data types to categorical data types.
    for category in categories:
        df_copy[category] = df[category].astype(categories[category]).cat.codes

    return (df_copy, categories)


def categorize_categories(read_filepath: str | Path, airport_locations_filepath: str | Path):
    flight_weather_df = load_parquet(read_filepath)
    airports_df = load_parquet(airport_locations_filepath)

    flight_weather_categorized_df, categries = index_categorical(flight_weather_df, airports_df)
