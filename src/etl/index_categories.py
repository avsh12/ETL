import pandas as pd


def index_categorical(
    df: pd.DataFrame, airports: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Categorical Columns:
    AIRLINE
    FLIGHT_NUMBER
    TAIL_NUMBER
    ORIGIN_AIRPORT
    DESTINATION_AIRPORT
    """

    # Create Categorical data types for the categorical columns.
    airline_categories = pd.CategoricalDtype(df["AIRLINE"].unique())
    flight_no_categories = pd.CategoricalDtype(df["FLIGHT_NUMBER"].unique())
    tail_no_categories = pd.CategoricalDtype(df["TAIL_NUMBER"].unique())
    airport_categories = pd.CategoricalDtype(airports["IATA"].unique())

    # Map between categorical columns and the respective data types.
    categories = {
        "AIRLINE": airline_categories,
        "FLIGHT_NUMBER": flight_no_categories,
        "TAIL_NUMBER": tail_no_categories,
        "ORIGIN_AIRPORT": airport_categories,
        "DESTINATION_AIRPORT": airport_categories,
    }

    # Change the data types to categorical data types.
    for category in categories:
        df[category] = df[category].astype(categories[category]).cat.codes

    return df, categories
