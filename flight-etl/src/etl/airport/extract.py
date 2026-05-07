from pathlib import Path

import pandas as pd
from airports import airport_data
from pandas import DataFrame, Index

from etl.utils.loaders import load_parquet, write_json, write_parquet
from etl.utils.logger import log_progress


def get_date_range(df: pd.DataFrame) -> list:
    d = df[["SCH_DEP_DATE", "SCH_ARI_DATE"]].values

    d_min = d.min()
    d_max = d.max()

    return [d_min, d_max]


# Returns a generator to select subset of key-value pairs from a list of dictionary.
def get_airport_data(airports: list[dict]):
    keys = ["country_code", "airport", "iata", "time", "utc", "latitude", "longitude"]
    for airport in airports:
        yield dict((k, airport[k]) for k in keys)


# Generate a Dataframe for airports in the US
def generate_airport_locations(write_filepath: str | Path):
    airports_us = airport_data.get_airport_by_country_code("US")
    airports = pd.DataFrame(get_airport_data(airports_us))

    airports.columns = airports.columns.str.upper()

    airports = airports.dropna()
    select_rows = airports.IATA.str.len() == 3

    airports = airports[select_rows]

    write_parquet(airports, write_filepath)


# takes the airport list and the path to the airport database location
# returns the selected rows from source truth
def get_airport_locations(airports: list | Index, airport_locations_filepath: str | Path):
    df = load_parquet(airport_locations_filepath)
    return df[df["IATA"].isin(airports)]


def get_airports_from_flight_df(df: DataFrame) -> Index:
    # Get unique airport IATA codes from the origin and destination airports.
    origin_airports = pd.Index(df["ORIGIN_AIRPORT"].unique())
    destination_airports = pd.Index(df["DESTINATION_AIRPORT"].unique())
    airports = origin_airports.union(destination_airports)

    return airports


def extract(flight_filepath: str | Path, airport_location_for_weather_filepath: str | Path):
    columns = ["ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCH_DEP_DATE", "SCH_ARI_DATE"]
    log_progress(f"Fetching columns from {flight_filepath} to get airport details.")
    df = load_parquet(flight_filepath, columns=columns)
    start_date, end_date = get_date_range(df)

    # increase the end to by one day to account for data on the last date
    end_date = str((pd.to_datetime(end_date) + pd.Timedelta(1, unit="D")).date())
    log_progress("Fetching date range for weather details")

    airports = {
        "airports": get_airports_from_flight_df(df).to_list(),
        "start_date": start_date,
        "end_date": end_date,
    }

    log_progress(f"Writing the airport details at {airport_location_for_weather_filepath}")
    write_json(airports, airport_location_for_weather_filepath)
