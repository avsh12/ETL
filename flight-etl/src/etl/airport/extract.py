import pandas as pd
from airports import airport_data
from pandas import DataFrame, Index
from upath import UPath as Path

from etl.utils.file_handler import load_parquet, write_json, write_parquet
import logging

logger = logging.getLogger(__name__)


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
def generate_airport_locations(write_filepath: str):
    airports_us = airport_data.get_airport_by_country_code("US")
    airports = pd.DataFrame(get_airport_data(airports_us))

    airports.columns = airports.columns.str.upper()

    airports = airports.dropna()
    select_rows = airports.IATA.str.len() == 3

    airports = airports[select_rows]

    write_parquet(write_filepath, airports)


# takes the airport list and the path to the airport database location
# returns the selected rows from source truth
def get_airport_locations(airports: list | Index, airport_locations_filepath: str):
    df = load_parquet(airport_locations_filepath)
    logger.info("Getting the location of the airports from the airport codes")
    logger.debug(f"Features present in the airport location data are: {df.columns}")
    return df[df["IATA"].isin(airports)]


def get_airports_from_flight_df(df: DataFrame) -> Index:
    logger.info("Getting the unique airport codes present in the flight data")
    # Get unique airport IATA codes from the origin and destination airports.
    origin_airports = pd.Index(df["ORIGIN_AIRPORT"].unique())
    destination_airports = pd.Index(df["DESTINATION_AIRPORT"].unique())
    airports = origin_airports.union(destination_airports)
    logger.debug(f"Number of unique airport codes present in the flight data: {len(airports)}")

    return airports


def extract(flight_filepath: str, airport_location_for_weather_filepath: str):
    columns = ["ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCH_DEP_DATE", "SCH_ARI_DATE"]
    df = load_parquet(flight_filepath, columns=columns)

    logger.info("Fetching date range from the flight data for weather details")
    start_date, end_date = get_date_range(df)
    logger.debug(f"The flights are in the range: {{start_date:{start_date}, end_date: {end_date}}}")
    # increase the end to by one day to account for data on the last date
    end_date = str((pd.to_datetime(end_date) + pd.Timedelta(1, unit="D")).date())
    logger.debug(
        f"Increased the end_date by one day. The date range used: {{start_date:{start_date}, end_date: {end_date}}}"
    )

    airports = {
        "airports": get_airports_from_flight_df(df).to_list(),
        "start_date": start_date,
        "end_date": end_date,
    }
    write_json(airport_location_for_weather_filepath, airports)
