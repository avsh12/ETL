import logging
import os
import time
from pathlib import Path

import numpy as np
import openmeteo_requests
import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from urllib3.util.retry import Retry

from etl.airport.extract import get_airport_locations
from utils.constants import WEATHER_FEATURES
from utils.helper import get_api_calls_per_min
from utils.loaders import load_json, read_bin, write_bin, write_parquet
from utils.logger import log_progress

load_dotenv()

# open-meteo source: https://github.com/open-meteo/sdk

# features is a newline-separated string of the data needed
# Weight of API call is calculated as
# weight = nLocations * (nDays / 14) * (nVariables / 10)
# Format of response. The data is stored as FlatBuffer.
"""
table WeatherApiResponse {
  latitude: float;
  longitude: float;
  elevation: float;
  generation_time_milliseconds: float;
  location_id: int64;
  model: Model;
  utc_offset_seconds: int32;
  timezone: string;
  timezone_abbreviation: string;
  current: VariablesWithTime;
  daily: VariablesWithTime;
  hourly: VariablesWithTime;
  minutely_15: VariablesWithTime;
  monthly: VariablesWithMonth;
  weekly: VariablesWithTime;
}

table VariablesWithTime {
  time: int64;
  time_end: int64;
  interval: int32;
  variables: [VariableWithValues];
}

table VariablesWithMonth {
  year: int16;
  month: int8;
  count: int32;
  variables: [VariableWithValues];
}

table VariableWithValues {
  variable: Variable;
  unit: Unit;

  value: float; // Only used for current conditions
  values: [float]; // Contains a time series of data
  values_int64: [int64];  // Only for sunrise/set as a unix timestamp

  altitude: int16;
  aggregation: Aggregation;
  pressure_level: int16;
  depth: int16;
  depth_to: int16;
  ensemble_member: int16;
  previous_day: int16;
  probability: Probability;
}

enum Aggregation: ubyte {
  none = 0,
  minimum,
  maximum,
  mean,
  p10,
  p25,
  median,
  p75,
  p90,
  dominant,
  sum,
  spread,
  anomaly,
  sot10,
  sot90,
  efi
}

enum Variable: ubyte {
  temperature_2m,
  rain,
  snowfall,
  cloud_cover_low,
  cloud_cover_high,
  wind_speed_10m,
  wind_speed_100m,
  wind_gusts_10m,
  ...
"""


def request_weather_api(
    url: str,
    latitude: list[float] | np.ndarray,
    longitude: list[float] | np.ndarray,
    start_date: str,
    end_date: str,
    features,
):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": features,
        "format": "flatbuffers",
    }

    retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504])
    openmeteo_adapter = HTTPAdapter(max_retries=retry_strategy)

    with requests.Session() as session:
        session.mount(url, openmeteo_adapter)
        try:
            response = session.post(url=url, data=params, stream=True)
            return response
        except RetryError as e:
            logging.error(f"Error while connecting to the weather API: {e}")


# Based on the API rate limit split the num_values of locations into batches.
def get_num_data_splits(start_date: str, end_date: str, num_features: int, num_values: int) -> int:
    num_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
    api_calls_per_min = get_api_calls_per_min(num_days, num_features, num_values)

    num_splits = int(num_values // (api_calls_per_min * 140 / (num_days * num_features)))

    print(f"Total time: {num_splits} min")

    return num_splits


def call_request_on(url: str, airports: pd.DataFrame, features: list, start_date: str, end_date: str):
    num_features = len(features)
    num_values = len(airports)
    log_progress(f"Number of features: {num_features}")
    log_progress(f"Number of values: {num_values}")

    num_splits = get_num_data_splits(start_date, end_date, num_features, num_values)
    log_progress(f"Number of batches = {num_splits}")

    responses = []

    if num_splits != 0:
        latlong_splitted = np.array_split(airports[["LATITUDE", "LONGITUDE"]].values.T, num_splits, axis=1)
    else:
        latlong_splitted = [airports[["LATITUDE", "LONGITUDE"]].values.T]

    # For sample run, consider two airports.
    # latlong_splitted = [airports.loc[:2, ["LATITUDE", "LONGITUDE"]].values.T]
    del_t = 0

    for ix, (latitude, longitude) in enumerate(latlong_splitted):
        try:
            start = time.time()
            response = request_weather_api(url, latitude, longitude, start_date, end_date, features)
            del_t = time.time() - start

            responses.append(response)

        except Exception as e:
            error = f"Error occured in block {ix}: {e}"
            logging.error(f" Error occured while executing batch {ix} of airport locations: {error}")
            print(error)

        if (del_t < 60) & (ix < len(latlong_splitted) - 1):
            time.sleep(65 - del_t)

    return responses


def extract(
    airport_location_for_weather_filepath: str | Path,
    airport_location_filepath: str | Path,
    write_filepath: str | Path,
):
    # weather features to fetch.
    weather_features: list[str] = os.getenv("WEATHER_FEATURES")  # type:ignore

    # weather api url
    weather_url: str = str(os.getenv("WEATHER_URL"))

    # get the airport details for which the weather details need to be fetched
    log_progress(f"Reading the airport location file at {airport_location_for_weather_filepath}")
    airports = load_json(airport_location_for_weather_filepath)
    start_date = airports["start_date"]
    end_date = airports["end_date"]
    # returns a list of airport codes
    airports = airports["airports"]

    # returns a dataframe object
    log_progress("Generate dataframe for the airport lcoations to fetch for weatehr details.")
    airport_locations = get_airport_locations(airports, airport_location_filepath)

    responses = call_request_on(
        url=weather_url,
        features=weather_features,
        airports=airport_locations,
        start_date=start_date,
        end_date=end_date,
    )

    # merge all the binary responses
    content = bytes(b"")
    for res in responses:
        content += res.content

    write_bin(content, write_filepath)

    return write_filepath


def get_openmeteo_objects_from_binary(content: bytes):
    from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse

    openmeteo_objects = []

    metadata_cursor = 0
    message_cursor = 4
    content_size = len(content)

    while metadata_cursor < content_size:
        message_size = int.from_bytes(bytes(content[metadata_cursor : metadata_cursor + 4]), byteorder="little")

        print(f"Metadata cursor at {metadata_cursor}", flush=True)
        print(f"Message size: {message_size}")
        buffer = bytearray(content[message_cursor : message_cursor + message_size])

        openmeteo_objects.append(WeatherApiResponse.GetRootAsWeatherApiResponse(buffer, 0))

        metadata_cursor += message_size + 4
        message_cursor += message_size + 4

    return openmeteo_objects


def transform(responses: list, airport_locations: pd.DataFrame | pd.Series, features) -> pd.DataFrame:
    # Create pandas DateTimeIndex for the range of date with a time step of 1 hour
    t = pd.date_range(
        start=pd.to_datetime(responses[0].Hourly().Time(), unit="s"),
        end=pd.to_datetime(responses[0].Hourly().TimeEnd(), unit="s"),
        freq=pd.Timedelta(responses[0].Hourly().Interval(), unit="s"),
        inclusive="left",
    )

    # Store the date and hour as a numpy array
    t = np.array([[str(t.date()), int(t.hour)] for t in t], dtype="object")

    # create repeated arrays for IATA codes and date to create cartesian product of
    # each iata code with the date array
    l_iata = len(airport_locations)
    l_date = len(t)

    # Concatenate the IATA code, date, and hour columns
    # Each IATA code is to be repeated vertically upto the full range of date.
    # Each date is to be repeated for the full set of IATA codes
    index = np.hstack(
        [
            np.repeat(
                airport_locations.to_numpy().reshape(-1, 1),
                l_date,
                axis=0,
            ),
            np.tile(t, (l_iata, 1)),
        ]
    )

    # Create multiindex using the index array
    index = pd.MultiIndex.from_arrays(index.T, names=["IATA", "DATE", "HOUR"])
    # Create DataFrame from the features in the respnses and the multiindex as index of the DataFrame.

    weather_stacked = [
        np.stack(
            [responses[j].Hourly().Variables(i).ValuesAsNumpy() for i in range(len(features))],
            axis=1,
        )
        for j in range(len(responses))
    ]

    weather = pd.DataFrame(
        np.concatenate(weather_stacked, axis=0),
        columns=features,
        index=index,
    )
    # Uppercase all the column names
    weather.columns = weather.columns.str.upper()

    return weather


def weather_bin_to_parquet(
    read_filepath: str | Path,
    write_filepath: str | Path,
    airport_location_for_weather_filepath: str | Path,
):
    responses = read_bin(read_filepath)

    openmeteo_objects = get_openmeteo_objects_from_binary(responses)

    airports = load_json(airport_location_for_weather_filepath)
    airport_locations = pd.Series(airports["airports"])

    weather_dataframe = transform(openmeteo_objects, airport_locations, WEATHER_FEATURES)

    write_parquet(weather_dataframe, write_filepath)


# import os

# from dotenv import load_dotenv

# load_dotenv()

# bronze_weather_path = Path(str(os.getenv("BRONZE_WEATHER_PATH")))
# silver_weather_path = Path(str(os.getenv("SILVER_WEATHER_PATH")))
# bronze_weather_filename = "bronze_weather_2026-04-28.bin"
# silver_weather_filename = "silver_weather_2026-04-28.parquet"

# airport_locations_path = Path(str(os.getenv("AIRPORT_LOCATIONS_PATH")))
# airport_location_for_weather_path = Path(str(os.getenv("AIRPORTS_FOR_WEATHER_PATH")))
# airport_locations_filename = "airports-locations.parquet"
# airport_location_for_weather_filename = "airports_for_weather_2026-04-28.json"


# bronze_weather_filepath = str(bronze_weather_path / bronze_weather_filename)
# silver_weather_filepath = str(silver_weather_path / silver_weather_filename)
# airport_locations_filepath = str(airport_locations_path / airport_locations_filename)
# airport_location_for_weather_filepath = str(
#     airport_location_for_weather_path / airport_location_for_weather_filename
# )

# weather_bin_to_parquet(
#     airport_location_for_weather_filepath=airport_location_for_weather_filepath,
#     read_filepath=bronze_weather_filepath,
#     write_filepath=silver_weather_filepath,
# )
