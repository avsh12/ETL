import logging
import time

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from etl.adapters.adapter import BaseExtract, BaseTransform
from etl.airport.extract import get_airport_locations
from etl.core.constants import SCHEMA_FILEPATH, WEATHER_URL
from etl.utils.file_handler import (
    load_json,
    load_yaml,
    read_bin,
    write_bin,
    write_parquet,
)
from etl.utils.helper import get_api_calls_per_min
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from upath import UPath as Path
from urllib3.util.retry import Retry

load_dotenv()

logger = logging.getLogger(__name__)

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


class WeatherExtract(BaseExtract):
    def __init__(
        self,
        airport_location_for_weather_filepath,
        airport_location_filepath,
        schema_filepath=SCHEMA_FILEPATH,
        weather_url=WEATHER_URL,
    ) -> None:
        super().__init__()
        self.weather_url = weather_url
        self.airport_location_for_weather_filepath = airport_location_for_weather_filepath
        self.airport_location_filepath = airport_location_filepath
        self.schema_filepath = schema_filepath

    def __enter__(self):
        self.total_retries = 3
        self.status_forcelist = [429, 500, 502, 503, 504]

        self.retry_strategy = Retry(total=self.total_retries, status_forcelist=self.status_forcelist)
        self.openmeteo_adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.session = requests.Session()
        logger.debug(f"Using the session {self.session}")
        self.session.mount(self.weather_url, self.openmeteo_adapter)
        return self

    def __exit__(self, type, value, traceback):
        self.session.close()

    def _request_weather_api(
        self,
        features: list | np.ndarray,
        latitude: list[float] | np.ndarray,
        longitude: list[float] | np.ndarray,
        start_date: str,
        end_date: str,
    ):
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": features,
            "format": "flatbuffers",
        }
        logger.info(f"Sending request to {self.weather_url}")
        try:
            response = self.session.post(url=self.weather_url, data=params, stream=True)
            return response
        except RetryError as e:
            logger.error(f"Error while connecting to the weather API: {e}")

    # Based on the API rate limit split the num_values of locations into batches.
    def _get_num_data_splits(self, start_date: str, end_date: str, num_features: int, num_values: int) -> int:
        logger.debug("Calculating the number of splits required for API call")
        num_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
        api_calls_per_min = get_api_calls_per_min(num_days, num_features, num_values)

        num_splits = int(num_values // (api_calls_per_min * 140 / (num_days * num_features)))

        logger.debug(f"Total time: {num_splits} min")

        return num_splits

    def _split_into_batches(self, data: pd.DataFrame, start_date, end_date, num_features):
        logger.info("Splitting the data into batches")
        num_values = len(data)
        num_splits = self._get_num_data_splits(start_date, end_date, num_features, num_values)
        logger.debug(f"Number of batches: {num_splits}")

        if num_splits != 0:
            latlong_batches = np.array_split(data.values.T, num_splits, axis=1)
        else:
            latlong_batches = [data.values.T]

        return latlong_batches

    def _batch_request_weather_api(self, latlong: pd.DataFrame, features: list, start_date: str, end_date: str):
        latlong_batches = self._split_into_batches(
            data=latlong,
            start_date=start_date,
            end_date=end_date,
            num_features=len(features),
        )

        logger.info("Sending weather request in batches")
        del_t = 0
        responses = []
        for ix, (latitude, longitude) in enumerate(latlong_batches):
            try:
                start = time.time()
                response = self._request_weather_api(
                    features=features,
                    latitude=latitude,
                    longitude=longitude,
                    start_date=start_date,
                    end_date=end_date,
                )
                del_t = time.time() - start

                responses.append(response)
            except Exception as e:
                logger.error(f"Error occured while executing batch {ix} of airport locations: {e}")

            logger.debug("Waiting for a moment before sending the next request")
            if (del_t < 60) & (ix < len(latlong_batches) - 1):
                time.sleep(65 - del_t)

        return responses

    # def fetch_weather_details(self,
    def extract(self):
        logger.info("Reading the schema for weather features")
        schema = load_yaml(self.schema_filepath)
        weather_features = schema["WEATHER_FEATURES"]

        # get the airport details for which the weather details need to be fetched
        airports = load_json(self.airport_location_for_weather_filepath)
        start_date = airports["start_date"]
        end_date = airports["end_date"]
        # returns a list of airport codes
        airports = airports["airports"]

        # returns a dataframe object
        airport_locations = get_airport_locations(airports, self.airport_location_filepath)

        responses = self._batch_request_weather_api(
            features=weather_features,
            latlong=airport_locations[["LATITUDE", "LONGITUDE"]],
            start_date=start_date,
            end_date=end_date,
        )

        # merge all the binary responses
        logger.info("Merging the weather responses into a single binary object")
        content = bytes(b"")
        for res in responses:
            content += res.content
        return content

    def load(self, write_filepath: str, content: bytes):
        write_bin(write_filepath, content)

    def execute(self, write_filepath: str):
        content = self.extract()
        self.load(write_filepath, content)
        return write_filepath


class WeatherTransform(BaseTransform):
    def __init__(
        self,
        bronze_weather_filepath,
        airport_location_for_weather_filepath,
        schema_filepath=SCHEMA_FILEPATH,
    ) -> None:
        super().__init__()
        self.bronze_weather_filepath = bronze_weather_filepath
        self.airport_location_for_weather_filepath = airport_location_for_weather_filepath
        self.schema_filepath = schema_filepath

    def _parse_flatbuffer_binary(self, content: bytes) -> list[WeatherApiResponse]:
        """The binary data stores the weather details for each location in the following format.
        [message_length][message][message_length][message]...
        The first four bytes store the length of the message following it. The next block of size message_length stores the actual weather detail.
        This sequence is repeated for each location.

        Args:
            content (bytes): The actual flatbuffer binary data.

        Returns:
            openmeteo_objects (WeatherApiResponse): The structured FlatBuffer objects.
        """
        logger.info("Parsing binary weather data to openmeteo objects")
        openmeteo_objects = []

        metadata_cursor = 0
        message_cursor = 4
        content_size = len(content)

        while metadata_cursor < content_size:
            message_size = int.from_bytes(bytes(content[metadata_cursor : metadata_cursor + 4]), byteorder="little")

            buffer = bytearray(content[message_cursor : message_cursor + message_size])

            openmeteo_objects.append(WeatherApiResponse.GetRootAsWeatherApiResponse(buffer, 0))

            metadata_cursor += message_size + 4
            message_cursor += message_size + 4

        return openmeteo_objects

    def _parse_flatbuffer_objects_to_dataframe(
        self,
        responses: list[WeatherApiResponse],
        airport_locations: pd.DataFrame | pd.Series,
        features,
    ) -> pd.DataFrame:
        logger.info("Parsing the openmeteo objects to DataFrame")
        logger.debug(
            f"Creating pandas multi-index object with {["IATA", "DATE", "HOUR"]} for weather details dataframe"
        )
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
        len_iata = len(airport_locations)
        len_date = len(t)

        # Concatenate the IATA code, date, and hour columns
        # Each IATA code is to be repeated vertically upto the full range of date.
        # Each date is to be repeated for the full set of IATA codes
        index = np.hstack(
            [
                np.repeat(
                    airport_locations.to_numpy().reshape(-1, 1),
                    len_date,
                    axis=0,
                ),
                np.tile(t, (len_iata, 1)),
            ]
        )

        # Create multiindex using the index array
        index = pd.MultiIndex.from_arrays(index.T, names=["IATA", "DATE", "HOUR"])
        # Create DataFrame from the features in the respnses and the multiindex as index of the DataFrame.

        logger.debug("Restructring the weather features into table with features as columns")
        weather_stacked = [
            np.stack(
                [responses[j].Hourly().Variables(i).ValuesAsNumpy() for i in range(len(features))],
                axis=1,
            )
            for j in range(len(responses))
        ]

        logger.info("Creating the weather DataFrame")
        weather = pd.DataFrame(
            np.concatenate(weather_stacked, axis=0),
            columns=features,
            index=index,
        )
        # Uppercase all the column names
        weather.columns = weather.columns.str.upper()

        return weather

    def transform(self):
        schema = load_yaml(self.schema_filepath)
        weather_features = schema["WEATHER_FEATURES"]

        responses = read_bin(self.bronze_weather_filepath)

        openmeteo_objects = self._parse_flatbuffer_binary(responses)

        airports = load_json(self.airport_location_for_weather_filepath)
        airport_locations = pd.Series(airports["airports"])

        weather_dataframe = self._parse_flatbuffer_objects_to_dataframe(
            openmeteo_objects, airport_locations, weather_features
        )
        return weather_dataframe

    def load(self, write_filepath: str, content: pd.DataFrame):
        write_parquet(write_filepath, content)

    def execute(self, write_filepath: str):
        content = self.transform()
        self.load(write_filepath, content)
        return write_filepath
