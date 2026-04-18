import pickle
import time

import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from utils.constants import DATA_DIR
from utils.helper import get_api_calls_per_min, get_date_range
from utils.logger import log_progress


# features is a newline-separated string of the data needed
# Weight of API call is calculated as
# weight = nLocations * (nDays / 14) * (nVariables / 10)
def request_weather_api(url, latitude, longitude, start_date, end_date, features):
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": features,
    }

    return openmeteo.weather_api(url, params=params)


def get_num_data_splits(start_date, end_date, num_features, num_values):
    num_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
    api_calls_per_min = get_api_calls_per_min(num_days, num_features, num_values)

    num_splits = num_values // (api_calls_per_min * 140 / (num_days * num_features))

    print(f"Total time: {num_splits} min")

    return num_splits


def extract(
    airports: pd.DataFrame,
    features: list,
    start_date="2015-01-01",
    end_date="2015-01-03",
):
    url = "https://archive-api.open-meteo.com/v1/archive"

    num_features = len(features)
    num_values = len(airports)
    log_progress(f"Number of features: {num_features}")
    log_progress(f"Number of values: {num_values}")

    num_splits = get_num_data_splits(start_date, end_date, num_features, num_values)
    log_progress(f"Number of batches = {num_splits}")

    responses = []
    logs = []

    if num_splits != 0:
        latlong_splitted = np.array_split(
            airports[["LATITUDE", "LONGITUDE"]].values.T, num_splits, axis=1
        )
    else:
        latlong_splitted = [airports[["LATITUDE", "LONGITUDE"]].values.T]

    # For sample run, consider two airports.
    # latlong_splitted = [airports.loc[:2, ["LATITUDE", "LONGITUDE"]].values.T]
    del_t = 0

    for ix, (latitude, longitude) in enumerate(latlong_splitted):
        try:
            start = time.time()
            response = request_weather_api(
                url, latitude, longitude, start_date, end_date, features
            )
            del_t = time.time() - start

            responses.extend(response)

        except Exception as e:
            error = f"Error occured in block {ix}: {e}"
            logs.append((ix, error))
            print(error)

        if (del_t < 60) & (ix < len(latlong_splitted) - 1):
            time.sleep(65 - del_t)

    return responses, logs
