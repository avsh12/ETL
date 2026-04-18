import pickle
from pathlib import Path

import pandas as pd
from etl import airports, date_encoding, flight, index_categories, join, scale, weather
from utils.constants import DATA_DIR
from utils.helper import flight_date_range
from utils.loaders import get_config_resource_path, load_yaml
from utils.logger import log_progress


def flight_etl(filepath: str | Path | None = None) -> pd.DataFrame:
    schema_path = get_config_resource_path("schema")
    schema = load_yaml(schema_path)

    # Flight Extract
    flight_data = flight.extract.extract(filepath)

    # Schema Validation
    flight_data = flight.schema_validation.schema_validaton(
        flight_data, schema["bronze_schema"]
    )

    # Clean & Deduplicate
    flight_data = flight.clean.clean(flight_data)

    # Flight Transform & Feature Engineering
    flight_data = (
        flight_data.pipe(flight.clean.drop_null)
        .pipe(flight.transform.create_new_features)
        .pipe(flight.clean.drop_null)
    )

    return flight_data


def airport_etl(flight_data: pd.DataFrame) -> pd.DataFrame:
    airport_codes = airports.get_airports(flight_data)
    return airports.get_airports_location(airport_codes)


def weather_etl(
    airport_locations: pd.DataFrame, features: list, start_date: str, end_date: str
) -> pd.DataFrame:
    # Weather Extract
    weather_data = weather.extract.extract(
        airport_locations, features, start_date, end_date
    )

    # Store the fetched data
    config_path = get_config_resource_path("config")
    config = load_yaml(config_path)

    bronze_file_path = (DATA_DIR / config["data"]["weather_bronze_data"]).resolve()
    with open(bronze_file_path, "wb") as f:
        pickle.dump(weather_data, f)

    # Weather Clean and Transform
    weather_data = weather.clean.clean(weather_data)
    weather_data = weather.transform.transform(
        weather_data, airport_locations, features
    )

    weather_data.to_parquet(DATA_DIR / config["data"]["weather_gold_data"])

    return weather_data


def pipeline(filepath: str | Path | None = None) -> pd.DataFrame:
    features = """temperature_2m
    rain
    snowfall
    cloud_cover_low
    cloud_cover_high
    wind_speed_10m
    wind_speed_100m
    wind_gusts_10m""".split()

    # Flight ETL
    flight_data = flight_etl(filepath)
    log_progress(f"Flight data shape: {flight_data.shape}")

    # Airports Location
    airport_locations = airport_etl(flight_data)
    log_progress(f"airport location data shape: {airport_locations.shape}")

    start_date, end_date = flight_date_range.values()
    log_progress(f"Start date: {start_date}\nEnd date: {end_date}\n")

    # Weather ETL
    weather_data = weather_etl(airport_locations, features, start_date, end_date)

    # Combine and Join
    flight_weather_data = join.combine_flight_weather(flight_data, weather_data)

    del flight_data, weather_data

    # Index Categories
    flight_weather_data, categories = index_categories.index_categorical(
        flight_weather_data, airport_locations
    )

    # Scaling and Cyclical Encoding
    flight_weather_data = scale.scale(flight_weather_data)
    flight_weather_data = date_encoding.encode_date(flight_weather_data)
    target = flight_weather_data["SCH_ARI_DELAY"]
    features = flight_weather_data.drop(columns=["SCH_ARI_DELAY"])

    return features, target, categories


# config_path = get_config_resource_path("config")
# config = load_yaml(config_path)

# flight_weather, categories = pipeline()

# with open(DATA_DIR / config["data"]["flight_weather_categories"], "wb") as f:
#     pickle.dump(categories, f)

# flight_weather.to_parquet(DATA_DIR / config["data"]["flight_weather"])
