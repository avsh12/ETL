import logging

import pandas as pd
from upath import UPath as Path

from etl.utils.file_handler import load_parquet

logger = logging.getLogger(__name__)


def join(flight: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    weather = weather.reset_index()

    logger.info("Merging the Weather details for the departure airports")
    flight_weather = pd.merge(
        flight,
        weather.rename(
            columns={
                "DATE": "SCH_DEP_DATE",
                "HOUR": "SCH_DEP_HOUR",
                "IATA": "ORIGIN_AIRPORT",
            }
        ),
        on=["ORIGIN_AIRPORT", "SCH_DEP_DATE", "SCH_DEP_HOUR"],
        how="left",
    )

    logger.info("Merging the weather details for the arrival airports")
    flight_weather = pd.merge(
        flight_weather,
        weather.rename(
            columns={
                "DATE": "SCH_ARI_DATE",
                "HOUR": "SCH_ARI_HOUR",
                "IATA": "DESTINATION_AIRPORT",
            }
        ),
        on=["DESTINATION_AIRPORT", "SCH_ARI_DATE", "SCH_ARI_HOUR"],
        how="left",
    )

    logger.debug(f"Dropping the date features: {["SCH_DEP_DATE", "SCH_ARI_DATE"]}")
    # Drop the date and time columns that are not needed.
    flight_weather.drop(["SCH_DEP_DATE", "SCH_ARI_DATE"], axis=1, inplace=True)

    return flight_weather


def join_flight_weather(flight_filepath: str, weather_filepath: str, write_filepath: str):
    flight_df = load_parquet(flight_filepath)
    weather_df = load_parquet(weather_filepath)

    flight_weather_df = join(flight_df, weather_df)
    flight_weather_df.to_parquet(write_filepath)
