from pathlib import Path

import pandas as pd

from utils.loaders import load_parquet


def join(df: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    weather = weather.reset_index()

    df_flight_weather = pd.merge(
        df,
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

    df_flight_weather = pd.merge(
        df_flight_weather,
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

    # Drop the date and time columns that are not needed.
    df_flight_weather.drop(["SCH_DEP_DATE", "SCH_ARI_DATE"], axis=1, inplace=True)

    return df_flight_weather


def join_flight_weather(flight_filepath: str | Path, weather_filepath: str | Path, write_filepath: str | Path):
    flight_df = load_parquet(flight_filepath)
    weather_df = load_parquet(weather_filepath)

    flight_weather_df = join(flight_df, weather_df)
    flight_weather_df.to_parquet(write_filepath)
