import pandas as pd


def combine_flight_weather(df: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
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
