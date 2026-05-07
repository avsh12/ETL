import numpy as np
import pandas as pd
from utils.loaders import get_config_resource_path, load_yaml

"""MONTH	DAY_OF_WEEK	SCH_DEP_DAY	SCH_DEP_HOUR	SCH_DEP_MIN	SCH_ARI_DAY	SCH_ARI_HOUR	SCH_ARI_MIN"""


def encode_date(df: pd.DataFrame) -> pd.DataFrame:
    time_period = {"month": 12, "day_of_week": 7, "day": 30, "hour": 24, "minute": 60}

    time_cols = {
        "month": ["MONTH"],
        "day_of_week": ["DAY_OF_WEEK"],
        "day": ["SCH_DEP_DAY", "SCH_ARI_DAY"],
        "hour": ["SCH_DEP_HOUR", "SCH_ARI_HOUR"],
        "minute": ["SCH_DEP_MIN", "SCH_ARI_MIN"],
    }

    for ix, col in enumerate(time_cols):
        df[[c + "_SIN" for c in time_cols[col]]] = np.sin(
            2 * np.pi * df[time_cols[col]] / time_period[col]
        )
        df[[c + "_COS" for c in time_cols[col]]] = np.cos(
            2 * np.pi * df[time_cols[col]] / time_period[col]
        )

    config_path = get_config_resource_path("config")
    config = load_yaml(config_path)

    df = df[config["pipeline"]["load_flight_weather_columns_final"]]
    return df
