import numpy as np
import pandas as pd
from utils.helper import flight_date_range, get_date_range
from utils.loaders import get_config_resource_path, load_yaml
from utils.logger import log_progress

"""Create features?
1. Previous_Flight_Delay (How late was the plane coming in?).
2. Origin_Airport_Congestion (Average delay of all flights leaving that airport in the last hour).
"""


def create_new_features(df: pd.DataFrame) -> pd.DataFrame:
    print("Feature Engineering")
    """
    1. First feature set
        The function creates the following features in the dataframe,
        1. SCH_DEP_HOUR
        2. SCH_DEP_MIN
        3. DEP_HOUR
        4. DEP_MIN
        5. SCH_ARI_HOUR
        6. SCH_ARI_MIN
        7. ARI_HOUR
        8. ARI_MIN
        using the times in the columns SCHEDULED_DEPARTURE, DEPARTURE_TIME, SCHEDULED_ARRIVAL, ARRIVAL_TIME.

        The time of departure and arrival are stored as float values in SCHEDULED_DEPARTURE, DEPARTURE_TIME, SCHEDULED_ARRIVAL, ARRIVAL_TIME. The float value is interpreted in four-digit format where the first two digits are for hours and the next two digits for minutes.

    2. Second feature set
        1. Recalculate SCH_ARI_MIN and ARI_MIN using the departure date and time, and the time of travel.
        2. Calculate SCH_DEP_DATE, SCH_ARI_DATE, and SCH_DEP_DAY.
    """

    # Separate the hour and minute values.
    h, m = np.divmod(df["SCHEDULED_DEPARTURE"], 100)
    df["SCH_DEP_HOUR"], df["SCH_DEP_MIN"] = h, m
    h, m = np.divmod(df["DEPARTURE_TIME"], 100)
    df["DEP_HOUR"], df["DEP_MIN"] = h, m

    h, m = np.divmod(df["SCHEDULED_ARRIVAL"], 100)
    df["SCH_ARI_HOUR"], df["SCH_ARI_MIN"] = h, m
    h, m = np.divmod(df["ARRIVAL_TIME"], 100)
    df["ARI_HOUR"], df["ARI_MIN"] = h, m

    # Reorder the columns. Move the created columns after the YEAR, MONTH, and DAY columns.
    cols = df.columns.to_list()
    cols = (
        cols[:3]
        + [
            "SCH_DEP_HOUR",
            "SCH_DEP_MIN",
            "DEP_HOUR",
            "DEP_MIN",
            "SCH_ARI_HOUR",
            "SCH_ARI_MIN",
            "ARI_HOUR",
            "ARI_MIN",
        ]
        + cols[3:-8]
    )
    log_progress(
        "Created features: SCH_DEP_HOUR, SCH_DEP_MIN, DEP_HOUR, DEP_MIN, SCH_ARI_HOUR, SCH_ARI_MIN, ARI_HOUR, ARI_MIN"
    )

    # Reorder the columns using the rearranged column names.
    df = df.reindex(columns=cols)

    # Drop the time columns after the hour and minute values are extracted.
    df.drop(
        ["SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME"],
        axis=1,
        inplace=True,
    )
    log_progress(
        "Features dropped: SCHEDULED_DEPARTURE, DEPARTURE_TIME, SCHEDULED_ARRIVAL, ARRIVAL_TIME"
    )

    # Convert the scheduled departure date and time values to pandas datetime object
    sch_departure_datetime = pd.to_datetime(
        df[["YEAR", "MONTH", "DAY", "SCH_DEP_HOUR", "SCH_DEP_MIN"]].rename(
            columns={
                "YEAR": "year",
                "MONTH": "month",
                "DAY": "day",
                "SCH_DEP_HOUR": "hour",
                "SCH_DEP_MIN": "minute",
            }
        )
    )

    # Convert the departure date and time values to pandas datetime object
    departure_datetime = pd.to_datetime(
        df[["YEAR", "MONTH", "DAY", "DEP_HOUR", "DEP_MIN"]].rename(
            columns={
                "YEAR": "year",
                "MONTH": "month",
                "DAY": "day",
                "DEP_HOUR": "hour",
                "DEP_MIN": "minute",
            }
        )
    )

    # Convert the scheduled time of travel values to pandas datetime object
    sch_time = pd.to_timedelta(df["SCHEDULED_TIME"], unit="m")
    # Convert the actual time of travel to pandas datetime object
    elapsed_time = pd.to_timedelta(df["ELAPSED_TIME"], unit="m")

    # The hour column of the arrival time is not the sum of departure and time of travel.
    # Time zone difference needs to be taken into account.
    # Assuming the timezone difference is integral number of hours, we can calculate the ARI_MIN using the departure datetime and the time of travel. We recalculate the feature as there can be errors in the precomputed values.
    df["SCH_ARI_MIN"] = (sch_departure_datetime + sch_time).dt.minute
    log_progress("SCH_ARI_MIN recalculated")
    df["ARI_MIN"] = (departure_datetime + elapsed_time).dt.minute
    log_progress("ARI_MIN recalculated")

    """
    sch_time = pd.to_timedelta(df["SCHEDULED_TIME"], unit="m")
    We have ignored the timezone difference for the present and calculate the scheduled arrival date as sum of sch_departure_datetime and 
    """
    sch_arrival_datetime = sch_departure_datetime + sch_time
    df.insert(0, "SCH_DEP_DATE", sch_departure_datetime.dt.date.astype("string"))
    df.insert(1, "SCH_ARI_DATE", sch_arrival_datetime.dt.date.astype("string"))
    df.insert(
        df.columns.get_loc("SCH_ARI_HOUR"), "SCH_ARI_DAY", sch_arrival_datetime.dt.day
    )

    log_progress("Features created: SCH_DEP_DATE, SCH_ARI_DATE, SCH_ARI_HOUR\n")

    flight_date_range["start_date"], flight_date_range["end_date"] = get_date_range(df)

    config_path = get_config_resource_path("config")
    config = load_yaml(config_path)

    df = df[config["pipeline"]["load_flight_gold_columns"]]
    df = df.rename(columns={"DAY": "SCH_DEP_DAY"})

    return df
