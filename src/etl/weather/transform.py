import pickle

import numpy as np
import pandas as pd
from utils.constants import DATA_DIR
from utils.logger import log_progress


def transform(
    responses: list, airport_locations: pd.DataFrame, features
) -> pd.DataFrame:
    # Create pandas DateTimeIndex for the range of date with a time step of 1 hour
    responses = responses[0]
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
                airport_locations["IATA"].values.reshape(-1, 1),
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
            [
                responses[j].Hourly().Variables(i).ValuesAsNumpy()
                for i in range(len(features))
            ],
            axis=1,
        )
        for j in range(len(responses))
    ]

    with open(DATA_DIR / "weather_stacked.pickle", "wb") as f:
        pickle.dump(weather_stacked, f)
    with open(DATA_DIR / "weather_stacked_index.pickle", "wb") as f:
        pickle.dump(index, f)

    weather = pd.DataFrame(
        np.concatenate(weather_stacked, axis=0),
        columns=features,
        index=index,
    )
    # Uppercase all the column names
    weather.columns = weather.columns.str.upper()

    return weather
