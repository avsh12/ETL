import numpy as np
import pandas as pd
from airports import airport_data


def get_airports(df: pd.DataFrame) -> pd.Index:
    # Get unique airport IATA codes from the origin and destination airports.
    origin_airports = pd.Index(df["ORIGIN_AIRPORT"].unique())
    destination_airports = pd.Index(df["DESTINATION_AIRPORT"].unique())
    airports = origin_airports.union(destination_airports)

    return airports


def get_airports_location(airports: pd.Index) -> pd.DataFrame:
    # Fetch the details of airports using their IATA codes.
    airports = airport_data.get_multiple_airports(airports)

    num_airports = len(airports)
    iata_codes = np.zeros(num_airports, dtype="U3")
    latitudes = np.zeros(num_airports)
    longitudes = np.zeros(num_airports)

    # Store IATA code, Latitude, and Longitude of the airports as DataFrame.
    for ix, airport in enumerate(airports):
        iata_codes[ix] = airport["iata"]
        latitudes[ix] = airport["latitude"]
        longitudes[ix] = airport["longitude"]
    airport_column_names = ["IATA", "LATITUDE", "LONGITUDE"]
    airport_columns = [iata_codes, latitudes, longitudes]

    airports = pd.DataFrame(dict(zip(airport_column_names, airport_columns)))
    return airports
