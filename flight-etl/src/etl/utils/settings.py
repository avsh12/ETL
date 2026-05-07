from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).parent.parent.parent


class Settings(BaseSettings):
    # Flight Storage Paths
    BRONZE_FLIGHT_PATH: Path
    SILVER_FLIGHT_PATH: Path
    GOLD_FLIGHT_PATH: Path
    DISCARDED_FLIGHT_PATH: Path

    BRONZE_FLIGHT_FILEPATH: str
    SILVER_FLIGHT_FILEPATH: str
    GOLD_FLIGHT_FLEPATH: str
    DISCARDED_FLIGHT_FILEPATH: str

    # Airport Storage Paths
    AIRPORT_LOCATIONS_PATH: Path
    AIRPORTS_FOR_WEATHER_PATH: Path

    AIRPORT_LOCATIONS_FILEPATH: str
    AIRPORTS_FOR_WEATHER_FILEPATH: str

    # Weather Storage Paths
    BRONZE_WEATHER_PATH: Path
    SILVER_WEATHER_PATH: Path

    BRONZE_WEATHER_FILEPATH: str
    SILVER_WEATHER_FILEPATH: str

    # Flight-weather Merged Storage Paths
    GOLD_FLIGHT_WEATHER_PATH: Path
    TRANSFORMED_FLIGHT_WEATHER_PATH: Path

    GOLD_FLIGHT_WEATHER_FILEPATH: str
    TRANSFORMED_FLIGHT_WEATHER_FILEPATH: str

    model_config = SettingsConfigDict(env_file=ROOT / ".env", extra="ignore")
