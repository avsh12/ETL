import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
READER_MAP = {
    ".csv": "read_csv",
    ".parquet": "read_parquet",
    ".xls": "read_excel",
    ".xlsx": "read_excel",
}

WEATHER_URL: str = str(os.getenv("WEATHER_URL"))
WEATHER_FEATURES: list[str] = os.getenv("WEATHER_FEATURES")


# =================== Generate Fight filepaths =================== #

bronze_flight_path = Path(str(os.getenv("BRONZE_FLIGHT_PATH")))
silver_flight_path = Path(str(os.getenv("SILVER_FLIGHT_PATH")))
gold_flight_path = Path(str(os.getenv("GOLD_FLIGHT_PATH")))
discarded_flight_path = Path(str(os.getenv("DISCARDED_FLIGHT_PATH")))

bronze_flight_filename = Path(str(os.getenv("BRONZE_FLIGHT_FILENAME")))
silver_flight_filename = Path(str(os.getenv("SILVER_FLIGHT_FILENAME")))
gold_flight_filename = Path(str(os.getenv("GOLD_FLIGHT_FILENAME")))
discarded_flight_filename = Path(str(os.getenv("DISCARDED_FLIGHT_FILENAME")))

bronze_flight_filepath = PROJECT_ROOT / bronze_flight_path / bronze_flight_filename
silver_flight_filepath = PROJECT_ROOT / silver_flight_path / silver_flight_filename
gold_flight_filepath = PROJECT_ROOT / gold_flight_path / gold_flight_filename
discarded_flight_filepath = PROJECT_ROOT / discarded_flight_path / discarded_flight_filename


# =================== Generate Airport filepaths =================== #

airport_location_path = Path(str(os.getenv("AIRPORT_LOCATION_PATH")))
airport_location_for_weather_path = Path(str(os.getenv("AIRPORT_FOR_WEATHER_PATH")))

airport_location_filename = Path(str(os.getenv("AIRPORT_LOCATION_FILENAME")))
airport_location_for_weather_filename = Path(str(os.getenv("AIRPORT_FOR_WEATHER_FILENAME")))

airport_location_filepath = PROJECT_ROOT / airport_location_path / airport_location_filename
airport_location_for_weather_filepath = (
    PROJECT_ROOT / airport_location_for_weather_path / airport_location_for_weather_filename
)


# =================== Generate Weather filepaths =================== #

bronze_weather_path = Path(str(os.getenv("BRONZE_WEATHER_PATH")))
silver_weather_path = Path(str(os.getenv("SILVER_WEATHER_PATH")))
bronze_weather_filename = Path(str(os.getenv("BRONZE_WEATHER_FILENAME")))
silver_weather_filename = Path(str(os.getenv("SILVER_WEATHER_FILENAME")))

bronze_weather_filepath = PROJECT_ROOT / bronze_weather_path / bronze_weather_filename
silver_weather_filepath = PROJECT_ROOT / silver_weather_path / silver_weather_filename


# =================== Generate Merged filepaths =================== #

gold_merged_path = Path(str(os.getenv("GOLD_MERGED_PATH")))
feature_store_path = Path(str(os.getenv("FEATURE_STORE_PATH")))

gold_merged_filename = Path(str(os.getenv("GOLD_MERGED_FILENAME")))
feature_store_filename = Path(str(os.getenv("FEATURE_STORE_FILENAME")))

gold_merged_filepath = PROJECT_ROOT / gold_merged_path / gold_merged_filename
feature_store_filepath = PROJECT_ROOT / feature_store_path / feature_store_filename
