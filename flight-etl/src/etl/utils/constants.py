import logging
import os

from dotenv import load_dotenv
from upath import UPath as Path

load_dotenv()

logger = logging.getLogger(__name__)


def get_env_var(var_name: str) -> str:
    logger.debug(f"Reading the environment variable {var_name}")
    env_var = os.getenv(var_name)
    if env_var is None:
        logger.warning(f"The environment variable {var_name} is not set.")
        logger.warning(f"Setting {var_name} to empty string.")
    return str(env_var)


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.resolve()

DATA_DIR = Path(get_env_var("DATA_DIR"))

READER_MAP = {
    ".csv": "read_csv",
    ".parquet": "read_parquet",
    ".xls": "read_excel",
    ".xlsx": "read_excel",
}

WEATHER_URL: str = get_env_var("WEATHER_URL")

# =================== Generate Config filepaths =================== #
CONFIG_DIR: Path = Path(get_env_var("CONFIG_DIR"))
CONFIG_FILENAME = Path(get_env_var("CONFIG_FILENAME"))
SCHEMA_FILENAME = Path(get_env_var("SCHEMA_FILENAME"))
CONFIG_FILEPATH = str(CONFIG_DIR / CONFIG_FILENAME)
SCHEMA_FILEPATH = str(CONFIG_DIR / SCHEMA_FILENAME)

# =================== Generate Fight filepaths =================== #
bronze_flight_path = Path(get_env_var("BRONZE_FLIGHT_PATH"))
silver_flight_path = Path(get_env_var("SILVER_FLIGHT_PATH"))
gold_flight_path = Path(get_env_var("GOLD_FLIGHT_PATH"))
discarded_flight_path = Path(get_env_var("DISCARDED_FLIGHT_PATH"))

bronze_flight_filename = Path(get_env_var("BRONZE_FLIGHT_FILENAME"))
silver_flight_filename = Path(get_env_var("SILVER_FLIGHT_FILENAME"))
gold_flight_filename = Path(get_env_var("GOLD_FLIGHT_FILENAME"))
discarded_flight_filename = Path(get_env_var("DISCARDED_FLIGHT_FILENAME"))

bronze_flight_filepath = str(DATA_DIR / bronze_flight_path / bronze_flight_filename)
silver_flight_filepath = str(DATA_DIR / silver_flight_path / silver_flight_filename)
gold_flight_filepath = str(DATA_DIR / gold_flight_path / gold_flight_filename)
discarded_flight_filepath = str(DATA_DIR / discarded_flight_path / discarded_flight_filename)


# =================== Generate Airport filepaths =================== #
airport_location_path = Path(get_env_var("AIRPORT_LOCATION_PATH"))
airport_location_for_weather_path = Path(get_env_var("AIRPORT_FOR_WEATHER_PATH"))

airport_location_filename = Path(get_env_var("AIRPORT_LOCATION_FILENAME"))
airport_location_for_weather_filename = Path(get_env_var("AIRPORT_FOR_WEATHER_FILENAME"))

airport_location_filepath = str(DATA_DIR / airport_location_path / airport_location_filename)
airport_location_for_weather_filepath = str(
    DATA_DIR / airport_location_for_weather_path / airport_location_for_weather_filename
)


# =================== Generate Weather filepaths =================== #
bronze_weather_path = Path(get_env_var("BRONZE_WEATHER_PATH"))
silver_weather_path = Path(get_env_var("SILVER_WEATHER_PATH"))
bronze_weather_filename = Path(get_env_var("BRONZE_WEATHER_FILENAME"))
silver_weather_filename = Path(get_env_var("SILVER_WEATHER_FILENAME"))

bronze_weather_filepath = str(DATA_DIR / bronze_weather_path / bronze_weather_filename)
silver_weather_filepath = str(DATA_DIR / silver_weather_path / silver_weather_filename)


# =================== Generate Merged filepaths =================== #
feature_store_path = Path(get_env_var("FEATURE_STORE_PATH"))

feature_store_filename = Path(get_env_var("FEATURE_STORE_FILENAME"))

feature_store_filepath = str(DATA_DIR / feature_store_path / feature_store_filename)
