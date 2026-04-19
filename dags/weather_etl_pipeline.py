import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Asset
from dotenv import load_dotenv

load_dotenv()

TIMESTAMP = str(datetime.today().date())

bronze_weather_path = Path(str(os.getenv("BRONZE_WEATHER_PATH")))
silver_weather_path = Path(str(os.getenv("SILVER_WEATHER_PATH")))
airport_locations_path = Path(str(os.getenv("AIRPORT_LOCATIONS_PATH")))
airport_locations_filename = Path(str(os.getenv("AIRPORT_LOCATIONS_FILENAME")))
airport_location_for_weather_path = Path(
    str(os.getenv("AIRPORT_LOCATION_FOR_WEATHER_PATH"))
)

bronze_weather_filepath = bronze_weather_path / "weather_" / TIMESTAMP / ".json"
silver_weather_filepath = (
    silver_weather_path / "silver_weather_" / TIMESTAMP / ".parquet"
)
airport_locations_filepath = airport_locations_path / airport_locations_filename
airport_location_for_weather_filepath = (
    airport_location_for_weather_path
    / "airports_for_weather_details_"
    / TIMESTAMP
    / ".json"
)


weather_etl_args = {
    "dag_id": "weather_etl",
    "start_date": datetime(2026, 4, 18),
    "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [Asset(str(airport_location_for_weather_filepath))],
    "max_active_runs": 1,
}


@task()
def extract(
    airport_locations_for_weather_filepath: str | Path,
    airport_locations_filepath: str | Path,
    write_filepath: str | Path,
):
    return write_filepath


@task(outlets=[Asset(str(silver_weather_filepath))])
def transform(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@dag(**weather_etl_args)
def weather_etl():
    extract_task = extract(
        airport_locations_for_weather_filepath=airport_location_for_weather_filepath,
        airport_locations_filepath=airport_locations_filepath,
        write_filepath=bronze_weather_filepath,
    )

    transform_task = transform(
        read_filepath=bronze_weather_filepath, write_filepath=silver_weather_filepath
    )

    extract_task >> transform_task


weather_etl_dag = weather_etl()
