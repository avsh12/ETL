import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, dag, task

TIMESTAMP = str(datetime.today().date())

gold_flight_path = Path(str(os.getenv("GOLD_FLIGHT_PATH")))
airport_location_for_weather_path = Path(
    str(os.getenv("AIRPORT_LOCATIONS_FOR_WEATHER_PATH"))
)

gold_flight_filepath = gold_flight_path / "flight_details" / TIMESTAMP / ".parquet"
airport_location_for_weather_filepath = (
    airport_location_for_weather_path
    / "airports_for_weather_details_"
    / TIMESTAMP
    / ".json"
)

# The dag runs on Asset scheduling.
# After the upstream flight DAG writes the data to final gold storage, this DAG is triggered.
airport_etl_dag_args = {
    "dag_id": "airport_etl",
    "start_date": datetime(2026, 4, 18),
    "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [Asset(str(gold_flight_filepath))],
    "max_active_runs": 1,
}

extract_args = {
    "flight_filepath": gold_flight_filepath,
    "airport_location_for_weather_filepath": airport_location_for_weather_filepath,
}


@task(outlets=[Asset(str(airport_location_for_weather_filepath))])
def extract(
    flight_filepath: str | Path, airport_location_for_weather_filepath: str | Path
):
    return airport_location_for_weather_filepath


@dag(**airport_etl_dag_args)
def airport_etl():
    extract(**extract_args)


airport_etl_dag = airport_etl()
