from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, Metadata, dag, task

from etl.adapters import openmeteo
from utils.constants import (
    airport_location_filepath,
    airport_location_for_weather_filepath,
    bronze_weather_filepath,
    silver_weather_filepath,
)
from utils.helper import stamp
from utils.logger import log_progress

airport_location_asset = Asset(str(airport_location_for_weather_filepath))
weather_asset = Asset(str(silver_weather_filepath))

weather_etl_args = {
    "dag_id": "weather_etl",
    "start_date": datetime(2026, 4, 18),
    # "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [airport_location_asset],
    "max_active_runs": 1,
}


@dag(**weather_etl_args)
def weather_etl():
    @task()
    def weather_extract(triggering_asset_events):
        extra = triggering_asset_events[airport_location_asset][-1].extra
        TIMESTAMP = extra["TIMESTAMP"]

        # Remove the condition
        resource_exists = Path(stamp(bronze_weather_filepath, TIMESTAMP)).exists()
        log_progress(
            f"Resource status at {stamp(bronze_weather_filepath, TIMESTAMP)}: {Path(stamp(bronze_weather_filepath, TIMESTAMP)).exists()}"
        )
        if not resource_exists:
            openmeteo.extract(
                stamp(airport_location_for_weather_filepath, TIMESTAMP),
                airport_location_filepath,
                stamp(bronze_weather_filepath, TIMESTAMP),
            )

        return TIMESTAMP

    @task(outlets=[weather_asset])
    def weather_transform(TIMESTAMP: str):

        openmeteo.weather_bin_to_parquet(
            stamp(bronze_weather_filepath, TIMESTAMP),
            stamp(silver_weather_filepath, TIMESTAMP),
            stamp(airport_location_for_weather_filepath, TIMESTAMP),
        )

        yield Metadata(weather_asset, {"TIMESTAMP": TIMESTAMP})

    TIMESTAMP = weather_extract()  # type:ignore
    weather_transform(TIMESTAMP=TIMESTAMP)


weather_etl_dag = weather_etl()
