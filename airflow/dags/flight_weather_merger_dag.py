from datetime import datetime

from airflow.sdk import Asset, dag, task
from etl import merge
from etl.core.constants import (
    feature_store_filepath,
    gold_flight_filepath,
    silver_weather_filepath,
)
from etl.utils.helper import stamp
from upath import UPath as Path

flight_asset = Asset(str(gold_flight_filepath))
weather_asset = Asset(str(silver_weather_filepath))


merger_dag_args = {
    "dag_id": "flight_weather_merger_dag",
    "start_date": datetime(2026, 5, 13),
    # "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [flight_asset, weather_asset],
    "max_active_runs": 1,
}


@dag(**merger_dag_args)
def merger_dag():
    @task()
    def merger_task(triggering_asset_events):
        extra = triggering_asset_events[weather_asset][-1].extra
        TIMESTAMP = extra["TIMESTAMP"]

        flight_weather_merger = merge.transform.FlightWeatherTransform(
            stamp(gold_flight_filepath, TIMESTAMP),
            stamp(silver_weather_filepath, TIMESTAMP),
        )
        flight_weather_merger.execute(stamp(feature_store_filepath, TIMESTAMP))

        return TIMESTAMP

    TIMESTAMP = merger_task()  # type: ignore


merger_etl_dag = merger_dag()
