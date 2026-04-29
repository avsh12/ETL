from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, dag, task

from etl.merge.join import join_flight_weather
from utils.constants import (
    feature_store_filepath,
    gold_flight_filepath,
    gold_merged_filepath,
    silver_weather_filepath,
)
from utils.helper import stamp

flight_asset = Asset(str(gold_flight_filepath))
weather_asset = Asset(str(silver_weather_filepath))


merger_dag_args = {
    "dag_id": "flight_weather_merger_dag",
    "start_date": datetime(2026, 4, 18),
    # "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [flight_asset, weather_asset],
    "max_active_runs": 1,
}


@dag(**merger_dag_args)
def merger_dag():
    @task()
    def combine(triggering_asset_events):
        extra = triggering_asset_events[weather_asset][-1].extra
        TIMESTAMP = extra["TIMESTAMP"]

        join_flight_weather(
            stamp(gold_flight_filepath, TIMESTAMP),
            stamp(silver_weather_filepath, TIMESTAMP),
            stamp(gold_merged_filepath, TIMESTAMP),
        )

        return TIMESTAMP

    @task()
    def transform(TIMESTAMP):
        return TIMESTAMP

    TIMESTAMP = combine()  # type: ignore
    transform(TIMESTAMP=TIMESTAMP)


merger_etl_dag = merger_dag()
