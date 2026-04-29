from datetime import datetime

from airflow.sdk import Asset, Metadata, dag, task

from etl import flight

# import the static filepaths for the data
from utils.constants import (
    bronze_flight_filepath,
    gold_flight_filepath,
    silver_flight_filepath,
)
from utils.helper import stamp

# create the asset which the downstream dags would subscribe for scheduling.
gold_flight_asset = Asset(str(gold_flight_filepath))

flight_etl_args = {
    "dag_id": "flight_etl",
    "start_date": datetime(2026, 4, 25),
    # "end_date": datetime(2026, 4, 30),
    "catchup": True,
    "schedule": "@daily",
    "max_active_runs": 1,
}


@dag(**flight_etl_args)
def flight_etl():
    @task()
    def flight_clean(**context):
        TIMESTAMP = context["ds"]

        flight.clean.clean(
            stamp(bronze_flight_filepath, TIMESTAMP),
            stamp(silver_flight_filepath, TIMESTAMP),
        )

        return TIMESTAMP

    @task(outlets=[gold_flight_asset])
    def flight_transform(TIMESTAMP: str, **context):
        flight.transform.transform(
            stamp(silver_flight_filepath, TIMESTAMP),
            stamp(gold_flight_filepath, TIMESTAMP),
        )

        yield Metadata(gold_flight_asset, {"TIMESTAMP": TIMESTAMP})

    TIMESTAMP = flight_clean()
    flight_transform(TIMESTAMP)  # type:ignore


flight_etl_dag = flight_etl()
