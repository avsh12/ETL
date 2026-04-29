from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, Metadata, dag, task

from etl import airport
from utils.constants import airport_location_for_weather_filepath, gold_flight_filepath
from utils.helper import stamp

# create assets
gold_flight_asset = Asset(str(gold_flight_filepath))
airport_location_asset = Asset(str(airport_location_for_weather_filepath))

# The dag runs on Asset scheduling.
# After the upstream flight DAG writes the data to final gold storage, this DAG is triggered.
airport_etl_dag_args = {
    "dag_id": "airport_etl",
    "start_date": datetime(2026, 4, 26),
    # "end_date": datetime(2026, 4, 30),
    "catchup": True,
    "schedule": [gold_flight_asset],
    "max_active_runs": 2,
}


@dag(**airport_etl_dag_args)
def airport_etl():
    @task(outlets=[airport_location_asset])
    def airport_extract(triggering_asset_events):
        """
        triggering_asset_events[gold_flight_asset] returns a list of AssetEventDagRunReferenceResult
        objects for each asset accessed by the consumer dag.
        AssetEventDagRunReferenceResult is a subclass of AssetEventDagRunReference,
        which is a schema class consisiting of the following.

        asset: AssetReferenceAssetEventDagRun
        extra: dict[str, JsonValue]
        source_task_id: str | None
        source_dag_id: str | None
        source_run_id: str | None
        source_map_index: int | None
        source_aliases: list[AssetAliasReferenceAssetEventDagRun]
        timestamp: UtcDateTime
        """
        # -1 index to fetch the latest event
        extra = triggering_asset_events[gold_flight_asset][-1].extra

        TIMESTAMP = extra["TIMESTAMP"]

        airport.extract.extract(
            stamp(gold_flight_filepath, TIMESTAMP),
            stamp(airport_location_for_weather_filepath, TIMESTAMP),
        )

        yield Metadata(airport_location_asset, {"TIMESTAMP": TIMESTAMP})

    airport_extract()  # type: ignore


airport_etl_dag = airport_etl()
