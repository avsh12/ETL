import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, dag, task
from dotenv import load_dotenv

load_dotenv()

TIMESTAMP = str(datetime.today().date())

gold_flight_path = Path(str(os.getenv("GOLD_FLIGHT_PATH")))
silver_weather_path = Path(str(os.getenv("SILVER_WEATHER_PATH")))
gold_flight_weather_path = Path(str(os.getenv("GOLD_FLIGHT_WEATHER_PATH")))
transformed_flight_weather_path = Path(
    str(os.getenv("TRANSFORMED_FLIGHT_WEATHER_PATH"))
)

gold_flight_filepath = gold_flight_path / "gold_flight_details" / TIMESTAMP / ".parquet"
silver_weather_filepath = (
    silver_weather_path / "silver_weather_" / TIMESTAMP / ".parquet"
)
gold_flight_weather_filepath = (
    gold_flight_weather_path / "gold_flight_weather_" / TIMESTAMP / ".parquet"
)
transformed_flight_weather_filepath = (
    transformed_flight_weather_path
    / "transformed_flight_weather_TIMESTAMP"
    / ".parquet"
)


merger_dag_args = {
    "dag_id": "flight_weather_merger_dag",
    "start_date": datetime(2026, 4, 18),
    "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": [Asset(str(silver_weather_filepath))],
    "max_active_runs": 1,
}


@task()
def combine(
    gold_flight_filepath: str | Path,
    silver_weather_filepath: str | Path,
    gold_flight_weather_filepath: str | Path,
):
    return gold_flight_weather_filepath


@task()
def transform(
    gold_flight_weather_filepath: str | Path,
    transformed_flight_weather_filepath: str | Path,
):
    return transformed_flight_weather_filepath


@dag(**merger_dag_args)
def merger_dag():
    combine_task = combine(
        gold_flight_filepath=gold_flight_filepath,
        silver_weather_filepath=silver_weather_filepath,
        gold_flight_weather_filepath=gold_flight_weather_filepath,
    )
    transform_task = transform(
        gold_flight_weather_filepath=gold_flight_weather_filepath,
        transformed_flight_weather_filepath=transformed_flight_weather_filepath,
    )

    combine_task >> transform_task


merger_etl_dag = merger_dag()
