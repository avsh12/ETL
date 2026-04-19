import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Asset
from dotenv import load_dotenv

flight_etl_args = {
    "dag_id": "flight_etl",
    "start_date": datetime(2026, 4, 18),
    "end_date": datetime(2026, 4, 19),
    "catchup": True,
    "schedule": "@daily",
    "max_active_runs": 1,
}

# load environment variables
load_dotenv()

bronze_flight_path = Path(str(os.getenv("BRONZE_FLIGHT_PATH")))
silver_flight_path = Path(str(os.getenv("SILVER_FLIGHT_PATH")))
gold_flight_path = Path(str(os.getenv("GOLD_FLIGHT_PATH")))
discarded_flight_path = Path(str(os.getenv("DISCARDED_FLIGHT_PATH")))

# unique timestamp for files
TIMESTAMP = str(datetime.today().date())

bronze_flight_filepath = bronze_flight_path / "flight_details" / ".csv"
silver_flight_filepath = (
    silver_flight_path / "silver_flight_details" / TIMESTAMP / ".parquet"
)
gold_flight_filepath = gold_flight_path / "gold_flight_details" / TIMESTAMP / ".parquet"
discarded_flight_filepath = (
    discarded_flight_path / "discarded_flight_details" / TIMESTAMP / ".parquet"
)


@task()
def clean(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@task(outlets=[Asset(str(gold_flight_filepath))])
def transform(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@dag(**flight_etl_args)
def flight_etl():
    clean_task = clean(bronze_flight_filepath, silver_flight_filepath)
    transform_task = transform(silver_flight_filepath, gold_flight_filepath)

    clean_task >> transform_task  # type: ignore


flight_etl_dag = flight_etl()
