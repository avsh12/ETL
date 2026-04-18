import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task  # type: ignore
from dotenv import load_dotenv

dag_id = "flight_etl"
start_date = datetime(2026, 4, 17)
end_date = datetime(2026, 4, 18)
catch_up = "@daily"
max_active_runs = 1

load_dotenv()

bronze_filepath = Path(str(os.getenv("BRONZE_FLIGHT_FILEPATH")))
silver_filepath = Path(str(os.getenv("SILVER_FLIGHT_FILEPATH")))
gold_filepath = Path(str(os.getenv("GOLD_FLIGHT_FILEPATH")))


@task
def clean(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@task
def transform(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@dag(
    dag_id=dag_id,
    start_date=start_date,
    end_date=end_date,
    catch_up=catch_up,
    max_active_runs=max_active_runs,
)
def flight_etl():
    clean_task = clean(bronze_filepath, silver_filepath)
    transform_task = transform(silver_filepath, gold_filepath)

    clean_task >> transform_task  # type: ignore


flight_etl_dag = flight_etl()
