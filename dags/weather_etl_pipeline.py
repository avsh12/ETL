import os
from pathlib import Path
from dotenv import load_dotenv

from airflow.operators import dag, task
from datetime import datetime

load_dotenv()

bronze_filepath = Path(str(os.getenv("BRONZE_WEATHER_FILEPATH")))
silver_filepath = Path(str(os.getenv("SILVER_WEATHER_FILEPATH")))

dag_id = "weather_etl"
start_date = datetime(2026, 4, 17)
end_date = datetime(2026, 4, 18)
catchup = "@daily"
max_axtive_runs = 1


@task
def extract(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@task
def transform(read_filepath: str | Path, write_filepath: str | Path):
    return write_filepath


@dag(
    dag_id=dag_id,
    start_date=start_date,
    end_date=end_date,
    catchup=catchup,
    max_axtive_runs=max_axtive_runs,
)
def weather_etl():
    extract_task = extract()
