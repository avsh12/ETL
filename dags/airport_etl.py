import os
from pathlib import Path

from airflow.decorators import dag, task  # type: ignore

gold_filepath = Path(str(os.getenv("GOLD_FLIGHT_FILEPATH")))
