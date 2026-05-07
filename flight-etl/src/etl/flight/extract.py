from pathlib import Path

import pandas as pd
from utils.constants import DATA_DIR, READER_MAP
from utils.loaders import get_config_resource_path, load_yaml
from utils.logger import log_progress
from etl.flight.schema_validation import schema_validaton


def load_flight_data(
    filepath: str | Path, columns: list | None = None, dtype=None
) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()

    if ext not in READER_MAP:
        raise ValueError(
            f"Unsupported file type: {ext}. Supported file types: {list(READER_MAP.keys())}"
        )

    file_reader = getattr(pd, READER_MAP[ext])

    kwargs = {}
    if ext == ".csv":
        kwargs["usecols"] = columns
        kwargs["dtype"] = dtype
        kwargs["low_memory"] = False
    elif ext == ".parquet":
        kwargs["columns"] = columns

    try:
        df = file_reader(filepath, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to load {filepath}: {e}")

    return df


def extract(
    filepath: str | Path | None = None,
    schema: dict | None = None,
    columns: list | None = None,
):
    log_progress("Extracting data")

    if filepath is None:
        config_path = get_config_resource_path("config")
        config = load_yaml(config_path)
        filepath = (DATA_DIR / config["data"]["raw_path"]).resolve()

    if schema is None:
        schema_path = get_config_resource_path("schema")
        schema = load_yaml(schema_path)

    if columns is None:
        config_path = get_config_resource_path("config")
        config = load_yaml(config_path)
        columns = config["pipeline"]["load_columns"]

    flight_data = load_flight_data(
        filepath=filepath,
        columns=columns,
        dtype=schema["bronze_schema"],
    )
    log_progress("Data Extraction Done!\n")

    schema_validaton(flight_data, schema["bronze_schema"])

    return flight_data
