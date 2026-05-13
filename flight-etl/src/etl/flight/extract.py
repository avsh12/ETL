import pandas as pd
from upath import UPath as Path

from etl.flight.schema_validation import schema_validaton
from etl.utils.constants import CONFIG_FILEPATH, DATA_DIR, READER_MAP, SCHEMA_FILEPATH
from etl.utils.file_handler import load_yaml
from etl.utils.logger import log_progress


def load_flight_data(filepath: str, columns: list | None = None, dtype=None) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()

    if ext not in READER_MAP:
        raise ValueError(f"Unsupported file type: {ext}. Supported file types: {list(READER_MAP.keys())}")

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
    filepath: str | None = None,
    schema: dict | None = None,
    columns: list | None = None,
):
    log_progress("Extracting data")

    if filepath is None:
        config = load_yaml(CONFIG_FILEPATH)
        filepath = (DATA_DIR / config["data"]["raw_path"]).resolve()

    if schema is None:
        schema = load_yaml(SCHEMA_FILEPATH)

    if columns is None:
        config = load_yaml(CONFIG_FILEPATH)
        columns = config["pipeline"]["load_columns"]

    flight_data = load_flight_data(
        filepath=filepath,
        columns=columns,
        dtype=schema["bronze_schema"],
    )
    log_progress("Data Extraction Done!\n")

    schema_validaton(flight_data, schema["bronze_schema"])

    return flight_data
