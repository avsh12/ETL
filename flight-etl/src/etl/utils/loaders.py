import json
from pathlib import Path
from typing import cast

import yaml
from pandas import DataFrame, read_csv, read_parquet

from etl.utils.constants import CONFIG_DIR


def get_config_resource_path(file_name):
    config_path = CONFIG_DIR / f"{file_name}.yaml"
    if config_path.exists():
        return config_path
    else:
        raise FileNotFoundError(f"Missing {file_name} at {config_path}")


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_file(filepath: str | Path, columns: list):
    # gs://flight_details/flights.csv
    pass


def load_json(filepath: str | Path):
    with open(filepath, "r") as f:
        data = json.load(f)
    return data


def load_csv(filepath: str | Path, *args, **kwargs) -> DataFrame:
    df = read_csv(filepath, *args, **kwargs)
    return cast(DataFrame, df)


def load_parquet(filepath: str | Path, *args, **kwargs):
    return read_parquet(filepath, *args, **kwargs)


def write_csv(df: DataFrame, filepath: str | Path, *args, **kwargs):
    df.to_csv(filepath)


def write_parquet(df: DataFrame, filepath: str | Path, *args, **kwargs):
    df.to_parquet(filepath)


def write_json(data: dict, filepath: str | Path):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def read_bin(filepath: str | Path) -> bytes:
    with open(filepath, "rb") as f:
        file = f.read()

    return file


def write_bin(data: bytes, filepath: str | Path):
    with open(filepath, "wb") as f:
        f.write(data)
