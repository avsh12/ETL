import yaml
from pathlib import Path
from utils.constants import CONFIG_DIR


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
