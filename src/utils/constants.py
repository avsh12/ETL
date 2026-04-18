from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
READER_MAP = {
    ".csv": "read_csv",
    ".parquet": "read_parquet",
    ".xls": "read_excel",
    ".xlsx": "read_excel",
}
