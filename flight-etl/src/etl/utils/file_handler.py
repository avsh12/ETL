import json
import logging
import os
from typing import cast

import fsspec
import yaml
from pandas import DataFrame, read_csv, read_parquet
from upath import UPath as Path

logger = logging.getLogger(__name__)


def get_file_size(filepath: str, format: str = "bytes"):
    size = Path(filepath).stat().st_size
    if format in ["kb", "kilobytes"]:
        size /= 1024
    if format in ["mb", "megabytes"]:
        size /= 1024 * 1024
    return size


def loader(func):
    def error_wrapper(filepath, *args, **kwargs):
        try:
            logger.info(f"Reading the file {filepath}.")
            content = func(filepath, *args, **kwargs)
            logger.debug(f"File read successfully: {filepath}.")
            return content
        except Exception as e:
            logger.error(f"Error occured while loading the file {filepath}: {e}")
            raise

    return error_wrapper


def dumper(func):
    def error_wrapper(filepath, *args, **kwargs):
        try:
            logger.info(f"Writing to the file {filepath}")
            func(filepath, *args, **kwargs)
            logger.debug(f"File written successfully: {filepath}.")
        except Exception as e:
            logger.error(f"Error occured during the file write {filepath}: {e}")
            raise

    return error_wrapper


@loader
def load_yaml(filepath):
    logger.info(f"File size: {get_file_size(filepath, format='kb')} KB")
    with fsspec.open(filepath, "r") as f:
        return yaml.safe_load(f)


@loader
def load_json(filepath: str):
    logger.info(f"File size: {get_file_size(filepath, format='kb')} KB")
    with fsspec.open(filepath, "r") as f:
        data = json.load(f)
    return data


@dumper
def write_json(filepath: str, data: dict):
    with fsspec.open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


@loader
def load_csv(filepath: str, *args, **kwargs) -> DataFrame:
    logger.info(f"File size: {get_file_size(filepath, format='mb')} MB")
    df = read_csv(filepath, *args, **kwargs)
    return cast(DataFrame, df)


@dumper
def write_csv(filepath: str, df: DataFrame, *args, **kwargs):
    df.to_csv(filepath)


@loader
def load_parquet(filepath: str, *args, **kwargs):
    logger.info(f"File size: {get_file_size(filepath, format='mb')} MB")
    return read_parquet(filepath, *args, **kwargs)


@dumper
def write_parquet(filepath: str, df: DataFrame, *args, **kwargs):
    df.to_parquet(filepath)


@loader
def read_bin(filepath: str) -> bytes:
    logger.info(f"File size: {get_file_size(filepath, format='mb')} MB")
    with fsspec.open(filepath, "rb") as f:
        file = f.read()

    return file


@dumper
def write_bin(filepath: str, data: bytes):
    with fsspec.open(filepath, "wb") as f:
        f.write(data)
