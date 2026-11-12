"""Minimal data-quality checks that fail loudly in a pipeline."""
from typing import Iterable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(ValueError):
    """Raised when a declared data contract is violated."""


def require_columns(df: DataFrame, columns: Iterable[str]) -> None:
    """Ensure all required column names exist before transforming data."""
    missing = set(columns) - set(df.columns)
    if missing:
        raise DataQualityError(f"Missing required columns: {sorted(missing)}")


def require_non_null(df: DataFrame, column: str) -> None:
    """Reject a DataFrame when a required field contains null values."""
    if df.filter(F.col(column).isNull()).limit(1).count():
        raise DataQualityError(f"Null values found in required column: {column}")


def duplicate_count(df: DataFrame, columns: list[str]) -> int:
    """Count surplus rows belonging to duplicated business keys."""
    return df.count() - df.dropDuplicates(columns).count()
