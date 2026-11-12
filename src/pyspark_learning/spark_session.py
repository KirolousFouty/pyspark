"""Consistent, laptop-friendly Spark session construction."""
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "PySpark Learning Lab", shuffle_partitions: int = 4) -> SparkSession:
    """Create a local Spark session with safe, visible learning defaults.

    Small shuffle partition counts make examples readable; production values must be
    sized from data volume and cluster resources rather than copied from this lab.
    """
    return (
        SparkSession.builder.master("local[*]").appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
