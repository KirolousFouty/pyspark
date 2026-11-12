"""A small, local batch ETL pipeline with explicit boundaries and contracts."""
import argparse
import logging
from pathlib import Path
from pyspark.sql import functions as F
from pyspark_learning.data_quality import require_columns
from pyspark_learning.spark_session import create_spark_session
from pyspark_learning.transformations import clean_orders, customer_metrics

LOGGER = logging.getLogger(__name__)


def run(input_path: str, output_path: str) -> None:
    """Ingest raw CSV, validate, clean, derive metrics, and write partitioned Parquet."""
    spark = create_spark_session("E-commerce ETL")
    source, destination = Path(input_path), Path(output_path)
    try:
        orders = spark.read.option("header", True).csv(str(source / "orders.csv"))
        customers = spark.read.option("header", True).csv(str(source / "customers.csv"))
        require_columns(orders, ["order_id", "customer_id", "order_ts", "amount", "ingested_at"])
        require_columns(customers, ["customer_id", "name", "country"])
        cleaned = clean_orders(orders)
        metrics = customer_metrics(cleaned, customers)
        destination.mkdir(parents=True, exist_ok=True)
        cleaned.withColumn("order_date", F.to_date("order_ts")).write.mode("overwrite").partitionBy("order_date").parquet(str(destination / "clean_orders"))
        metrics.write.mode("overwrite").parquet(str(destination / "customer_metrics"))
        LOGGER.info("Wrote ETL outputs to %s", destination)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="data/processed")
    args = parser.parse_args()
    run(args.input, args.output)
