"""Small, testable business transformations built with Spark expressions."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_orders(orders: DataFrame) -> DataFrame:
    """Parse, validate, and deterministically deduplicate raw orders.

    Invalid customer IDs, timestamps, and non-positive amounts are rejected rather
    than silently coerced, keeping downstream metrics trustworthy.
    """
    parsed = orders.withColumn("order_ts", F.to_timestamp("order_ts"))
    valid = parsed.filter(
        F.col("customer_id").isNotNull()
        & F.col("order_ts").isNotNull()
        & (F.col("amount").cast("double") > 0)
    ).withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    # Keep newest source version when duplicate order identifiers arrive.
    window = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc_nulls_last())
    return valid.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def customer_metrics(clean_orders_df: DataFrame, customers: DataFrame) -> DataFrame:
    """Return one row per customer, retaining customers with no valid orders."""
    metrics = clean_orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("lifetime_revenue"),
        F.max("order_ts").alias("last_order_ts"),
    )
    return customers.join(metrics, "customer_id", "left").fillna({"order_count": 0, "lifetime_revenue": 0})
