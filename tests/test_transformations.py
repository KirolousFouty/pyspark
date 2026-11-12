from pyspark_learning.transformations import clean_orders, customer_metrics


def test_clean_orders_rejects_invalid_and_keeps_latest_duplicate(spark):
    raw = spark.createDataFrame([
        ("o1", "c1", "2025-01-01T10:00:00", "10.0", "2025-01-01T11:00:00"),
        ("o1", "c1", "2025-01-01T10:00:00", "12.0", "2025-01-02T11:00:00"),
        ("o2", None, "2025-01-01T10:00:00", "5", "2025-01-01T11:00:00"),
        ("o3", "c1", "bad", "5", "2025-01-01T11:00:00"),
    ], ["order_id", "customer_id", "order_ts", "amount", "ingested_at"])
    actual = clean_orders(raw).select("order_id", "amount").collect()
    assert [(row.order_id, str(row.amount)) for row in actual] == [("o1", "12.00")]


def test_customer_metrics_keeps_zero_order_customers(spark):
    customers = spark.createDataFrame([("c1", "Ada"), ("c2", "Lin")], ["customer_id", "name"])
    orders = spark.createDataFrame(
        [("o1", "c1", "2025-01-01 00:00:00", 4.0)],
        ["order_id", "customer_id", "order_ts", "amount"],
    )
    results = {r.customer_id: (r.order_count, r.lifetime_revenue) for r in customer_metrics(orders, customers).collect()}
    assert float(results["c1"][1]) == 4.0
    assert results["c1"][0] == 1
    assert float(results["c2"][1]) == 0.0
    assert results["c2"][0] == 0
