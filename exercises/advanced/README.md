# Advanced challenges

1. Write clean orders as date-partitioned Parquet and prove a date filter appears in `explain("formatted")`.
2. Construct an intentionally skewed key, inspect partition-level counts, and explain one mitigation.
3. Add a business-rule quality gate that rejects negative item quantities and test it with pytest.
4. Implement a local file-stream source with a checkpoint directory; explain output modes and watermark trade-offs.
5. Profile the final ETL's joins and propose an optimization only after observing its plan.
