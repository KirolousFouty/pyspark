# Performance guide

1. Measure a representative workload and inspect `explain("formatted")`; laptop timings do not generalize.
2. Read only needed columns and filter early so column pruning and predicate pushdown can apply to Parquet.
3. Avoid Python UDFs when a Spark SQL built-in exists; Python serialization creates a JVM/Python boundary.
4. Avoid unnecessary `count`, `show`, `collect`, repeated actions, and repeated scans. Persist only reused expensive results and unpersist afterward.
5. Treat `groupBy`, `distinct`, joins, global ordering, and `repartition` as shuffle candidates.
6. Broadcast genuinely small lookup tables after validating size and memory. Do not broadcast large tables by habit.
7. Diagnose skew from partition sizes and long-running tasks. Salt a key or split skewed keys only after measurement.
8. Choose output partitions for downstream reads; avoid thousands of tiny files. AQE can coalesce shuffle partitions at runtime.
