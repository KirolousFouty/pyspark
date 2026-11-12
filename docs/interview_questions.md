# PySpark interview questions

**What is lazy evaluation?** Transformations create a lineage/DAG. Spark selects and executes a physical plan only when an action needs a result.

**Narrow vs. wide transformation?** A narrow transformation reads parent data from corresponding partitions. A wide transformation needs data redistributed across partitions and normally creates a shuffle boundary.

**`repartition` vs `coalesce`?** `repartition` can increase or decrease partitions and fully redistributes data. `coalesce` usually reduces partitions cheaply, but may create uneven output.

**Why are Python UDFs slower?** Rows cross between the JVM execution engine and Python, limiting Catalyst visibility and adding serialization overhead. Prefer native expressions.

**How do you address skew?** First prove it via task/partition metrics. Then broadcast a small side, filter early, salt a hot key, or process hot keys separately.

**Why can a left join increase rows?** Every matching row on the right is emitted; a non-unique right key multiplies left rows.
