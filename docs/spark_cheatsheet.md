# PySpark cheat sheet

| Need | DataFrame API | SQL |
| --- | --- | --- |
| Filter | `df.filter(F.col("amount") > 0)` | `WHERE amount > 0` |
| Select | `df.select("id", "amount")` | `SELECT id, amount` |
| Derive | `withColumn("x", expr)` | `SELECT ..., expr AS x` |
| Aggregate | `groupBy("country").agg(F.sum("amount"))` | `GROUP BY country` |
| Join | `left.join(right, "id", "left")` | `LEFT JOIN ... USING (id)` |
| Window | `F.row_number().over(window)` | `ROW_NUMBER() OVER (...)` |

`show()` is an action intended for small inspection. `collect()` moves every row to the driver; prefer `limit`, `take`, writes, or aggregations for large data. `repartition(n, key)` causes a shuffle; `coalesce(n)` reduces partitions without a full reshuffle. Always use `explain("formatted")` before guessing at performance.
