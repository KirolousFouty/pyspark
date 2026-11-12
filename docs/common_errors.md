# Common PySpark errors

| Symptom | Cause and response |
| --- | --- |
| `AnalysisException: cannot resolve` | A column name is misspelled or dropped; check `printSchema()` and aliases. |
| Ambiguous reference after join | Both inputs expose a name; alias inputs and select qualified columns. |
| Row count explodes after join | One or both join keys are non-unique; profile keys before joining. |
| Nulls vanish in joins | `NULL = NULL` is not true in ordinary SQL joins; use null-safe equality only when business logic requires it. |
| Driver OOM | `collect()`/large `toPandas()` brought distributed data to one process. Aggregate or write instead. |
| “Why did this run now?” | An action materializes a lazy lineage; transformations only describe work. |
| Unexpected shuffle | A wide operation needs rows with equal keys together; inspect the physical plan. |
