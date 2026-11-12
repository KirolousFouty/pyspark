# PySpark Learning Lab

A local, hands-on course for learning PySpark from first `SparkSession` through a tested e-commerce ETL pipeline. The course uses the same small e-commerce model throughout, so new Spark concepts build on familiar data.

## Outcomes

You will be able to reason about Spark's driver/executors, lazy plans, shuffles and partitions; use DataFrames and SQL confidently; inspect execution plans; test transformations; and build a local, production-shaped batch pipeline. RDDs appear only where they clarify Spark's foundations.

## Prerequisites and installation

Python 3.9–3.12, Java 17 or 21 (recommended), and 4 GB free memory. Spark 3.5.5 is pinned because it is a stable PySpark 3.5 release. Java 25 is newer than Spark's commonly tested JDKs; use Java 17/21 if you encounter JVM errors.

```bash
python3 -m venv .venv
source .venv/bin/activate              # Windows: .venv\\Scripts\\activate
pip install -e '.[dev]'
python data/generators/generate_data.py
jupyter lab
```

Open notebooks in numerical order. Each is executable in local mode and writes only below `data/`. If Java is not detected, set `JAVA_HOME` to your JDK directory before starting Jupyter.

```bash
pytest -q
```

## Study order and plan

| Week | Milestone |
| --- | --- |
| 1 | 00–05: local Spark, schemas, transformations, column expressions |
| 2 | 06–12: aggregations, joins, windows, SQL, cleaning and storage |
| 3 | 13–16: partitions, plans, performance, UDF trade-offs |
| 4 | 17–20: streaming, quality tests, ETL and capstone |
| 5–6 | Repeat exercises unaided; vary generator sizes and inspect plans |

## Data model

`customers 1—* orders 1—* order_items *—1 products`; payments attach to orders and click events attach to customers. The generator deliberately emits duplicate orders, null customer IDs, malformed timestamps, null prices, invalid quantities, late events, and a high-activity customer. Defaults are laptop-friendly and reproducible (`--seed 42`).

## Important local-mode notes

Local mode preserves Spark's planning and partition semantics but does not emulate a multi-node cluster. Timings are demonstrations, not universal benchmarks: data size, disk, RAM, cores, configuration, and skew change results. Spark UI is normally at `http://localhost:4040` while a context is active.

## Useful commands

```bash
python data/generators/generate_data.py --customers 500 --orders 3000
python -m pyspark_learning.pipelines.ecommerce_etl --input data/raw --output data/processed
pytest -q
```

See [docs/spark_cheatsheet.md](docs/spark_cheatsheet.md), [docs/performance_guide.md](docs/performance_guide.md), [docs/common_errors.md](docs/common_errors.md), and [docs/interview_questions.md](docs/interview_questions.md).
