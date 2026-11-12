#!/usr/bin/env python3
"""Build validated, readable course notebooks from the maintained lesson outline.

The source stays intentionally compact so lesson structure can evolve consistently;
generated notebooks are ordinary Jupyter JSON and are committed course materials.
"""
import json
from pathlib import Path

LESSONS = [
    ("00_getting_started", "Getting Started", "Spark is a distributed compute engine that lets one program describe work over data too large or slow for a single-process workflow.", "Create a local SparkSession, inspect a small DataFrame, and notice that `show`, `count`, and `explain` trigger or describe execution.", "Compare `df.filter('amount > 20').count()` with `df.filter('amount > 20').explain()`. Which line asks Spark for a result?"),
    ("01_spark_architecture", "Spark Architecture", "The driver plans work; executors run tasks over partitions. A job is created by an action, stages are separated by shuffles, and tasks operate on partition-sized slices.", "Use `getNumPartitions`, an RDD `mapPartitionsWithIndex`, and `explain` to connect DataFrame code to partitions and a DAG.", "Change the partition count below. Why can more partitions improve parallelism but also add scheduling overhead?"),
    ("02_dataframe_basics", "DataFrame Basics", "A DataFrame is a distributed table with named, typed columns and a lazily composed query plan.", "Create DataFrames from Python records and files, select a projection, and compare immutable transformations with actions.", "Predict the schema and rows after selecting `customer_id` and `amount * 1.1`."),
    ("03_schemas_and_data_types", "Schemas and Data Types", "Schemas are data contracts: they tell Spark how to parse, optimize, and safely interpret values.", "Define an explicit StructType and contrast it with inference; inspect strings, numbers, dates, arrays, and structs.", "Make `amount` a DecimalType and explain why money should not be a floating-point business key."),
    ("04_transformations_and_actions", "Transformations and Actions", "Transformations describe a new dataset without executing it; actions materialize a lineage into a job.", "Chain filters and projections, then use `explain` before triggering `count`.", "Identify the action in a two-line pipeline and predict how many jobs it starts."),
    ("05_filtering_and_columns", "Filtering and Column Expressions", "Column expressions build a distributed expression tree. Native functions stay visible to Catalyst and are usually faster than Python row logic.", "Use `filter`, `when`, `otherwise`, `withColumn`, casts, and null-safe predicates.", "Classify orders into small/large without a Python UDF; then test what null amounts do."),
    ("06_aggregations", "Aggregations", "Grouping brings equal keys together, so it often shuffles data. Aggregation collapses many rows into one row per key.", "Compare grouped revenue and global totals; inspect why `groupBy` differs from a window calculation.", "Find revenue per country and explain which columns must cross the network in a distributed cluster."),
    ("07_joins", "Joins", "Joins combine related tables by key. Non-unique keys multiply rows, and distributed joins may shuffle both sides unless a small table can be broadcast.", "Run inner, left, semi, anti, and broadcast joins; profile keys before trusting output counts.", "Create a duplicate product key and predict the changed order-item row count."),
    ("08_window_functions", "Window Functions", "Windows calculate across related rows while preserving each input row, unlike groupBy which collapses rows.", "Use partitionBy/orderBy with row_number, rank, lag, running sums, and moving averages.", "Return the latest order per customer and compare it to a groupBy-max solution."),
    ("09_spark_sql", "Spark SQL", "DataFrame expressions and Spark SQL share the Catalyst optimizer and underlying execution engine; choose the form your team can read best.", "Create a temporary view, run a CTE and a window query, then reproduce one query with DataFrames.", "Translate the revenue-by-country calculation into SQL and explain why both forms can have the same plan."),
    ("10_missing_and_dirty_data", "Missing and Dirty Data", "Cleaning is a business decision expressed as explicit, testable rules—not a blanket `dropna` call.", "Profile nulls, parse types safely, reject invalid records, and deduplicate using a latest-ingestion rule.", "Decide whether a missing customer should be quarantined or imputed; state the business consequence."),
    ("11_dates_strings_and_complex_types", "Dates, Strings, and Complex Types", "Built-in functions model common parsing and nested-data operations inside Spark's optimizer.", "Parse timestamps, extract dates, clean strings, and inspect JSON arrays, structs, and maps.", "Explode an array of tags, then explain why the output has more rows."),
    ("12_reading_and_writing_data", "Reading and Writing Data", "File format and layout affect both correctness and cost. Parquet stores typed, columnar data that Spark can prune and push filters into.", "Read CSV/JSON with schemas and write/read Parquet partitioned by date.", "Compare CSV and Parquet schemas and list one reason not to partition by a high-cardinality ID."),
    ("13_partitioning_and_shuffling", "Partitioning and Shuffling", "Partitions define task-sized chunks. A shuffle redistributes rows by key and is expensive because it exchanges and materializes data.", "Inspect partitions, use repartition versus coalesce, and identify wide operations in a plan.", "Predict whether `repartition(4, 'customer_id')` and `coalesce(2)` require a full shuffle."),
    ("14_performance_optimization", "Performance Optimization", "Optimization begins with a measured bottleneck and execution plan, not folklore. Read less, shuffle less, and reuse only expensive intermediate data.", "Compare native expressions with a UDF-shaped alternative, cache a reused result, and broadcast a lookup table.", "Change a filter position and inspect whether column pruning/predicate pushdown is possible with Parquet."),
    ("15_execution_plans", "Execution Plans", "Logical plans describe what a query means; physical plans describe how Spark will execute it. Catalyst rewrites plans and AQE can adjust at runtime.", "Read parsed, analyzed, optimized, and physical plan sections using `explain('formatted')`.", "Locate an Exchange in a join plan and explain what it says about data movement."),
    ("16_udfs_and_advanced_transformations", "UDFs and Advanced Transformations", "Python UDFs solve gaps in native expressions but add JVM/Python serialization and hide logic from many optimizer rules.", "Compare a built-in string expression with a scalar UDF; learn when Pandas UDFs/Arrow may help.", "Replace the provided UDF with native Spark functions and compare the formatted plans."),
    ("17_structured_streaming", "Structured Streaming", "Structured Streaming applies DataFrame-style plans to incrementally arriving data, normally as micro-batches with checkpointed progress.", "Read a local JSON directory stream, use append output, checkpoint it, and explain watermarks/stateful aggregates.", "What failure does a checkpoint address, and why must its path be durable in production?"),
    ("18_data_quality_and_testing", "Data Quality and Testing", "Pipelines need executable contracts: schema expectations, null constraints, uniqueness, business rules, and deterministic unit tests.", "Use reusable quality checks and pytest Spark fixtures; compare small DataFrames without relying on row order.", "Write a test that proves a duplicate order keeps the newest ingestion timestamp."),
    ("19_real_world_etl", "Real-World ETL", "A production-shaped ETL separates orchestration from reusable transformations and makes validation, logging, output layout, and tests explicit.", "Run the package ETL over generated raw files and inspect clean orders plus customer metrics.", "Add a metric to the transformation module first, then test it before adding it to pipeline output."),
    ("20_final_capstone", "Final Capstone: E-commerce Analytics", "The capstone combines ingestion, schemas, cleaning, deduplication, joins, windows, metrics, quality gates, partitioned Parquet, and performance analysis.", "Build a customer analytics mart from the deliberately flawed e-commerce sources and defend each business/data decision.", "Explain how you would handle late events and a skewed high-activity customer in a real cluster."),
]

SETUP = """from pathlib import Path
import sys
import os
# Make the local src-layout package importable in VS Code/Jupyter without installation.
project_root = Path.cwd().resolve().parent if Path.cwd().name == "notebooks" else Path.cwd().resolve()
sys.path.insert(0, str(project_root / "src"))
# Spark 3.5 is compatible with the installed Java 17 runtime; Java 25 is not.
java17_home = Path("/opt/miniconda3/pkgs/openjdk-17.0.18-h258754b_0/lib/jvm")
if java17_home.exists():
    os.environ["JAVA_HOME"] = str(java17_home)
    os.environ["PATH"] = f"{{java17_home / 'bin'}}:{{os.environ['PATH']}}"
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark_learning.spark_session import create_spark_session

spark = create_spark_session(\"Learning Lab: {title}\")
raw = Path.cwd().parent / \"data\" / \"raw\"
if not raw.exists():
    raw = Path(\"data/raw\")
orders = spark.read.option(\"header\", True).csv(str(raw / \"orders.csv\"))
customers = spark.read.option(\"header\", True).csv(str(raw / \"customers.csv\"))
orders.show(5, truncate=False)
orders.printSchema()"""

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": [line + "\n" for line in text.strip().splitlines()]}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": [line + "\n" for line in text.strip().splitlines()]}

def notebook(slug, title, what, lesson, experiment):
    common_example = """# Native expressions form one optimized plan; no large data is collected to Python.
typed_orders = (orders
    .withColumn(\"amount_num\", F.col(\"amount\").cast(\"double\"))
    .withColumn(\"order_time\", F.to_timestamp(\"order_ts\"))
    .filter(F.col(\"amount_num\") > 0))

summary = typed_orders.groupBy(\"status\").agg(
    F.count(\"order_id\").alias(\"orders\"),
    F.round(F.sum(\"amount_num\"), 2).alias(\"revenue\"),
).orderBy(F.desc(\"revenue\"))
summary.show()
summary.explain(\"formatted\")"""
    cells = [
        md(f"""# {title}

## What is it?
{what}

## Why does it exist?
Spark lets a single program express parallel, fault-tolerant data work. Its optimizer can inspect DataFrame/SQL expressions before running them.

## When should I use it?
Use this technique when the data relationship or operational concern in the examples matches your problem; validate assumptions with data profiles and plans.

## How does it work?
```mermaid
flowchart LR
  A[DataFrame transformations] --> B[Logical plan]
  B --> C[Catalyst / physical plan]
  C --> D[Tasks over partitions]
  D --> E[Action result or files]
```

{lesson}"""),
        code(SETUP.format(title=title)),
        md("""## Guided example and output explanation

The result has one row per `status`: `count` counts non-null order IDs and `sum` adds valid numeric values. The formatted plan reveals scans, exchanges, and aggregate operators; execution is only demanded by `show()`.

**Common mistake:** treating a Python variable as local data after a transformation. It is a distributed plan until an action produces a result. **Performance:** select/filter early and use native functions so Spark can optimize work."""),
        code(common_example),
        md(f"""## Interactive experiment

{experiment}

Change one parameter, run the cell, and inspect both output and plan. Record your prediction before running it."""),
        code("""# Experiment: partition count is observable, while an action materializes the plan.
print(\"Input partitions:\", orders.rdd.getNumPartitions())
rebalanced = typed_orders.repartition(4, \"customer_id\")
print(\"Rebalanced partitions:\", rebalanced.rdd.getNumPartitions())
print(\"Valid rows (action):\", rebalanced.count())"""),
        md("""## Real-world lens

An e-commerce team commonly applies this pattern to raw order data before deriving a customer metric table. In production, inputs have explicit schemas, rejected records are observable, output is Parquet, and the transformation is covered by tests—see `src/pyspark_learning`.

## Practice exercises

1. Filter for completed orders, select `customer_id` and a numeric amount, and predict the schema before calling `printSchema()`.
2. Calculate revenue per customer, sort descending, and explain whether grouping can shuffle.
3. Join the customer lookup, preserving all valid orders; verify the row count and investigate any increase.

### Debugging challenge

This fails because `amount` is a string: `orders.filter(F.col('amount') > 10)`. Repair it without a Python loop and decide what to do with malformed values.

### Concept check

Which of `filter`, `withColumn`, `groupBy`, `show`, and `count` are actions? Which may create a shuffle, and why?"""),
        md("""## Recap and readiness checklist

- I can state the business and execution purpose of this module's operation.
- I can predict whether my code remains lazy or requests a result.
- I can inspect schema, sample safely, and use `explain('formatted')`.
- I can name at least one correctness risk and one performance risk.

Solutions are in the matching file under `solutions/`. Stop the session when working outside Jupyter with `spark.stop()`."""),
    ]
    return {"cells": cells, "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python", "version": "3.9"}}, "nbformat": 4, "nbformat_minor": 5}

def main():
    root = Path(__file__).resolve().parents[1]
    notebook_dir, solutions = root / "notebooks", root / "solutions"
    notebook_dir.mkdir(exist_ok=True); solutions.mkdir(exist_ok=True)
    for slug, title, what, lesson, experiment in LESSONS:
        (notebook_dir / f"{slug}.ipynb").write_text(json.dumps(notebook(slug, title, what, lesson, experiment), indent=2) + "\n", encoding="utf-8")
        solution = f"# Solutions: {title}\n\n## Exercise 1\n```python\norders.withColumn('amount_num', F.col('amount').cast('double')).filter(F.col('status') == 'completed').select('customer_id', 'amount_num').printSchema()\n```\n\n## Exercise 2\n```python\ntyped_orders.groupBy('customer_id').agg(F.sum('amount_num').alias('revenue')).orderBy(F.desc('revenue')).show()\n```\n`groupBy` is wide because records with the same key may begin in different partitions.\n\n## Exercise 3\n```python\ntyped_orders.join(customers, 'customer_id', 'left').count()\n```\nCompare this count to `typed_orders.count()`; any increase indicates duplicate customer keys.\n\n## Debugging\nCast first: `orders.withColumn('amount_num', F.col('amount').cast('double')).filter(F.col('amount_num') > 10)`. Null after a cast represents unparseable input; quarantine or reject it according to a documented rule.\n\n## Concept answer\n`show` and `count` are actions. `filter` and `withColumn` are narrow in typical cases; `groupBy` can shuffle.\n"
        (solutions / f"{slug}_solutions.md").write_text(solution, encoding="utf-8")

if __name__ == "__main__":
    main()
