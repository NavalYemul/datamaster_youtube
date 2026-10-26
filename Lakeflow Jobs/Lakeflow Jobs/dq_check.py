# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DMC Bank - Silver Data Quality Check
# MAGIC
# MAGIC Runs inside the `dq_for_each_entity` For-each task, once per entity (`{{input}}`). Validates `silver.<entity>` for minimum row count, null primary keys and duplicate primary keys, logs results to `ops.dq_results`, and fails the iteration on any breach.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "yt_dev")
dbutils.widgets.dropdown("entity", "customer", ["customer", "account", "transaction"])
dbutils.widgets.text("min_rows", "1")
dbutils.widgets.text("run_id", "interactive")

catalog = dbutils.widgets.get("catalog")
entity = dbutils.widgets.get("entity")
min_rows = int(dbutils.widgets.get("min_rows"))
run_id = dbutils.widgets.get("run_id")

# Primary key per silver entity
PRIMARY_KEYS = {"customer": "customer_id", "account": "account_id", "transaction": "transaction_id"}
assert entity in PRIMARY_KEYS, f"Unknown entity '{entity}'"

table = f"{catalog}.silver.{entity}"
pk = PRIMARY_KEYS[entity]
print(f"DQ check -> table={table} | pk={pk} | min_rows={min_rows} | run_id={run_id}")

# COMMAND ----------

# DBTITLE 1,Run DQ checks
from pyspark.sql import functions as F

m = (
    spark.table(table)
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col(pk).isNull(), 1).otherwise(0)).alias("null_keys"),
        (F.count(pk) - F.countDistinct(pk)).alias("duplicate_keys"),
    )
    .first()
)

checks = [
    ("row_count_min", int(m["row_count"]), min_rows, m["row_count"] >= min_rows),
    ("null_primary_keys", int(m["null_keys"] or 0), 0, (m["null_keys"] or 0) == 0),
    ("duplicate_primary_keys", int(m["duplicate_keys"]), 0, m["duplicate_keys"] == 0),
]

results_df = (
    spark.createDataFrame(checks, "check_name string, metric_value long, threshold long, passed boolean")
    .withColumn("run_id", F.lit(run_id))
    .withColumn("entity", F.lit(entity))
    .withColumn("table_name", F.lit(table))
    .withColumn("checked_at", F.current_timestamp())
    .select("run_id", "entity", "table_name", "check_name", "metric_value", "threshold", "passed", "checked_at")
)
display(results_df)

# COMMAND ----------

# DBTITLE 1,Persist results and enforce
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops COMMENT 'Operational metadata for DMC Bank jobs'")
results_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.dq_results")

failed = [c[0] for c in checks if not c[3]]
if failed:
    # Raising fails this For-each iteration -> job marks DQ as failed -> failure_alert fires
    raise Exception(f"DQ FAILED for {table}: {failed}")

dbutils.notebook.exit(f"DQ PASSED for {table}")