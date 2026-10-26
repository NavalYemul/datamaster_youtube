# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DMC Bank - No New Data Handler
# MAGIC
# MAGIC False branch of the `if_new_data` condition task. Records a `SKIPPED_NO_NEW_DATA` event in `ops.job_run_audit` and exits cleanly so the run ends green without running the pipeline.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "yt_dev")
dbutils.widgets.text("job_id", "interactive")
dbutils.widgets.text("run_id", "interactive")
dbutils.widgets.text("source_volume", "/Volumes/yt_dev/bronze/dmc_bank_raw")

catalog = dbutils.widgets.get("catalog")
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
source_volume = dbutils.widgets.get("source_volume")

# COMMAND ----------

# DBTITLE 1,Log skip and exit
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops COMMENT 'Operational metadata for DMC Bank jobs'")

skip_df = spark.createDataFrame(
    [(job_id, run_id, "SKIPPED_NO_NEW_DATA", 0, "n/a", f'{{"source_volume": "{source_volume}"}}')],
    "job_id string, run_id string, status string, new_file_count int, dq_summary string, job_params string",
).withColumn("logged_at", F.current_timestamp())

skip_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.job_run_audit")
display(skip_df)

print(f"No new files in {source_volume}. Pipeline, DQ and dashboard refresh skipped.")
dbutils.notebook.exit("SKIPPED_NO_NEW_DATA")