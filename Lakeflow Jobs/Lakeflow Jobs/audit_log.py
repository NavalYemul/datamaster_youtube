# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DMC Bank - Job Audit Log
# MAGIC
# MAGIC Shared run-audit notebook used by two tasks: `audit_log` (run_if = ALL_DONE, `mode=completed`) and `failure_alert` (run_if = AT_LEAST_ONE_FAILED, `mode=failure`). Appends one row per event to `ops.job_run_audit`.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "yt_dev")
dbutils.widgets.text("job_id", "interactive")
dbutils.widgets.text("run_id", "interactive")
dbutils.widgets.dropdown("mode", "completed", ["completed", "failure", "skipped"])
dbutils.widgets.text("file_count", "0")
dbutils.widgets.text("job_params", "{}")

catalog = dbutils.widgets.get("catalog")
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
mode = dbutils.widgets.get("mode")
file_count = int(dbutils.widgets.get("file_count") or 0)
job_params = dbutils.widgets.get("job_params")

STATUS = {"completed": "COMPLETED", "failure": "FAILED", "skipped": "SKIPPED_NO_NEW_DATA"}
print(f"job_id={job_id} | run_id={run_id} | mode={mode} | file_count={file_count}")

# COMMAND ----------

# DBTITLE 1,Write audit record
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops COMMENT 'Operational metadata for DMC Bank jobs'")

# DQ outcome for this run (empty if DQ was skipped or not reached)
dq_table = f"{catalog}.ops.dq_results"
dq_summary = "n/a"
if spark.catalog.tableExists(dq_table):
    dq = spark.table(dq_table).filter(F.col("run_id") == run_id).agg(
        F.count("*").alias("checks"), F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("failed")
    ).first()
    if dq["checks"]:
        dq_summary = f"{dq['checks']} checks, {dq['failed']} failed"

audit_df = spark.createDataFrame(
    [(job_id, run_id, STATUS[mode], file_count, dq_summary, job_params)],
    "job_id string, run_id string, status string, new_file_count int, dq_summary string, job_params string",
).withColumn("logged_at", F.current_timestamp())

audit_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.job_run_audit")
display(audit_df)

# COMMAND ----------

# DBTITLE 1,Exit
if mode == "failure":
    # Re-raise so this leaf task keeps the overall job run FAILED (and triggers failure emails)
    raise Exception(f"[ALERT] DMC Bank job run {run_id} FAILED - check the run page and {catalog}.ops.dq_results")
dbutils.notebook.exit(STATUS[mode])