# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DMC Bank - Ingestion Precheck
# MAGIC
# MAGIC Task 1 of the `dmc_bank_e2e_daily` Lakeflow Job. Scans the landing volume for each entity, compares files against those already ingested into bronze, and publishes task values (`has_new_files`, `file_count`, `entities`) that drive the downstream If/else and For-each tasks.

# COMMAND ----------

# DBTITLE 1,Job parameters
import json

# Job parameters (job-level params override these defaults at run time)
dbutils.widgets.text("catalog", "yt_dev")
dbutils.widgets.text("source_volume", "/Volumes/yt_dev/bronze/dmc_bank_raw")
dbutils.widgets.text("entities", '["customer","account","transaction"]')
dbutils.widgets.dropdown("force_run", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
source_volume = dbutils.widgets.get("source_volume").rstrip("/")
entities = json.loads(dbutils.widgets.get("entities"))
force_run = dbutils.widgets.get("force_run").lower() == "true"

assert isinstance(entities, list) and entities, "entities must be a non-empty JSON array"
print(f"catalog={catalog} | source_volume={source_volume} | entities={entities} | force_run={force_run}")

# COMMAND ----------

# DBTITLE 1,Scan landing volume vs bronze
from pyspark.sql import functions as F

rows = []
for entity in entities:
    path = f"{source_volume}/{entity}"
    try:
        landed = {f.name for f in dbutils.fs.ls(path) if not f.isDir()}
    except Exception as e:
        print(f"[WARN] cannot list {path}: {e}")
        landed = set()

    # Files already ingested by the pipeline's bronze streaming table
    bronze_table = f"{catalog}.bronze.raw_{entity}"
    if spark.catalog.tableExists(bronze_table):
        ingested = {
            r["_source_file_name"]
            for r in spark.table(bronze_table).select("_source_file_name").distinct().collect()
        }
    else:
        ingested = set()

    new_files = sorted(landed - ingested)
    rows.append((entity, len(landed), len(ingested), len(new_files), ", ".join(new_files)))

summary_df = spark.createDataFrame(
    rows, "entity string, landed_files int, ingested_files int, new_files int, new_file_names string"
)
display(summary_df)

# COMMAND ----------

# DBTITLE 1,Publish task values
total_landed = sum(r[1] for r in rows)
new_file_count = sum(r[3] for r in rows)
has_new_files = new_file_count > 0 or (force_run and total_landed > 0)

# Consumed downstream as {{tasks.ingestion_precheck.values.<key>}}
dbutils.jobs.taskValues.set(key="has_new_files", value=has_new_files)
dbutils.jobs.taskValues.set(key="file_count", value=new_file_count)
dbutils.jobs.taskValues.set(key="entities", value=entities)

print(f"has_new_files={has_new_files} | new_file_count={new_file_count} | total_landed={total_landed} | force_run={force_run}")