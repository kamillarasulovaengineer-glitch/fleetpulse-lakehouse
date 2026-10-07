# Databricks notebook source

import os

from pyspark.sql import functions as F

PARAMS = ("catalog", "schema", "landing_path", "checkpoint_volume")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

catalog, schema, landing = params["catalog"], params["schema"], params["landing_path"]
checkpoints = f"/Volumes/{catalog}/{schema}/{params['checkpoint_volume']}"


def has_files(path):
    try:
        with os.scandir(path) as entries:
            return any(entry.is_file() for entry in entries)
    except FileNotFoundError:
        return False


# Auto Loader can't infer a schema from an empty directory
if not has_files(landing):
    dbutils.notebook.exit(f"nothing to ingest yet in {landing}")

# COMMAND ----------

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoints}/bronze_schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(landing)
    .select(
        "truck_id",
        "latitude",
        "longitude",
        "event_ts",
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .writeStream.option("checkpointLocation", f"{checkpoints}/bronze")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_pings")
)
query.awaitTermination()
