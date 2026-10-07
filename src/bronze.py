# Databricks notebook source

import os

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("landing_volume", "landing")
dbutils.widgets.text("checkpoint_volume", "checkpoints")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not schema:
    raise ValueError("schema is required")

landing = f"/Volumes/{catalog}/{schema}/{dbutils.widgets.get('landing_volume')}/pings"
checkpoints = f"/Volumes/{catalog}/{schema}/{dbutils.widgets.get('checkpoint_volume')}"


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
