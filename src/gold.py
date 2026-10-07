# Databricks notebook source

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("checkpoint_volume", "checkpoints")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not schema:
    raise ValueError("schema is required")

checkpoints = f"/Volumes/{catalog}/{schema}/{dbutils.widgets.get('checkpoint_volume')}"
SILVER = f"{catalog}.{schema}.silver_pings"
TRUCK_DETAILS = f"{catalog}.{schema}.truck_details"
GOLD = f"{catalog}.{schema}.gold_truck_position"

# COMMAND ----------

truck_details = spark.read.table(TRUCK_DETAILS).drop("updated_at")

positions = (
    spark.readStream.table(SILVER)
    .select("truck_id", "event_ts", "latitude", "longitude")
    .join(F.broadcast(truck_details), "truck_id", "left")
)


def upsert_batch(batch_df, batch_id):
    latest_per_truck = Window.partitionBy("truck_id").orderBy(F.col("event_ts").desc())
    (
        batch_df.withColumn("_rn", F.row_number().over(latest_per_truck))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("updated_at", F.current_timestamp())
        .createOrReplaceTempView("gold_updates")
    )

    batch_df.sparkSession.sql(f"""
        MERGE INTO {GOLD} AS t
        USING gold_updates AS s
        ON t.truck_id = s.truck_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

(
    positions.writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/gold")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

spark.table(GOLD).orderBy("truck_id").show(25, truncate=False)
