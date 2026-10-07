# Databricks notebook source

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("checkpoint_volume", "checkpoints")
dbutils.widgets.text("region_window_minutes", "5")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not schema:
    raise ValueError("schema is required")

window_minutes = int(dbutils.widgets.get("region_window_minutes"))
if window_minutes <= 0:
    raise ValueError("region_window_minutes must be a positive number of minutes")

checkpoints = f"/Volumes/{catalog}/{schema}/{dbutils.widgets.get('checkpoint_volume')}"
SILVER = f"{catalog}.{schema}.silver_pings"
TRUCK_DETAILS = f"{catalog}.{schema}.truck_details"
GOLD = f"{catalog}.{schema}.gold_region_pings"
WINDOW = f"{window_minutes} minutes"

# COMMAND ----------


def upsert_batch(batch_df, batch_id):
    session = batch_df.sparkSession

    # Recount every window this micro-batch touched from silver itself instead of adding the batch's
    # counts to the old ones, so a replayed batch can't double-count.
    touched = batch_df.select(F.window("event_ts", WINDOW).alias("w")).distinct()
    regions = session.read.table(TRUCK_DETAILS).select("truck_id", "region")
    counts = (
        session.read.table(SILVER)
        .withColumn("w", F.window("event_ts", WINDOW))
        .join(touched, "w")
        .join(F.broadcast(regions), "truck_id", "left")
        .groupBy(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.coalesce(F.col("region"), F.lit("unknown")).alias("region"),
        )
        .agg(F.count("*").alias("pings"), F.countDistinct("truck_id").alias("trucks"))
        .select(F.lit(window_minutes).alias("window_minutes"), "*")
        .withColumn("updated_at", F.current_timestamp())
    )
    counts.createOrReplaceTempView("region_window_updates")

    cols = counts.columns
    set_clause = ", ".join(f"{c} = s.{c}" for c in cols)
    insert_cols = ", ".join(cols)
    insert_values = ", ".join(f"s.{c}" for c in cols)
    session.sql(f"""
        MERGE INTO {GOLD} AS t
        USING region_window_updates AS s
        ON t.window_minutes = s.window_minutes AND t.window_start = s.window_start AND t.region = s.region
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """)


# COMMAND ----------

# One checkpoint per window size: changing region_window_minutes starts a fresh stream that replays
# silver from the beginning and backfills the new size; rows for other sizes are left as they are.
(
    spark.readStream.table(SILVER)
    .select("truck_id", "event_ts")
    .writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/gold_region_windows_{window_minutes}m")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

spark.table(GOLD).where(F.col("window_minutes") == window_minutes).orderBy("window_start", "region").show(
    40, truncate=False
)
