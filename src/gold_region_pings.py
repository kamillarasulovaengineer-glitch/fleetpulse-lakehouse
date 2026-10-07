# Databricks notebook source

from pyspark.sql import functions as F

PARAMS = ("catalog", "schema", "checkpoint_volume", "region_window_minutes")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

catalog, schema = params["catalog"], params["schema"]
window_minutes = int(params["region_window_minutes"])
if window_minutes <= 0:
    raise ValueError("region_window_minutes must be a positive number of minutes")

checkpoints = f"/Volumes/{catalog}/{schema}/{params['checkpoint_volume']}"
SILVER = f"{catalog}.{schema}.silver_pings"
TRUCK_DETAILS = f"{catalog}.{schema}.truck_details"
GOLD = f"{catalog}.{schema}.gold_region_pings"
WINDOW = f"{window_minutes} minutes"

# COMMAND ----------


def upsert_batch(batch_df, batch_id):
    session = batch_df.sparkSession

    # recount the touched time range from silver and replace it in gold
    bounds = (
        batch_df.select(F.window("event_ts", WINDOW).alias("w"))
        .agg(F.min("w.start").alias("lo"), F.max("w.end").alias("hi"))
        .first()
    )
    if bounds is None or bounds.lo is None:
        return

    regions = session.read.table(TRUCK_DETAILS).select("truck_id", "region")
    counts = (
        session.read.table(SILVER)
        .where((F.col("event_ts") >= F.lit(bounds.lo)) & (F.col("event_ts") < F.lit(bounds.hi)))
        .withColumn("w", F.window("event_ts", WINDOW))
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
    session.sql(
        f"""
        MERGE INTO {GOLD} AS t
        USING region_window_updates AS s
        ON t.window_minutes = s.window_minutes AND t.window_start = s.window_start AND t.region = s.region
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
        WHEN NOT MATCHED BY SOURCE
            AND t.window_minutes = :window_minutes AND t.window_start >= :lo AND t.window_start < :hi
            THEN DELETE
        """,
        args={"window_minutes": window_minutes, "lo": bounds.lo, "hi": bounds.hi},
    )


# COMMAND ----------

# one checkpoint per window size, so a new size backfills from the start
(
    spark.readStream.table(SILVER)
    .select("truck_id", "event_ts")
    .writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/gold_region_windows_{window_minutes}m")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

summary = (
    spark.table(GOLD)
    .where(F.col("window_minutes") == window_minutes)
    .agg(F.count("*").alias("rows"), F.coalesce(F.sum("pings"), F.lit(0)).alias("pings"))
)
row = summary.first()
dbutils.notebook.exit(f"gold_region_pings: {row.rows} region windows of {window_minutes} min, {row.pings} pings")
