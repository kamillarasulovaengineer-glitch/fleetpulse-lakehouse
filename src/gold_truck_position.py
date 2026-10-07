# Databricks notebook source

from pyspark.sql import Window
from pyspark.sql import functions as F

PARAMS = ("catalog", "schema", "checkpoint_volume", "geofence_box")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

catalog, schema = params["catalog"], params["schema"]
checkpoints = f"/Volumes/{catalog}/{schema}/{params['checkpoint_volume']}"
SILVER = f"{catalog}.{schema}.silver_pings"
TRUCK_DETAILS = f"{catalog}.{schema}.truck_details"
GOLD = f"{catalog}.{schema}.gold_truck_position"

try:
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = (float(v) for v in params["geofence_box"].replace(",", " ").split())
except ValueError:
    raise ValueError("geofence_box must be 'lat_min lat_max lon_min lon_max'") from None
if not (LAT_MIN < LAT_MAX and LON_MIN < LON_MAX):
    raise ValueError("geofence_box needs lat_min < lat_max and lon_min < lon_max")

# COMMAND ----------

truck_details = spark.read.table(TRUCK_DETAILS).select(
    "truck_id", "make", "model", "capacity_lbs", "home_depot", "region", "driver"
)

positions = (
    spark.readStream.table(SILVER)
    .select("truck_id", "event_ts", "latitude", "longitude")
    .join(F.broadcast(truck_details), "truck_id", "left")
)


def upsert_batch(batch_df, batch_id):
    latest_per_truck = Window.partitionBy("truck_id").orderBy(F.col("event_ts").desc())
    updates = (
        batch_df.withColumn("_rn", F.row_number().over(latest_per_truck))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn(
            "in_geofence",
            F.col("latitude").between(LAT_MIN, LAT_MAX) & F.col("longitude").between(LON_MIN, LON_MAX),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    updates.createOrReplaceTempView("gold_updates")

    # named columns, not SET *, so older code survives a new gold column
    cols = updates.columns
    set_clause = ", ".join(f"{c} = s.{c}" for c in cols)
    insert_cols = ", ".join(cols)
    insert_values = ", ".join(f"s.{c}" for c in cols)
    batch_df.sparkSession.sql(f"""
        MERGE INTO {GOLD} AS t
        USING gold_updates AS s
        ON t.truck_id = s.truck_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """)


# COMMAND ----------

(
    positions.writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/gold")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# apply a changed geofence_box to every truck, not only to those that pinged again
box = {"lat_min": LAT_MIN, "lat_max": LAT_MAX, "lon_min": LON_MIN, "lon_max": LON_MAX}
in_box = "(latitude BETWEEN :lat_min AND :lat_max AND longitude BETWEEN :lon_min AND :lon_max)"
stale = spark.sql(f"SELECT count(*) AS n FROM {GOLD} WHERE in_geofence IS DISTINCT FROM {in_box}", args=box).first().n
if stale:
    spark.sql(
        f"UPDATE {GOLD} SET in_geofence = {in_box}, updated_at = current_timestamp() "
        f"WHERE in_geofence IS DISTINCT FROM {in_box}",
        args=box,
    )

dbutils.notebook.exit(f"gold_truck_position: {spark.table(GOLD).count()} trucks, geofence re-flagged {stale}")
