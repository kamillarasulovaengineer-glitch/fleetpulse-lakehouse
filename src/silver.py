# Databricks notebook source

from pyspark.sql import functions as F

PARAMS = ("catalog", "schema", "checkpoint_volume")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

catalog, schema = params["catalog"], params["schema"]
checkpoints = f"/Volumes/{catalog}/{schema}/{params['checkpoint_volume']}"
BRONZE = f"{catalog}.{schema}.bronze_pings"
SILVER = f"{catalog}.{schema}.silver_pings"
QUARANTINE = f"{catalog}.{schema}.silver_pings_quarantine"

STREAM = "silver"  # checkpoint and quarantine txnAppId; rename to replay

# COMMAND ----------


def upsert_batch(batch_df, batch_id):
    checked = (
        batch_df.withColumn("_lat", F.expr("try_cast(latitude AS DOUBLE)"))
        .withColumn("_lon", F.expr("try_cast(longitude AS DOUBLE)"))
        .withColumn("_ts", F.expr("try_cast(event_ts AS TIMESTAMP)"))
        .withColumn(
            "_reason",
            F.when(F.col("truck_id").isNull(), "missing truck_id")
            .when(F.col("_ts").isNull(), "unparseable event_ts")
            .when(F.col("_lat").isNull() | F.col("_lon").isNull(), "missing coordinates")
            .when(
                ~F.col("_lat").between(-90, 90) | ~F.col("_lon").between(-180, 180),
                "coordinates out of range",
            ),
        )
    )

    (
        checked.filter("_reason IS NOT NULL")
        .select(
            "truck_id",
            "latitude",
            "longitude",
            "event_ts",
            "_rescued_data",
            "_source_file",
            "_ingested_at",
            F.col("_reason").alias("reason"),
            F.current_timestamp().alias("_quarantined_at"),
        )
        .write.option("txnAppId", f"fleetpulse_{STREAM}_quarantine")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable(QUARANTINE)
    )

    updates = (
        checked.filter("_reason IS NULL")
        .select(
            "truck_id",
            F.col("_ts").alias("event_ts"),
            F.col("_lat").alias("latitude"),
            F.col("_lon").alias("longitude"),
            "_source_file",
            "_ingested_at",
            F.current_timestamp().alias("_processed_at"),
        )
        .dropDuplicates(["truck_id", "event_ts"])
    )
    updates.createOrReplaceTempView("silver_updates")

    # insert-only, so gold can keep streaming from silver
    insert_cols = ", ".join(updates.columns)
    insert_values = ", ".join(f"s.{c}" for c in updates.columns)
    batch_df.sparkSession.sql(f"""
        MERGE INTO {SILVER} AS t
        USING silver_updates AS s
        ON t.truck_id = s.truck_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """)


# COMMAND ----------

(
    spark.readStream.table(BRONZE)
    .writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/{STREAM}")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
