# Databricks notebook source
# bronze_pings -> silver_pings, rejects -> silver_pings_quarantine.
# Types are applied here and invalid rows are quarantined with a reason. Duplicates are removed
# within each micro-batch and against silver itself (insert-only MERGE on truck_id + event_ts),
# so replaying a batch after a failure can't double-count anything.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("checkpoint_volume", "checkpoints")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not schema:
    raise ValueError("schema is required")

checkpoints = f"/Volumes/{catalog}/{schema}/{dbutils.widgets.get('checkpoint_volume')}"
BRONZE = f"{catalog}.{schema}.bronze_pings"
SILVER = f"{catalog}.{schema}.silver_pings"
QUARANTINE = f"{catalog}.{schema}.silver_pings_quarantine"

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

    # txnAppId + txnVersion make this append idempotent: if the same micro-batch is replayed,
    # Delta sees the version was already committed and skips it. If the silver checkpoint is ever
    # reset, batch ids restart from 0, so change the app id at the same time.
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
        .write.option("txnAppId", "fleetpulse_silver_quarantine")
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable(QUARANTINE)
    )

    (
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
        .createOrReplaceTempView("silver_updates")
    )

    # Insert-only on purpose: silver never rewrites existing files, so it stays a valid
    # append-only streaming source for gold.
    batch_df.sparkSession.sql(f"""
        MERGE INTO {SILVER} AS t
        USING silver_updates AS s
        ON t.truck_id = s.truck_id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)


# COMMAND ----------

(
    spark.readStream.table(BRONZE)
    .writeStream.foreachBatch(upsert_batch)
    .option("checkpointLocation", f"{checkpoints}/silver")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
