# Databricks notebook source

import random

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not schema:
    raise ValueError("schema is required")

MAKES = [("Freightliner", "Cascadia"), ("Volvo", "VNL"), ("Kenworth", "T680"), ("Peterbilt", "579")]
DEPOTS = [("Chicago", "Midwest"), ("Dallas", "South"), ("Denver", "West"), ("Atlanta", "Southeast")]
DRIVERS = [
    "A. Rivera",
    "B. Chen",
    "C. Okafor",
    "D. Patel",
    "E. Nguyen",
    "F. Santos",
    "G. Kim",
    "H. Brooks",
    "I. Novak",
    "J. Alvarez",
]
CAPACITIES_LBS = [20000, 26000, 34000, 40000]

# COMMAND ----------

rng = random.Random(42)
trucks = []
for i in range(1, 21):
    make, model = rng.choice(MAKES)
    depot, region = rng.choice(DEPOTS)
    capacity = rng.choice(CAPACITIES_LBS)
    driver = rng.choice(DRIVERS)
    trucks.append((f"TRK-{i:03d}", make, model, capacity, depot, region, driver))

spark.createDataFrame(
    trucks,
    "truck_id STRING, make STRING, model STRING, capacity_lbs INT, home_depot STRING, region STRING, driver STRING",
).createOrReplaceTempView("truck_details_src")

spark.sql(f"""
    MERGE INTO {catalog}.{schema}.truck_details AS t
    USING truck_details_src AS s
    ON t.truck_id = s.truck_id
    WHEN MATCHED AND NOT (
            t.make <=> s.make
        AND t.model <=> s.model
        AND t.capacity_lbs <=> s.capacity_lbs
        AND t.home_depot <=> s.home_depot
        AND t.region <=> s.region
        AND t.driver <=> s.driver
    ) THEN UPDATE SET
        make = s.make,
        model = s.model,
        capacity_lbs = s.capacity_lbs,
        home_depot = s.home_depot,
        region = s.region,
        driver = s.driver,
        updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (truck_id, make, model, capacity_lbs, home_depot, region, driver, updated_at)
        VALUES (s.truck_id, s.make, s.model, s.capacity_lbs, s.home_depot, s.region, s.driver, current_timestamp())
""").show()
