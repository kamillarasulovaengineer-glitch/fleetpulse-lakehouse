# Databricks notebook source

import random

PARAMS = ("catalog", "schema", "fleet_size")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

catalog, schema = params["catalog"], params["schema"]
fleet_size = int(params["fleet_size"])

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
for i in range(1, fleet_size + 1):
    make, model = rng.choice(MAKES)
    depot, region = rng.choice(DEPOTS)
    capacity = rng.choice(CAPACITIES_LBS)
    driver = rng.choice(DRIVERS)
    trucks.append((f"TRK-{i:03d}", make, model, capacity, depot, region, driver))

spark.createDataFrame(
    trucks,
    "truck_id STRING, make STRING, model STRING, capacity_lbs INT, home_depot STRING, region STRING, driver STRING",
).createOrReplaceTempView("truck_details_src")

result = spark.sql(f"""
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
""").first()

dbutils.notebook.exit(
    f"truck_details: {fleet_size} trucks, {result.num_inserted_rows} inserted, {result.num_updated_rows} updated"
)
