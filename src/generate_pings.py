# Databricks notebook source

import json
import os
import random
import time
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "telematics")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("landing_volume", "landing")
dbutils.widgets.text("batches", "20")
dbutils.widgets.text("interval_seconds", "3")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_volume = dbutils.widgets.get("landing_volume")
batches = int(dbutils.widgets.get("batches"))
interval_seconds = float(dbutils.widgets.get("interval_seconds"))
if not schema:
    raise ValueError("schema is required")

landing = f"/Volumes/{catalog}/{schema}/{landing_volume}/pings"

TRUCKS = [f"TRK-{i:03d}" for i in range(1, 21)]
LAT0, LON0 = 41.85, -87.65  # Chicago

# COMMAND ----------


def make_ping(truck_id):
    return {
        "truck_id": truck_id,
        "latitude": round(LAT0 + random.uniform(-0.2, 0.2), 6),
        "longitude": round(LON0 + random.uniform(-0.2, 0.2), 6),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


os.makedirs(landing, exist_ok=True)

for batch in range(batches):
    rows = [make_ping(random.choice(TRUCKS)) for _ in range(random.randint(5, 15))]
    rows.append(dict(rows[0]))
    rows.append({**make_ping(random.choice(TRUCKS)), "latitude": None})

    path = f"{landing}/pings_{int(time.time() * 1000)}_{batch:03d}.json"
    with open(path, "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)
    print(f"wrote {path} ({len(rows)} rows)")

    if batch < batches - 1:
        time.sleep(interval_seconds)
