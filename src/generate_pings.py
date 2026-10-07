# Databricks notebook source

import json
import os
import random
import time
from datetime import datetime, timezone

PARAMS = ("landing_path", "fleet_size", "batches", "interval_seconds")
for name in PARAMS:
    dbutils.widgets.text(name, "")

# COMMAND ----------

params = {name: dbutils.widgets.get(name) for name in PARAMS}
missing = [name for name, value in params.items() if not value]
if missing:
    raise ValueError(f"missing job parameters: {', '.join(missing)}")

landing = params["landing_path"]
fleet_size = int(params["fleet_size"])
batches = int(params["batches"])
interval_seconds = float(params["interval_seconds"])

TRUCKS = [f"TRK-{i:03d}" for i in range(1, fleet_size + 1)]  # same ids as seed_truck_details
CENTER_LAT, CENTER_LON = 41.85, -87.65  # Chicago
SPREAD_DEG = 0.2
PINGS_PER_FILE = (5, 15)

# COMMAND ----------


def make_ping(truck_id):
    return {
        "truck_id": truck_id,
        "latitude": round(CENTER_LAT + random.uniform(-SPREAD_DEG, SPREAD_DEG), 6),
        "longitude": round(CENTER_LON + random.uniform(-SPREAD_DEG, SPREAD_DEG), 6),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


os.makedirs(landing, exist_ok=True)

for batch in range(batches):
    rows = [make_ping(random.choice(TRUCKS)) for _ in range(random.randint(*PINGS_PER_FILE))]
    # planted: one duplicate, one missing latitude
    rows.append(dict(rows[0]))
    rows.append({**make_ping(random.choice(TRUCKS)), "latitude": None})

    path = f"{landing}/pings_{int(time.time() * 1000)}_{batch:03d}.json"
    with open(path, "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)
    print(f"wrote {path} ({len(rows)} rows)")

    if batch < batches - 1:
        time.sleep(interval_seconds)
