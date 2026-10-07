# fleetpulse

GPS pings from a truck fleet, streamed through bronze / silver / gold on Databricks and joined with
truck reference data. Everything (schema, volumes, tables, jobs, dashboard) is deployed by one bundle to
`dev`, `test` and `prod`.

![Data flow](docs/architecture.png)

The diagram is generated from [docs/architecture.mmd](docs/architecture.mmd) with mermaid-cli
(`mmdc -i docs/architecture.mmd -o docs/architecture.png -b white -s 2 -w 1400`).

Design notes and trade-offs are in [DESIGN.md](DESIGN.md). Proof that it runs is in [docs/README.md](docs/README.md).

## Layout

```
databricks.yml                     targets and variables
resources/
  unity_catalog.yml                schema + landing / checkpoints volumes
  fleetpulse_pipeline.job.yml      migrate -> seed_truck_details, bronze -> silver -> both gold tables
  fleetpulse_simulator.job.yml     synthetic ping generator
  fleetpulse.dashboard.yml         fleet overview dashboard
src/
  migrations/V*.sql                versioned DDL, applied in order by migrate.py
  migrate.py
  seed_truck_details.py
  generate_pings.py
  bronze.py  silver.py  gold.py  gold_region_windows.py
  dashboards/fleet_overview.lvdash.json
.github/workflows/bundle.yml       CI/CD: dev -> dev, test -> test, main -> prod
docs/                              data-flow diagram, CLI logs and screenshots from the runs
```

## Targets

|                   | dev                          | test                     | prod                       |
|-------------------|------------------------------|--------------------------|----------------------------|
| mode              | development                  | -                        | production                 |
| schema            | `telematics.dev_<user>_dev`  | `telematics.test`        | `telematics.prod`          |
| pipeline job      | `[dev <user>] fleetpulse-pipeline-dev` | `fleetpulse-pipeline-test` | `fleetpulse-pipeline-prod` |
| schedule          | paused                       | hourly                   | every 15 minutes           |

Schedules run on US Eastern time. Development mode prefixes names with the deploying user so people
don't collide in dev.
`databricks bundle summary -t <target>` prints the exact names.

## Configuration

Everything that changes between environments or might be tuned is a bundle variable in
`databricks.yml`. Override per target there, or for one deploy with `--var name=value`.

| Variable | Default | Used for |
|----------|---------|----------|
| `catalog` | `telematics` | catalog that holds every target's schema |
| `schema` | per target | `dev` (prefixed per user), `test`, `prod` |
| `pipeline_cron` | hourly; prod every 15 min | pipeline schedule (US Eastern) |
| `pipeline_pause_status` | `PAUSED`; test/prod `UNPAUSED` | whether the schedule runs |
| `region_window_minutes` | `5` | tumbling window for `gold_region_pings` |
| `geofence_box` | `41.80,41.95,-87.75,-87.55` | box behind `gold_truck_position.in_geofence` |
| `warehouse_id` | looked up by name | SQL warehouse behind the dashboard |

Example: `databricks bundle deploy -t dev --var region_window_minutes=10`. A new window size gets its
own checkpoint, so the next run backfills it from all of silver; rows for other sizes stay.

## Prerequisites

- A Databricks workspace (Free Edition is fine). No compute is declared anywhere in the bundle, so
  every task runs on serverless.
- Databricks CLI, recent 1.x (developed against v1.19.0).
- Auth: `databricks configure` with the workspace URL and a personal access token, then
  `databricks current-user me` to check.

## One-time setup

The catalog is shared by all three targets, so no single target owns it. Create it once, from the
SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS telematics;
```

On Free Edition this has to go through SQL (or Catalog Explorer): the metastore has no storage root,
so `databricks catalogs create` is rejected, while SQL picks up the account's default storage.

If your workspace doesn't let you create catalogs, use one you can write to and pass
`--var catalog=<name>` to every `deploy` and `run` below.

## Deploy and run

```
databricks bundle validate

databricks bundle deploy -t dev
databricks bundle run fleetpulse_simulator -t dev   # ~20 files of pings into the landing volume
databricks bundle run fleetpulse_pipeline  -t dev   # migrations, then bronze -> silver -> gold
```

Then the same for `test` and `prod`:

```
databricks bundle deploy -t test && databricks bundle run fleetpulse_simulator -t test && databricks bundle run fleetpulse_pipeline -t test
databricks bundle deploy -t prod && databricks bundle run fleetpulse_simulator -t prod && databricks bundle run fleetpulse_pipeline -t prod
```

More data: `databricks bundle run fleetpulse_simulator -t dev --params batches=50`.
On Free Edition I deploy `test` and `prod` with `--var pipeline_pause_status=PAUSED` between demos so
the schedules don't burn the daily compute quota; deploy without the flag to turn them back on.
Running the pipeline again only processes files that arrived since the last run.

## Checking the result

```sql
SELECT truck_id, driver, region, home_depot, make, model, event_ts, latitude, longitude, in_geofence
FROM telematics.prod.gold_truck_position
ORDER BY region, truck_id;

-- pings per region per window (window size is a bundle variable)
SELECT window_minutes, window_start, region, pings, trucks
FROM telematics.prod.gold_region_pings
ORDER BY window_start DESC, region;

-- what was rejected and why
SELECT reason, count(*) FROM telematics.prod.silver_pings_quarantine GROUP BY reason;

-- schema version of an environment
SELECT * FROM telematics.prod._schema_migrations ORDER BY version;
```

## Dashboard

`fleetpulse fleet overview (<target>)` is deployed with the bundle to every target. Its queries use
unqualified table names and the bundle points them at the target's catalog and schema
(`dataset_catalog` / `dataset_schema`); the SQL warehouse is looked up by name. Times are shown in
US Eastern.

![Fleet overview dashboard in prod](docs/screenshots/09-prod-dashboard.png)

## Changing a table

Tables are never created or altered by hand, in any environment.

1. Add `src/migrations/V<next>__<what>.sql`. Never edit a migration that has already been
   applied anywhere; the runner checksums them and will fail the job if one changes.
2. Change the code that writes the table, in the same commit.
3. Deploy and run `dev`, then `test`, then `prod`. The `migrate` task runs first in every
   pipeline run, so the new column exists before any writer needs it.

`V002__gold_truck_position_add_in_geofence.sql` went through exactly this; the before/after for
`prod`, including `DESCRIBE HISTORY` showing the `ADD COLUMNS` done by the pipeline job, is in
[docs/evidence](docs/evidence/).

## CI/CD

Each long-lived branch maps to one target:

| Branch | Target |
|--------|--------|
| `dev`  | dev    |
| `test` | test   |
| `main` | prod   |

`.github/workflows/bundle.yml` validates the bundle against the target of the branch a pull request
merges into, and deploys that target when the pull request is merged. A change moves
`feature -> dev -> test -> main`, one pull request per step, so it reaches prod only after it has been
deployed to dev and test.

Setup: repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. Schedules are deployed paused
unless the repository variable `PIPELINE_PAUSE_STATUS` is set to `UNPAUSED` (Free Edition quota).
Deploys run as GitHub environments `dev`, `test` and `prod`, so required reviewers can be added to
`prod` without touching the workflow.
