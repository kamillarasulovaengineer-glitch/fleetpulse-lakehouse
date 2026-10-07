# Design notes

## Structured Streaming, not Lakeflow

I wrote the three hops as plain Structured Streaming jobs. The brief puts a lot of weight on
idempotent writes and on table structure being versioned code, and with hand-rolled streams both
are explicit: every write is a MERGE (or a transactional append) I can point at, and every table
comes from a numbered migration. Lakeflow would be less code (expectations, managed checkpoints,
lineage for free), and it's what I'd reach for with a bigger team, but it owns the table
lifecycle: a schema change is "edit the definition and let the pipeline reconcile it", which often
means a full refresh, and that's a weaker story for "prod is deploy-only, changes are reviewed
migrations".

## Triggers

Every stream uses `availableNow`. Serverless doesn't support processing-time triggers anyway, and
for this workload it's the right shape: a scheduled run drains whatever has landed, commits, and
stops, so compute is only paid for while there's work. Latency is bounded by the schedule
(15 minutes in prod). If the business needed seconds, I'd move to a continuous Lakeflow pipeline
or classic compute with a processing-time trigger, and pay for the always-on cluster.

## Bronze / silver / gold

- **Bronze** is Auto Loader into an append-only table, values kept as the strings we received,
  plus `_source_file` and `_ingested_at`. Schema inference runs once and is then held
  (`schemaEvolutionMode = rescue`): an unexpected field lands in `_rescued_data` instead of
  widening the table on its own. Nothing is lost, and structural change stays a deliberate migration.
- **Silver** applies types with `try_cast` and quarantines anything that fails, with a reason,
  rather than dropping it silently. The dedup key is `(truck_id, event_ts)`: a truck can't be in
  two places at the same instant, so the same key means the same ping sent twice. Duplicates are
  removed within the batch (`dropDuplicates`) and against silver itself (insert-only MERGE).
  Insert-only matters: silver never rewrites files, so gold can keep streaming from it.
- **Gold** is `gold_truck_position`: the latest position of every truck with vehicle, depot,
  region and driver. MERGE on `truck_id`, and a row only moves forward in time, so a late or
  replayed ping can't overwrite a newer position.

## The stream-static join

`truck_details` is read with `spark.read.table` and broadcast. Because it's a Delta table, each
micro-batch joins against its latest version, so a driver reassignment shows up on the next batch
without restarting anything. The trade-offs I accepted:

- Rows already in gold aren't re-enriched until that truck pings again.
- There's no history: a ping is joined to the details as they are now, not as they were at
  `event_ts`. If that mattered (driver pay, compliance), `truck_details` would become SCD2 and the
  join an as-of join on `event_ts`.
- It's a left join, so a ping from an unknown truck still gets a position with empty details
  instead of disappearing.

It stays cheap at scale because the static side is small: even 500k trucks is tens of MB, well
within broadcast range. Past that I'd drop the broadcast hint and cluster both sides on `truck_id`.

## Checkpoints and restarts

Each stream has its own checkpoint under the target's `checkpoints` volume, separate from
`landing`. Kill a run at any point and the next one resumes from the last committed offset. The
batch that was in flight gets replayed, which is harmless: silver and gold are MERGEs keyed on
natural keys, and the quarantine append is guarded by `txnAppId`/`txnVersion`, so Delta skips a
batch it has already committed. `max_concurrent_runs: 1` makes sure two runs never share a
checkpoint.

## Environments

One workspace, so environments are separated by schema and volumes, not by workspace. `dev` uses
development mode (per-user prefix, schedules paused), `test` runs hourly, `prod` every 15 minutes
in production mode with a pinned root path. The `telematics` catalog is shared by all three, so no
target owns it; it's created once outside the bundle (in a real setup the platform team would
own it).

## Tables as code, prod as deploy-only

The first task of every pipeline run is `migrate.py`. It applies `src/migrations/V*.sql` in
version order, each exactly once, and records them in `_schema_migrations` with a checksum, so
an applied file can't be edited quietly: the job fails and the fix is a new migration. Scripts are
idempotent where SQL allows it (`CREATE TABLE IF NOT EXISTS`). `ADD COLUMN` can't be re-run, so
each ALTER goes in its own file and the tracking table guarantees it runs once.

Promotion is just the same commit deployed to `dev`, then `test`, then `prod`. Because `migrate`
runs before the writers in the same job, new code never meets an old schema. I keep changes on the
hot path additive (expand, then contract later): adding a column is safe for every reader still on
the previous version. Renames need column mapping and break downstream streams unless they track
schema, so I'd avoid them on streaming tables.

**Rollback.** Code: redeploy the previous commit. Schema: roll forward with a new migration that
reverses the change. An additive column can usually just stay, because old code ignores it. Data:
Delta time travel (`RESTORE TABLE ... TO VERSION AS OF`).

Nobody writes to prod by hand. In this exercise that's a convention. In a real setup it would be
enforced: jobs `run_as` a service principal, humans get `SELECT` only on the prod schema, and
prod deploys come from CI on a tag behind an environment approval.

## CI

GitHub Actions validates the bundle on every PR and deploys `dev` on push to `main`, using
`DATABRICKS_HOST`/`DATABRICKS_TOKEN` secrets. Next steps: OIDC workload identity federation with a
service principal instead of a PAT, `test` deployed on merge with a smoke run, and `prod` on a
release tag through a protected environment with required reviewers.

## From 20 trucks to hundreds of thousands

- **Ingest.** Devices would publish to Kafka or Event Hubs rather than drop files. If files stay,
  Auto Loader in file-notification mode instead of directory listing.
- **Silver dedup.** A MERGE against all of silver gets expensive. Bound it: add `event_date`, make
  it part of the merge condition with a lateness window (say 2 days), and liquid-cluster on
  `(event_date, truck_id)`. Alternatively `dropDuplicatesWithinWatermark` with RocksDB state and a
  plain append.
- **Gold** stays one row per truck, so even 500k rows is a small, cheap MERGE once clustered on
  `truck_id`.
- **Latency and cost.** More frequent triggers or a continuous pipeline, plus alerting on stream
  lag and on the quarantine rate.

## What I'd do next

Unit tests for the silver validation rules (pytest against a local SparkSession), a second gold
table with pings per region per 5-minute window, a data-quality alert when the quarantine share
jumps, and SCD2 on `truck_details`.
