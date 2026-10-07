# Design notes

- **Structured Streaming over Lakeflow.** It keeps idempotency and schema changes explicit: every
  write is a MERGE on a natural key, a `txnVersion`-guarded append or an idempotent UPDATE, and every
  table except the migration ledger comes from a numbered migration. Lakeflow
  is less code, but it owns the table lifecycle and schema changes often mean a full refresh.
- **Triggers.** `availableNow` everywhere: serverless has no processing-time triggers, and a scheduled
  run that drains what has landed and stops only costs compute while there is work.
- **Bronze** keeps values as received strings. Auto Loader's inferred schema is held in `rescue` mode,
  so an unexpected field lands in `_rescued_data` instead of changing the table outside a migration.
- **Dedup key** `(truck_id, event_ts)`: a truck can't be in two places at once. Silver drops
  duplicates within the batch and against itself with an insert-only MERGE, which keeps silver
  append-only so gold can stream from it. Bad rows go to a quarantine table with a reason.
- **Stream-static join.** `truck_details` is a broadcast `spark.read.table`; being Delta, each
  micro-batch joins its latest version, so changes show up without a restart. Even 500k trucks is tens
  of MB, fine to broadcast; history would need SCD2 and an as-of join.
- **Two gold outputs.** `gold_truck_position` (latest ping per truck, geofence flag) and
  `gold_region_pings` (pings per region per tumbling window). Window size and geofence are bundle
  variables; a new geofence re-flags every truck on the next run. The window table recounts the time
  range a batch touches from silver and replaces it, so neither a replay nor a truck changing region
  can double-count, and each window size has its own checkpoint.
- **Checkpoints.** One per stream in the target's `checkpoints` volume. A killed run resumes from the
  last committed offset; the replayed batch is harmless (MERGE on natural keys, quarantine append
  guarded by `txnVersion`). `max_concurrent_runs: 1` keeps runs off each other's checkpoints.
- **Environments.** One workspace, separated by schema and volumes: `dev` (development mode), `test`
  hourly, `prod` every 15 minutes in production mode. On Free Edition test and prod are deployed with
  schedules paused to save quota; one variable turns them on.
- **Tables as code, prod deploy-only.** The first task of every run applies `src/migrations/V*.sql`
  once each, recorded with a checksum. DDL is `IF NOT EXISTS`, an existing `ADD COLUMN` counts as
  done, backfills are `WHERE col IS NULL`. Changes stay additive (a rename is add, backfill, switch,
  drop). Rollback: redeploy the previous commit, which works because writers name their MERGE
  columns; the schema only moves forward, and `RESTORE TABLE` is kept for bad data.
- **CI/CD.** Branches `dev`, `test`, `main` map to dev, test, prod: a pull request lints and validates
  against its target, the merge deploys and runs the pipeline. People never write to prod: in a shared workspace a CI
  service principal (OIDC, not a PAT) owns the prod schema and is the jobs' `run_as`, people get
  `SELECT` by grant, and `prod` has required reviewers. On Free Edition one user owns everything, so
  here that is a convention.
- **Scaling to 100k+ trucks.** Kafka or Event Hubs instead of files; bound the silver MERGE with an
  `event_date` window and liquid clustering on `(event_date, truck_id)`; alert on stream lag and
  quarantine rate.
