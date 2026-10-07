# Design notes

- **Structured Streaming over Lakeflow.** It keeps idempotency and schema changes explicit: every
  write is a MERGE or a transactional append, every table comes from a numbered migration. Lakeflow
  is less code, but it owns the table lifecycle and schema changes often mean a full refresh.
- **Triggers.** `availableNow` everywhere: serverless has no processing-time triggers, and a scheduled
  run that drains what has landed and stops only costs compute while there is work.
- **Dedup key** `(truck_id, event_ts)`: a truck can't be in two places at once. Silver drops
  duplicates within the batch and against itself with an insert-only MERGE, which keeps silver
  append-only so gold can stream from it. Bad rows go to a quarantine table with a reason.
- **Stream-static join.** `truck_details` is a broadcast `spark.read.table`; being Delta, each
  micro-batch joins its latest version, so changes show up without a restart. Old gold rows refresh
  on the truck's next ping; history would need SCD2 and an as-of join.
- **Checkpoints.** One per stream in the target's `checkpoints` volume. A killed run resumes from the
  last committed offset, and the replayed batch is harmless (MERGE on natural keys, quarantine append
  guarded by `txnVersion`). `max_concurrent_runs: 1` keeps runs off each other's checkpoints.
- **Environments.** One workspace, separated by schema and volumes: `dev` (development mode),
  `test` hourly, `prod` every 15 minutes in production mode. The shared catalog is created once
  outside the bundle.
- **Tables as code, prod deploy-only.** The first task of every run applies `src/migrations/V*.sql`
  once each, recorded with a checksum. DDL is `IF NOT EXISTS`, an existing `ADD COLUMN` counts as
  done, backfills are `WHERE col IS NULL`. Changes stay additive (a rename is add, backfill, switch,
  drop). Rollback: redeploy the previous commit, which works because writers name their MERGE
  columns; the schema only moves forward with a new migration, and `RESTORE TABLE` is kept for bad
  data.
- **CI/CD.** Branches `dev`, `test`, `main` deploy to dev, test, prod; a pull request validates against
  its target. Next: OIDC instead of a PAT, a service principal for `run_as`, reviewers on `prod`.
- **Scaling to 100k+ trucks.** Kafka or Event Hubs instead of files; bound the silver MERGE with an
  `event_date` window and liquid clustering on `(event_date, truck_id)`; alert on stream lag and
  quarantine rate.
