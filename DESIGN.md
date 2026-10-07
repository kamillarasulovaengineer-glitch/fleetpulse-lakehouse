# Design notes

**Structured Streaming over Lakeflow.** Hand-rolled streams keep what the brief cares most about
explicit: every write is a MERGE or a transactional append, every table comes from a numbered
migration. Lakeflow is less code, but it owns the table lifecycle, so a schema change becomes "edit
the definition and reconcile", often with a full refresh.

**Triggers.** `availableNow` everywhere. Serverless has no processing-time triggers, and a scheduled
run that drains what has landed and stops only costs compute while there is work. Latency is the
schedule interval.

**Layers.** Bronze is Auto Loader, append-only, values kept as received strings; the inferred schema
is held (`rescue` mode) so unexpected fields go to `_rescued_data` instead of widening the table.
Silver casts with `try_cast` and quarantines failures with a reason. The dedup key is
`(truck_id, event_ts)`: a truck can't be in two places at once, so the same key is the same ping sent
twice. Duplicates go within the batch and against silver through an insert-only MERGE, which also
keeps silver append-only so gold can stream from it. Gold holds the latest position per truck and
only moves forward in time.

**Stream-static join.** `truck_details` is a broadcast `spark.read.table`. Being Delta, each
micro-batch joins its latest version, so a driver change shows up on the next batch without a
restart. Gold rows aren't re-enriched until the truck pings again, and there's no history (that would
be SCD2 plus an as-of join). Left join, so unknown trucks still get a position.

**Checkpoints.** One per stream in the target's `checkpoints` volume. A killed run resumes from the
last committed offset; the replayed batch is harmless because silver and gold MERGE on natural keys
and the quarantine append uses `txnAppId`/`txnVersion`. `max_concurrent_runs: 1` keeps two runs off
the same checkpoint.

**Environments.** One workspace, isolated by schema and volumes: `dev` (development mode, per-user
prefix), `test` hourly, `prod` every 15 minutes in production mode, schedules on US Eastern time.
The shared `telematics` catalog is created once outside the bundle. The simulator is deployed
everywhere only because there is no real feed here.

**Tables as code, prod deploy-only.** `migrate` runs first in every pipeline run and applies
`src/migrations/V*.sql` once each, in order, recorded with a checksum so an applied file can't be
edited quietly. DDL is `IF NOT EXISTS`, an `ADD COLUMN` that already exists counts as done (a run can
die between the ALTER and the ledger insert), backfills are `WHERE col IS NULL`. Promotion is the
same commit deployed to dev, test, then prod. Changes stay additive; a rename would be add, backfill,
switch readers, drop. Rollback is redeploying the previous commit: writers name their MERGE columns,
so old code keeps working against a newer schema. The schema itself only moves forward (a V003 that
reverses V002); `RESTORE TABLE` would leave the migration ledger ahead of the table and, on silver,
break the stream gold reads, so it's for bad data, not for undoing a migration. For real,
prod would also be enforced: jobs `run_as` a service principal, humans `SELECT` only, deploys from CI
on a tag behind an approval, OIDC instead of a PAT.

**Scaling to 100k+ trucks.** Kafka or Event Hubs instead of files. Bound the silver MERGE with an
`event_date` lateness window and liquid clustering on `(event_date, truck_id)`, or use
`dropDuplicatesWithinWatermark` with plain appends. Gold stays one row per truck. Alert on stream
lag and quarantine rate.
