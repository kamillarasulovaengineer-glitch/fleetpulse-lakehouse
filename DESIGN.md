# Design notes

**Structured Streaming over Lakeflow.** The brief weighs idempotent writes and versioned table
structure heavily, and hand-rolled streams make both explicit: every write is a MERGE or a
transactional append, every table comes from a numbered migration. Lakeflow is less code and what
I'd pick for a larger team, but it owns the table lifecycle, so schema changes become "edit the
definition and let it reconcile", often with a full refresh.

**Triggers.** `availableNow` everywhere: serverless has no processing-time triggers, and a scheduled
run that drains what has landed and stops only costs compute while there is work. Latency is the
schedule (15 min in prod); seconds would mean a continuous pipeline and an always-on bill.

**Layers.** Bronze is Auto Loader, append-only, values kept as received strings. The inferred
schema is held (`schemaEvolutionMode = rescue`), so unexpected fields go to `_rescued_data` rather
than silently widening the table. Silver casts with `try_cast` and quarantines failures with a
reason. Dedup key is `(truck_id, event_ts)`: a truck can't be in two places at once, so the same
key is the same ping sent twice. Duplicates are dropped within the batch and against silver via an
insert-only MERGE, which also keeps silver append-only so gold can stream from it. Gold keeps the
latest position per truck; its MERGE only moves forward in time.

**Stream-static join.** `truck_details` is read with `spark.read.table` and broadcast. Being Delta,
each micro-batch joins its latest version, so a driver change shows up on the next batch with no
restart. Trade-offs: gold rows aren't re-enriched until the truck pings again, and there's no
history (that would be SCD2 plus an as-of join). Left join, so unknown trucks still get a position.
Even 500k trucks is tens of MB, fine to broadcast.

**Checkpoints.** One per stream in the target's `checkpoints` volume. A killed run resumes from the
last committed offset; the replayed batch is harmless because silver and gold MERGE on natural keys
and the quarantine append uses `txnAppId`/`txnVersion`. `max_concurrent_runs: 1` stops two runs
sharing a checkpoint.

**Environments.** One workspace, so isolation is by schema and volumes. `dev`: development mode,
per-user prefix, paused. `test`: hourly. `prod`: production mode, every 15 min. The `telematics`
catalog is shared by all three, so no target owns it; it's created once outside the bundle.

**Tables as code, prod deploy-only.** `migrate` is the first task of every run. It applies
`src/migrations/V*.sql` in order, once each, recorded in `_schema_migrations` with a checksum, so an
applied file can't be edited quietly. DDL is idempotent (`CREATE TABLE IF NOT EXISTS`); an
`ADD COLUMN` that already exists is treated as done, which covers a run dying between the ALTER and
the ledger insert; backfills are `UPDATE ... WHERE col IS NULL`. Promotion is the same commit
deployed to dev, test, then prod, and since `migrate` runs before the writers, new code never meets
an old schema. Changes stay additive (expand, then contract later); a rename needs column mapping
and breaks downstream streams, so it would be add, backfill, switch readers, drop.
Rollback: redeploy the previous commit; reverse schema with a new forward migration; data with
`RESTORE TABLE ... TO VERSION AS OF`. Here "nobody writes to prod" is a convention; for real it
would be jobs `run_as` a service principal, humans with `SELECT` only, and prod deployed from CI on a
tag behind an approval. CI today validates on PRs and deploys dev on push; next is OIDC instead of a
PAT.

**Scaling to 100k+ trucks.** Kafka/Event Hubs instead of files (or Auto Loader file notifications).
Bound the silver MERGE by adding `event_date` to the key with a lateness window and liquid-clustering
on `(event_date, truck_id)`, or switch to `dropDuplicatesWithinWatermark` and plain appends. Gold
stays one row per truck. Add alerts on stream lag and quarantine rate.
