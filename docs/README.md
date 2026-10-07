# Evidence

CLI output captured while building and promoting the bundle. Workspace host and user are redacted,
times are US Eastern.

| File | What it shows |
|------|---------------|
| [00-cli-version](evidence/00-cli-version.txt) | Databricks CLI used for every deploy |
| [01-dev-deploy](evidence/01-dev-deploy.txt) | first `validate` + `deploy` of dev: schema, volumes and jobs created |
| [02](evidence/02-dev-simulator.txt), [03](evidence/03-dev-pipeline.txt) | simulator and pipeline runs in dev |
| [04-dev-checks](evidence/04-dev-checks.txt) | 20 files, 250 bronze rows, 210 silver + 20 quarantined (the 20 planted duplicates dropped), 20 trucks in gold joined with `truck_details` |
| [05-dev-rerun-no-new-data](evidence/05-dev-rerun-no-new-data.txt) | pipeline rerun with nothing new: counts unchanged, silver history still has a single MERGE |
| [06-test-v001](evidence/06-test-v001.txt), [07-prod-v001](evidence/07-prod-v001.txt) | test and prod at V001: deploy, simulator, pipeline |
| [08](evidence/08-dev-v002-before.txt), [09](evidence/09-dev-v002-run.txt), [10](evidence/10-dev-v002-after.txt) | V002 in dev (code deployed by CI): before, run, after |
| [12-test-v002](evidence/12-test-v002.txt) | V002 in test |
| [11](evidence/11-prod-v002-before.txt), [13](evidence/13-prod-v002.txt), [15](evidence/15-prod-v002-after.txt) | V002 in prod: before (V1, 11 columns), deploy + run, after (V2, `in_geofence`, and `DESCRIBE HISTORY` with `ADD COLUMNS` / `UPDATE` done by the pipeline job) |
| [14-dev-stop-restart](evidence/14-dev-stop-restart.txt) | run cancelled during silver after bronze took 10 new files; the rerun resumes from the checkpoint with one MERGE of 89 rows, and bronze 594 = silver 494 + quarantine 50 + 50 planted duplicates |
| [16-test](evidence/16-test-dashboard-deploy.txt), [17-prod](evidence/17-prod-dashboard-deploy.txt) | dashboard deployed to test and prod |
