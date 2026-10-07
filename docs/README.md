# Evidence

CLI output captured while building and promoting the bundle, and screenshots of the workspace.
Workspace host and user are redacted, times are US Eastern.

## Schema changes reaching prod

| Change | How it reached prod | Proof |
| ------ | ------------------- | ----- |
| V002: `in_geofence` added to gold | deploy + run, dev, then test, then prod | [08](evidence/08-dev-v002-before.txt) [09](evidence/09-dev-v002-run.txt) [10](evidence/10-dev-v002-after.txt) [12](evidence/12-test-v002.txt) [11](evidence/11-prod-v002-before.txt) [13](evidence/13-prod-v002.txt) [15](evidence/15-prod-v002-after.txt) |
| V003: `gold_region_pings` table | CLI deploy + run in dev, test and prod; the same commit then went through PR #1 and PR #2 with no-op CI deploys | [18](evidence/18-dev-v003.txt) [19](evidence/19-test-v003.txt) [20](evidence/20-prod-v003.txt) |
| V004: `in_geofence` column comment | CI only: push to `dev`, PR #3 `dev → test`, PR #4 `test → main`; each merge deployed and ran the pipeline | [22](evidence/22-v004-before.txt) [23](evidence/23-v004-after-ci.txt) |

In every case prod's table history shows the change made by the pipeline job, not by a person.

## CLI logs

| File | What it shows |
| ---- | ------------- |
| [00-cli-version](evidence/00-cli-version.txt) | Databricks CLI used for every deploy |
| [01-dev-deploy](evidence/01-dev-deploy.txt) | first `validate` + `deploy` of dev: schema, volumes and jobs created |
| [02](evidence/02-dev-simulator.txt), [03](evidence/03-dev-pipeline.txt) | simulator and pipeline runs in dev |
| [04-dev-checks](evidence/04-dev-checks.txt) | 20 files, 250 bronze rows, 210 silver + 20 quarantined (the 20 planted duplicates dropped), 20 trucks in gold joined with `truck_details` |
| [05-dev-rerun-no-new-data](evidence/05-dev-rerun-no-new-data.txt) | pipeline rerun with nothing new: counts unchanged, silver history still has a single MERGE |
| [06-test-v001](evidence/06-test-v001.txt), [07-prod-v001](evidence/07-prod-v001.txt) | test and prod at V001: deploy, simulator, pipeline |
| [14-dev-stop-restart](evidence/14-dev-stop-restart.txt) | run cancelled during silver after bronze took 10 new files; the rerun resumes from the checkpoint with one MERGE of 89 rows, and bronze 594 = silver 494 + quarantine 50 + 50 planted duplicates |
| [16-test](evidence/16-test-dashboard-deploy.txt), [17-prod](evidence/17-prod-dashboard-deploy.txt) | dashboard deployed to test and prod |
| [18](evidence/18-dev-v003.txt), [19](evidence/19-test-v003.txt), [20](evidence/20-prod-v003.txt) | V003 applied, and the window table's totals equal silver's row count: 698, 550, 407 |
| [21-dev-window-variable](evidence/21-dev-window-variable.txt) | window size changed with `--var region_window_minutes=10` only: the next run backfilled 10-minute windows next to the 5-minute ones, both totalling 698 |
| [22](evidence/22-v004-before.txt), [23](evidence/23-v004-after-ci.txt) | V004 before and after in every environment, with the CI run ids |

## Screenshots

| | |
| --- | --- |
| [01 jobs](screenshots/01-jobs.png) | all six jobs (pipeline and simulator for dev, test, prod), deployed by the bundle; test/prod schedules paused for the Free Edition quota |
| [02 prod pipeline](screenshots/02-prod-pipeline-tasks.png) | task graph, "Connected to Declarative Automation Bundles", schedule, job parameters |
| [03 prod runs](screenshots/03-prod-pipeline-runs.png) | run history of the prod pipeline |
| [04 V002 run](screenshots/04-prod-run-v002.png) | the prod run that applied V002 (before the second gold table existed) |
| [05 V004 run](screenshots/05-prod-run-v004-ci.png) | the prod run CI started after PR #4: migrate, seed/bronze, silver, both gold tables (runs started through the API by `bundle run` show as "Manually") |
| [06 migrations ledger](screenshots/06-prod-schema-migrations.png) | SQL editor: `_schema_migrations` in prod, V1 to V4 with checksums |
| [07 gold columns](screenshots/07-prod-gold-columns.png) | gold columns, including `in_geofence` and its V004 comment |
| [08 gold query](screenshots/08-prod-gold-query.png) | SQL editor: gold rows joined with `truck_details` |
| [09 dashboard](screenshots/09-prod-dashboard.png) | fleet overview dashboard in prod |
| [10 CI runs](screenshots/10-github-actions.png) | GitHub Actions: pull request checks and deploy-and-run on merge |
| [11 CLI deploy](screenshots/11-cli-deploy-all-targets.png) | `bundle validate` and `bundle deploy` from the CLI to dev, test and prod |
| [12 gold history](screenshots/12-prod-gold-history-query.png) | SQL editor: prod gold history, every schema change made by the pipeline job |
| [13 region windows](screenshots/13-prod-region-pings-query.png) | SQL editor: `gold_region_pings`, total pings equal silver's row count |
| [14 pull requests](screenshots/14-github-pull-requests.png) | pull requests #1 to #4 (V003 and V004 promotions), all merged |

![prod dashboard](screenshots/09-prod-dashboard.png)
