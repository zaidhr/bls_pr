# Declarative Automation Bundle - implementation plan

Goal: deploy the ingestion job and the medallion pipeline from this repository to a single
`prod` target with one command, with no environment-specific values in code.

Draft files already in the repo: `databricks.yml`, `resources/bls_pr_medallion.pipeline.yml`,
`resources/bls_pr_daily.job.yml`. The `prod` target passes `databricks bundle validate --strict`;
nothing has been deployed yet.

## Step 1 - Put the repository under Git

* Create an empty remote repository (e.g. GitHub) and clone it into the workspace as a Git folder.
* Move the contents of this folder into the Git folder, keeping the layout.
* Repoint the pipeline's `root_path` and `libraries` glob to the new location (or skip this once
  step 6 hands the pipeline over to the bundle).
* First commit: everything except what `.gitignore` excludes.

The bundle already validates from this plain folder, but it should be deployed from a Git folder
so every deployment maps to a commit.

## Step 2 - Environment

One target only (a dev target was dropped as unnecessary for this project):

| Target | Mode | Catalog | Who deploys | Schedule |
| --- | --- | --- | --- | --- |
| `prod` | production | `productivity` | CI or a release owner | unpaused after first verified run |

* Keep the existing schemas and volumes (`bronze_pr`, `silver_pr`, `gold_pr`, `pr_raw`,
  `pr_ingestion_state`) rather than bundle-managing them; they already hold data.

## Step 3 - Identity and permissions

* Create a service principal for prod and set `run_as.service_principal_name` in the `prod` target.
* Grant it: `USE CATALOG` on `productivity`; `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`
  on the three schemas; `READ VOLUME` / `WRITE VOLUME` on `pr_raw` and `pr_ingestion_state`.
* Note: changing `run_as` makes the service principal the owner of the pipeline's tables.
* Add `permissions` to the job and pipeline (e.g. `CAN_VIEW` for the team group, `CAN_MANAGE` for owners).
* Replace the personal email in `notification_email` and in `config/bls_pr_ingestion.yaml`
  (`http.user_agent`) with a team mailbox.

## Step 4 - Wire the ingestion notebook to configuration (DONE, except the last bullet)

* The notebook reads `config/bls_pr_ingestion.yaml` from a fixed relative path.
* Catalog, schema and raw volume are read from the variable defaults in `databricks.yml`. The
  notebook takes no parameters, so keep the prod target's values equal to the defaults.
* Optional: remove the `CREATE SCHEMA` / `CREATE VOLUME` cell, since the objects already exist.

## Step 5 - Validate

* `databricks bundle validate --strict --target prod`

## Step 6 - Hand over the existing pipeline and deploy

The pipeline `6239ee2d-6e46-47fe-8744-235df5400abb` already owns the prod tables. Bind it instead
of letting the bundle create a duplicate:

* `databricks bundle deployment bind bls_pr_medallion 6239ee2d-6e46-47fe-8744-235df5400abb --target prod`
* `databricks bundle deploy --target prod`, then run a pipeline dry run and one update.
* Run the job once; confirm the volume sync summary, the pipeline update, and row counts
  (bronze per-file counts = file lines, silver `pr_data` = distinct keys, gold facts = silver Q01-Q04 / Q05).
* Unpause the `bls_pr_daily` schedule after the first good run.

## Step 7 - CI/CD (optional next phase)

* On pull request: `bundle validate --strict --target prod`.
* On merge to `main`: `bundle deploy --target prod` using the service principal's OAuth credentials.
* Optional: unit tests for `utilities/pr_files.py` and the sync helpers.

## Open decisions

* Service principal for prod.
* Team mailbox for alerts and the BLS User-Agent.
* Schedule (draft: 07:00 America/New_York daily; BLS PR data changes only on release days).
* Whether the analytics notebook should become a scheduled job or a dashboard.
