# Process

## Architecture

### Bronze / silver / gold modeling

#### Bronze

- The Bronze Layer was structured so as to retain as much of the raw data structure as possible. Starting with the volumes, one volume **bronze_pr.pr_raw** is dedicated simply as a landing zone for ingested files. As it is, all new files land in the volume, old files remain (even if deleted at the source), and changed files are overwritten. Another volume, **bronze_pr.pr_ingestion_state**, stores the manifest.json, which keeps track of all processed files, when they were processed, and if they have changed. This aids the ingestion notebook in determining which files to copy or ignore from the source.
- Once the data lands in the **bronze_pr.pr_raw** volume, the declarative pipeline picks up the data from there. Every tab-delimited file (BLS) and every *.json file (DataUSA) in the volume is discovered dynamically and converted into a streaming table per file, named after the file (e.g. pr.data.0.Current -> pr_data_0_current); the narrative text files (pr.txt, pr.contacts) are skipped. Minimal transformations are applied besides normalizing header names (BLS headers contain padding spaces) and deriving metadata columns such as _ingested_at, _source_file, _source_file_modified_at, plus Auto Loader's _rescued_data for the BLS files. All BLS values are kept as raw strings, and each DataUSA file is stored unparsed as a single raw_json text column. Because the sync overwrites changed files in place, Auto Loader runs with cloudFiles.allowOverwrites so every republished version is appended once, keeping the full history in bronze.

#### Silver

- In the Silver Layer, the data undergoes standardization via data type enforcement and data quality expectations. Three checks are enforced: non-null keys (declared NOT NULL in each table schema, plus fail-level expectations), one row per key (enforced by de-duplication rather than a constraint), and that exactly the expected source columns are coming through (a schema-drift check that fails the update; for the DataUSA file, a private check table compares its declared columns). A few non-critical rules (e.g. missing values, unexpected periods) are warn-only expectations. Each silver table has an explicitly declared column list and types.
- The Silver Pipeline also trims values, converts blanks into nulls, and keeps the data from the latest version of the source file based on the metadata from the Bronze Layer. The two BLS data files (pr.data.0.Current, a subset of pr.data.1.AllData) are combined into one de-duplicated pr_data table.

#### Gold

- In the Gold Layer, the BLS data is separated into two fact tables that capture either the annual data (Q05) or the quarterly data (Q01 - Q04). Keeping the grains apart prevents silently wrong aggregates: Q05 is published by BLS rather than being the average of the quarters, and 54 series only exist annually. Both facts join to the same dimensions, and the descriptive codes are copied onto each fact row (a star rather than a snowflake). Instead of filtering out unmatched series, the update fails if a fact row has no matching series. It also standardizes the date key in a conformed **dim_date** (yyyyQ for quarters, yyyy0 for years, shared with the population fact), and enriches the data further with the **value_type** column (dim_duration), **parent_sector_code** (dim_sector), an **is_revised** flag, and generalized geography columns for the population star.

### PySpark over SQL for analytics

- Honestly, I chose PySpark over SQL for analytics because I simply enjoy PySpark more. Spark SQL does actually allow for some simpler syntax for certain operations, such as window functions. A SQL version of the analytics notebook is kept alongside it, and both return identical results.

### Safe re-ingestion

- There are a few ways re-ingestion was handled safely in the ingestion notebook. Firstly, the manifest keeps track of already ingested files by recording the fingerprint (last modified date + content length) and SHA256 hash. The notebook then skips any unchanged files based on the manifest state. If the incoming fingerprint differs from that in the manifest, the file is downloaded and hashed. Then, if the hash matches, the file is skipped at that point. On top of these checks, the process downloads to a local temp file, verifies its size against the source, copies it to a hidden .partial file in the volume, and only then renames it to the real file name, so a failed run never leaves a half-written file. The manifest stores the latest synced state of every file and is saved after each file, so a crashed run resumes without re-downloading what was already done, which keeps ingestion once-only.
- Further safeguards: each file is processed in its own try/except so one failure doesn't stop the others (the run still fails at the end), an empty source listing aborts the run before anything changes, HTTP requests retry with backoff, files removed at the source are never deleted, and the job allows only one concurrent run. Downstream, bronze appends each changed file version once, silver keeps only the latest version, and the job refreshes the pipeline without a full refresh.

## Trade-offs

- For a real client, there are definitely a few things I would do differently. 

- First, I would probably separate a few pipelines. Currently, the DataUSA and BLS data are ingested by the same notebook and processed by the same pipeline, since I know that they are the only data sources right now. Assuming we were pulling more data in from each source, and depending on the recency SLA's we would have to meet, it would be smart to have two separate pipelines, optimized per source, going to the Bronze Layer as needed. As of right now, they work fine.

- Next, I would implement some level of controlled schema evolution in the Auto Loader ingestion portion of the Bronze Layer definitions in the declarative pipeline. Today bronze uses Auto Loader's default behavior (new columns are added automatically), and silver fails the update on any column change, which is safe but requires a manual code change each time. If we expect the data, columns, or general structure to change (as can often be expected of API data), then schema evolution configuration will help to at the very least control the behavior towards 'acceptable' and 'unacceptable' drift in the schema.

- In terms of data volume, the current files are small (the largest is about 4 MB), so the silver and gold materialized views can simply be recomputed on each run. At larger volumes I would move silver to incremental processing (e.g. Auto CDC from snapshot instead of re-reading the latest file version), use file-event-based Auto Loader discovery, and tune clustering keys on the facts.

- In terms of cost, I would very much optimize around our expected update schedule and downstream usage. For the sake of simplicity, I'm using Serverless Compute for this job today. In the future, since we may not expect the data to hugely vary in size and structure, I can definitely set up some cluster policies to create appropriately sized cluster variants depending on the data load. Furthermore, depending on our downstream business use cases, such as reporting, I would create some views to service reports and potential reverse ETL use cases. Yet again, depending on SLA's, these may need to be materialized views rather than logical views.

- In terms of access control, there needs to be groups, service principals, etc. that allow users to be added to, for example, BUSINESS-ANALYSTS, who only have read access to gold tables, for example. This same access level needs to be implemented in views as they're created. Databricks views read their underlying tables with the view owner's permissions, so per-user restrictions need dynamic views (e.g. is_account_group_member()) or row filters and column masks. Only service principals should have access to run jobs, different ones can deploy resources, etc. Then, if there's any PHI or PII, we can use governed tags with attribute-based (ABAC) masking policies to mask the data.

- Assuming a larger platform with more vast and various data streams, I would probably lean towards implementing DQX over injecting the expectations in the declarative pipelines. More complex data that requires more nuanced transformations would benefit from leaning towards jobs/notebooks over pipelines, and DQX can sit right inside those notebooks. On top of that, it can profile your tables for potential data/schema drift and recommend data quality expectations to enforce.

- In terms of monitoring, today there is only a job failure email and the pipeline's expectation metrics. I would add alerts on the pipeline event log (failed updates, expectation violations), freshness checks on the bronze and gold tables, and a simple check on the sync manifest (e.g. no file updated within the expected BLS release window).

## Retrospective

### Hardest to get right

- Honestly, I can break the hardest part of the process into two main components: the raw file ingestion and the standardization.

- Raw file ingestion is always a tedious build because you have to account for disconnections, error handling, partial writes and chunking, and similar operations. It just means you have to be thorough with the logic and test extensively, but it does take more time than you would want.

- I spent the most time just standardizing (config setup, naming conventions, medallion structure, automation bundle setup). Building a working solution is easy, but standardizing it according to the needs of the business isn't always an exact science. I did the best with the time and context I had, but would love to see it in a fully operating production environment so I can iterate on the design and orient the platform towards business efficiency.