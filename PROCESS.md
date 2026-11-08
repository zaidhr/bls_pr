# Process

## Architecture

### Bronze / silver / gold modeling

#### Bronze

- The Bronze Layer was structured so as to retain as much of the raw data structure as possible. Starting with the volumes, one volume **bronze_pr.pr_raw** is dedicated simply as a landing zone for ingested files. As it is, all new files land in the volume, old files remain (even if deleted at the source), and changed files are overwritten. Another volume, **bronze_pr.pr_ingestion_state**, stores the manifest.json, which keeps track of all processed files, when they were processed, and if they have changed. This aids the ingestion pipeline in determining which files to copy or ignore from the source.
- Once the data lands in the **bronze_pr.pr_raw** volume, the declarative pipeline picks up the data from there. All data within the volume, both BLS and DataUSA data, is read and converted into a table per file, named based on the data source. Minimal transformations are applied to the data besides deriving metadata columns such as _ingested_at, _source_file, _rescued_data, _source_file_modified_at. All data types are inferred as strings at this point.

#### Silver

- In the Silver Layer, the data undergoes standardization via data type enforcement and data quality expectations. Two such expectations are enforced: non-null key constraints, unique key constraints, and that expected source columns are coming through.
- The Silver Pipeline also trims values, converts blanks into nulls, and keeps the data from the latest version of the source file based on the metadata from the Bronze Layer.

#### Gold

- In the Gold Layer, the BLS data is separated into to fact tables that capture either the annual data (Q05) or the quarterly data (Q01 - Q04). This allows joins from both fact tables for business aggregates against either level of granularity. It also filters out unmatched series (no opportunity for enrichment), standardizes the date key **dim_date**, and enriches the data further with the **value_type** column and new geography columns.

### PySpark over SQL for analytics

- Honeslty, I chose PySpark over SQL for analytics because I simply enjoy PySpark more. Spark SQL does actually allow for some simpler syntax for certain operations, such as window functions. 

### Safe re-ingestion

- There are a few ways re-ingestion was handled safely in the ingestion notebook. Firstly, the manifest keeps track of already ingested files by recording the fingerprint (last modified date + content length) and SHA256 hash. The notebook then skips any unchanged files based on the manifest state. If the incoming fingerprint differs from that in the manifest, the file is downloaded and hashed. Then, if the hash matches, the file is skipped at that point. On top of these checks, the process begins downloading a temp file, and only after the full file has completed is it moved to the Bronze Layer volume. All the download history is stored in the manifest in order to maintain enforce only-once ingestion.

## Trade-offs

- For a real client, there are definitely a few things I would do differently. 

- First, I would probably separate a few pipelines. Currently, the DataUSA and BLS data are ingested in the same pipeline, since I know that they are the only data sources right now. Assuming we were pulling more data in from each source, and depending on the recency SLA's we would have to meet, it would be smart to have two separate pipelines, optimized per source, going to the Bronze Layer as needed. As of right now, they work fine.

- Next, I would implement some level of controlled schema evolution in the Auto Loader ingestion portion of the Bronze Layer definitions in the declarative pipeline. If we expect the data, columns, or general structure to change (as can often be expected of API data), then schema evolution configuration will help to at the very least control the behavior towards 'acceptable' and 'unacceptable' drift in the schema.

- In terms of cost, I would very much optimize around our expected update schedule and downstream usage. For the sake of simplicity, I'm using Serverless Compute for this job today. In the future, since we may not expect the data to hugely vary in size and structure, I can definitely set up some cluster policies to create appropriately sized cluster variants depending on the data load. Furthermore, depending on our downstream business use cases, such as reporting, I would create some views to service reports and potential reverse ETL use cases. Yet again, depending on SLA's, these may need to be materialized views rather than logical views.

- In terms of access control, there needs to be groups, service principals, etc. that allow users to be added to, for example, BUSINESS-ANALYSTS, who only have read access to gold tables, for example. This same access level needs to be implemented in views as they're created, using commands like SQL SECURITY INVOKER. Only service principals should have access to run jobs, different ones can deploy resources, etc. Then, if there's any PHI or PII, we can use governed tags to mask the data.

- Assuming a larger platform with more vast and various data streams, I would probably lean towards implementing DQX over injecting the expectations in the declarative pipelines. More complex data that requires more nuanced transformations would benefit from leaning towards jobs/notebooks over pipelines, and DQX can sit right inside those notebooks. On top of that, it can profile your tables for potential data/schema drift and recommend data quality expectations to enforce.

## Retrospective

### Hardest to get right

- Honestly, I can break the hardest part of the process into two main components: the raw file ingestion and the standardization.

- Raw file ingestion is always a tedious build because you have to account for disconnections, error handling, partial writes and chunking, and similar operations. It just means you have to be thorough with the logic and test extensively, but it does take more time than you would want.

- I spent the most time just standardizing (config setup, naming conventions, medallion structure, automation bundle setup). Building a working solution is easy, but standardizing it according to the needs of the business isn't always an exact science. I did the best with the time and context I had, but would love to see it in a fully operating production environment so I can iterate on the design and orient the platform towards business efficiency.