# DuckDB BigQuery Extension

This community extension allows [DuckDB](https://duckdb.org) to query data from Google BigQuery using a mix of BigQuery Storage (Read/Write) and REST API. It enables users to access, manage, and manipulate their BigQuery datasets/tables directly from DuckDB using standard SQL queries. Inspired by official DuckDB RDBMS extensions like [MySQL](https://duckdb.org/docs/extensions/mysql.html), [PostgreSQL](https://github.com/duckdb/postgres_scanner), and [SQLite](https://github.com/duckdb/sqlite_scanner), this extension offers a similar feel. See [Important Notes](#important-notes-on-using-google-bigquery) for disclaimers and usage information.

> This extension supports the following builds: `linux_amd64`, `linux_arm64`, `linux_amd64_musl`, `osx_amd64`, `osx_arm64`, and `windows_amd64`. The builds `wasm_mvp`, `wasm_eh`, `wasm_threads`, and `windows_amd64_mingw` are not supported.

## Preliminaries

### Authentication Option 1: Configure ADC with your Google Account

To authenticate using your Google Account, first install the [Google Cloud CLI (gcloud)](https://cloud.google.com/sdk/gcloud). Download the latest version from the [Google Cloud CLI installation page](https://cloud.google.com/sdk/docs/install) and follow the instructions to select and authenticate your Google Cloud project for using BigQuery.

After installation, run the following command to authenticate and follow the steps along:

```bash
gcloud auth application-default login
```

### Authentication Option 2: Configure ADC with Service Account Keys

Alternatively, you can authenticate using a service account. First, create a service account in the Google Cloud Console, assign the necessary roles, and download the JSON key file. Next, set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the file path. For example:

```bash
# On Linux or macOS
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/my/service-account-credentials.json"

# On Windows
set GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\my\service-account-credentials.json"
```

### Windows gRPC Configuration

On Windows, gRPC requires an additional environment variable to configure the trust store for SSL certificates. Download and configure it using (see [official documentation](https://github.com/googleapis/google-cloud-cpp/blob/f2bd9a9af590f58317a216627ae9e2399c245bab/google/cloud/storage/quickstart/README.md#windows)):

```bash
@powershell -NoProfile -ExecutionPolicy unrestricted -Command ^
    (new-object System.Net.WebClient).Downloadfile( ^
        'https://pki.google.com/roots.pem', 'roots.pem')
set GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=%cd%\roots.pem
```

This downloads the `roots.pem` file and sets the `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` environment variable to its location.

## Quickstart

The BigQuery extension can be installed from the official [Community Extension Repository](https://community-extensions.duckdb.org/), eliminating the need to enable the `unsigned` mode. Just use the following command to install and load the extension:

```sql
-- Install and load the DuckDB BigQuery extension from the Community Repository
FORCE INSTALL 'bigquery' FROM community;
LOAD 'bigquery';
```

> **Note**: Windows user require an additional step to configure the gRPC SSL certificates (see [here](#windows-grpc-configuration)).

After loading the extension, you can connect to your BigQuery project using the `ATTACH` statement. Replace `my_gcp_project` with the name of your actual Google Cloud Project. Here is an example:

```sql
-- Attach to your BigQuery Project
D ATTACH 'project=my_gcp_project' as bq (TYPE bigquery, READ_ONLY);

-- Show all tables in all datasets in the attached BigQuery project
D SHOW ALL TABLES;
┌──────────┬──────────────────┬──────────┬──────────────┬───────────────────┬───────────┐
│ database │      schema      │   name   │ column_names │   column_types    │ temporary │
│ varchar  │     varchar      │  varchar │  varchar[]   │     varchar[]     │  boolean  │
├──────────┼──────────────────┼──────────┼──────────────┼───────────────────┼───────────┤
│ bq       │ quacking_dataset │ duck_tbl │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
| bq       | barking_dataset  | dog_tbl  | [i, s]       | [BIGINT, VARCHAR] │ false     |
└──────────┴──────────────────┴──────────┴──────────────┴───────────────────┴───────────┘

-- Select data from a specific table in BigQuery
D SELECT * FROM bq.quacking_dataset.duck_tbl;
┌───────┬────────────────┐
│   i   │       s        │
│ int32 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

Depending on the number of schemas and tables, initializing the BigQuery catalog may take some time. However, once initialized, the tables are cached. To speed up this process, you also focus the loading process on a particular dataset by specifying a `dataset=` parameter as follows.

```sql
-- Attach to your BigQuery Project
D ATTACH 'project=my_gcp_project dataset=quacking_dataset' as bq (TYPE bigquery);

-- Show all tables in all datasets in the attached BigQuery project
D SHOW ALL TABLES;
┌──────────┬──────────────────┬──────────┬──────────────┬───────────────────┬───────────┐
│ database │      schema      │   name   │ column_names │   column_types    │ temporary │
│ varchar  │     varchar      │  varchar │  varchar[]   │     varchar[]     │  boolean  │
├──────────┼──────────────────┼──────────┼──────────────┼───────────────────┼───────────┤
│ bq       │ quacking_dataset │ duck_tbl │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
└──────────┴──────────────────┴──────────┴──────────────┴───────────────────┴───────────┘
```

When working with BigQuery, you may need to separate storage and compute across different GCP projects. You can achieve this by using the `billing_project` parameter with the `ATTACH` command:

```sql
-- Attach to a storage project while billing compute to a different project
D ATTACH 'project=my_storage_project billing_project=my_compute_project' AS bq (TYPE bigquery, READ_ONLY);

-- Query data from the storage project, billed to the compute project
D SELECT * FROM bq.quacking_dataset.duck_tbl WHERE i = 12;
┌───────┬────────────────┐
│   i   │       s        │
│ int32 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
└───────┴────────────────┘
```

In this configuration:

- `project=storage-project` specifies where your data is stored
- `billing_project=compute-project` specifies which project will be billed for query execution and compute resources

This approach allows you to maintain clear separation between data storage costs and compute costs across different GCP projects.

## Additional Operations and Settings

The following SQL statements provide a brief overview of supported functionalities and include examples for interacting with BigQuery:

```sql
ATTACH 'project=my_gcp_project' as bq (TYPE bigquery);

-- Create a BigQuery dataset
CREATE SCHEMA bq.some_dataset;

-- Create a BigQuery table in some dataset
CREATE TABLE bq.some_dataset.tbl(id INTEGER, some_string VARCHAR);

-- Insert values into the table 
INSERT INTO bq.some_dataset.tbl VALUES (42, "my quacking string");

-- Retrieves rows from the table
SELECT some_string FROM bq.some_dataset.tbl;

-- Drop a BigQury table in some dataset
DROP TABLE IF EXISTS bq.some_dataset.tbl;

-- Drop a BigQuery dataset
DROP SCHEMA IF EXISTS bq.some_dataset;

-- Altering tables - rename table
ALTER TABLE bq.some_dataset.tbl RENAME TO tbl_renamed;

-- Altering tables - rename column
ALTER TABLE bq.some_dataset.tbl RENAME COLUMN i TO i2;

-- Altering tables - add column
ALTER TABLE bq.some_dataset.tbl ADD COLUMN j INTEGER;

-- Altering tables - drop column
ALTER TABLE bq.some_dataset.tbl DROP COLUMN i;

-- Altering tables - change column type
ALTER TABLE bq.some_dataset.tbl ALTER COLUMN i TYPE DOUBLE;

-- Altering tables - drop not null condition
ALTER TABLE bq.some_dataset.tbl ALTER COLUMN i DROP NOT NULL;
```

### `bigquery_scan` Function

The `bigquery_scan` function provides direct, efficient reads from a single table within your BigQuery project. This function is ideal for simple reads where no complex SQL is required, and it supports simple projection pushdown from DuckDB.

If you would rather query just one table directly instead of attaching all tables, you can achieve this by directly using the `bigquery_scan` function, such as:

```sql
D SELECT * FROM bigquery_scan('my_gcp_project.quacking_dataset.duck_tbl');
┌───────┬────────────────┬──────────────────────────┐
│   i   │       s        │        timestamp         │
│ int32 │    varchar     │        timestamp         │
├───────┼────────────────┼──────────────────────────┤
│    12 │ quack 🦆       │ 2024-03-21 08:01:02 UTC  │
│    13 │ quack quack 🦆 │ 2024-05-19 10:25:44 UTC  │
└───────┴────────────────┴──────────────────────────┘
```

> By default, `bigquery_scan` now uses the optimized Arrow-based implementation (formerly `bigquery_arrow_scan`). You can set `use_legacy_scan=true` to use the legacy implementation if needed. For backwards compatibility, the separate `bigquery_arrow_scan` function is still available and now behaves identically to the default `bigquery_scan`.

The function supports filter pushdown by accepting [row restriction filter statements](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions) as an optional argument. These filters are passed directly to BigQuery and restrict which rows are transfered from the source table. For example:

```sql
D SELECT * FROM bigquery_scan('my_gcp_project.quacking_dataset.duck_tbl', filter='i=13 AND DATE(timestamp)=DATE(2023, 5, 19)'));
┌───────┬────────────────┬──────────────────────────┐
│   i   │       s        │        timestamp         │
│ int32 │    varchar     │        timestamp         │
├───────┼────────────────┼──────────────────────────┤
│    13 │ quack quack 🦆 │ 2024-05-19 10:25:44 UTC  │
└───────┴────────────────┴──────────────────────────┘
```

The filter syntax follows the same rules as the `row_restriction` field in BigQuery's Storage Read API.                                          |

While `bigquery_scan` offers high-speed data retrieval, it does not support reading from views or external tables due to limitations of the Storage Read API. For those cases, consider using the `bigquery_query` function, which allows more complex querying capabilities.

The `bigquery_scan` function supports the following named parameters:

| Parameter         | Type      | Description                                                                      |
| ----------------- | --------- | -------------------------------------------------------------------------------- |
| `filter`          | `VARCHAR` | Row restriction filter statements passed directly to BigQuery Storage Read API.  |
| `use_legacy_scan` | `BOOLEAN` | Use legacy scan implementation: `true` (legacy) or `false` (optimized, default). |
| `billing_project` | `VARCHAR` | Project ID to bill for query execution (useful for public datasets).             |
| `api_endpoint`    | `VARCHAR` | Custom BigQuery API endpoint URL.                                                |
| `grpc_endpoint`   | `VARCHAR` | Custom BigQuery Storage gRPC endpoint URL.                                       |

### `bigquery_query` Function

The `bigquery_query` function allows you to run custom [GoogleSQL](https://cloud.google.com/bigquery/docs/introduction-sql) read queries within your BigQuery project. Like `bigquery_scan`, it uses the optimized Arrow-based implementation by default, with the option to use `use_legacy_scan=true` for the legacy implementation if needed. This function is especially useful to get around the limitations of the BigQuery Storage Read API, such as reading from views or external tables.

```sql
D SELECT * FROM bigquery_query('my_gcp_project', 'SELECT * FROM `my_gcp_project.quacking_dataset.duck_tbl`');
┌───────┬────────────────┐
│   i   │       s        │
│ int32 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

> **Note**: If your goal is straightforward table reads, `bigquery_scan` is often more efficient, as it bypasses the SQL layer for direct data access. However, `bigquery_query` is ideal when you need to execute custom SQL that requires the full querying capabilities of BigQuery expressed in GoogleSQL. In this case, BigQuery transparently creates an anonymous temporary result table, which is fetched using the selected scan engine.

The `dry_run` parameter allows you to validate a query without executing it. This is useful for estimating query costs and checking syntax before running expensive queries:

```sql
D SELECT * FROM bigquery_query('my_gcp_project', 'SELECT * FROM `my_gcp_project.quacking_dataset.duck_tbl`', dry_run=true);
┌───────────────────────┬───────────┬──────────┐
│ total_bytes_processed │ cache_hit │ location │
│        int64          │  boolean  │ varchar  │
├───────────────────────┼───────────┼──────────┤
│                    54 │ false     │ US       │
└───────────────────────┴───────────┴──────────┘
```

The `bigquery_query` function supports the following named parameters:

| Parameter         | Type      | Description                                                                                                        |
| ----------------- | --------- | ------------------------------------------------------------------------------------------------------------------ |
| `use_legacy_scan` | `BOOLEAN` | Use legacy scan implementation: `true` (legacy) or `false` (optimized, default).                                   |
| `dry_run`         | `BOOLEAN` | When `true`, validates the query without executing it. Returns metadata: `total_bytes_processed`, `cache_hit`, and `location`. |
| `billing_project` | `VARCHAR` | Project ID to bill for query execution (useful for public datasets).                                               |
| `api_endpoint`    | `VARCHAR` | Custom BigQuery API endpoint URL.                                                                                  |
| `grpc_endpoint`   | `VARCHAR` | Custom BigQuery Storage gRPC endpoint URL.                                                                         |

### `bigquery_execute` Function

The `bigquery_execute` function runs arbitrary GoogleSQL queries directly in BigQuery. These queries are executed without interpretation by DuckDB. The call is synchronous and returns a result with details about the query execution, like the following.

```sql
D ATTACH 'project=my_gcp_project' as bq (TYPE bigquery);
D CALL bigquery_execute('bq', '
    CREATE SCHEMA deluxe_dataset
    OPTIONS(
        location="us",
        default_table_expiration_days=3.75,
        labels=[("label1","value1"),("label2","value2")]
    )
');
┌─────────┬──────────────────────────────────┬─────────────────┬──────────┬────────────┬───────────────────────┬───────────────────────┐
│ success │             job_id               │    project_id   │ location │ total_rows │ total_bytes_processed │ num_dml_affected_rows │
│ boolean │             varchar              │     varchar     │ varchar  │   uint64   │         int64         │        varchar        │
├─────────┼──────────────────────────────────┼─────────────────┼──────────┼────────────┼───────────────────────┼───────────────────────┤
│ true    │ job_-Xu_D2wxe2Xjh-ArZNwZ6gut5ggi │ my_gcp_project  │ US       │          0 │                     0 │ 0                     │
└─────────┴──────────────────────────────────┴─────────────────┴──────────┴────────────┴───────────────────────┴───────────────────────┘
```

Similar to `bigquery_query`, the `dry_run` parameter allows you to validate queries without executing them:

```sql
D CALL bigquery_execute('my_gcp_project', 'SELECT * FROM `my_gcp_project.quacking_dataset.duck_tbl`', dry_run=true);
┌───────────────────────┬───────────┬──────────┐
│ total_bytes_processed │ cache_hit │ location │
│        int64          │  boolean  │ varchar  │
├───────────────────────┼───────────┼──────────┤
│                    54 │ false     │ US       │
└───────────────────────┴───────────┴──────────┘
```

The `bigquery_execute` function supports the following named parameters:

| Parameter       | Type      | Description                                                                                                        |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------ |
| `dry_run`       | `BOOLEAN` | When `true`, validates the query without executing it. Returns metadata: `total_bytes_processed`, `cache_hit`, and `location`. |
| `api_endpoint`  | `VARCHAR` | Custom BigQuery API endpoint URL.                                                                                  |
| `grpc_endpoint` | `VARCHAR` | Custom BigQuery Storage gRPC endpoint URL.                                                                         |

### `bigquery_jobs` Function

The `bigquery_jobs` fucntion retrieves a list of jobs within the specified project. It displays job metadata such as
job state, start and end time, configuration, statistics, and many more. More information on the job information can be found [here](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list).

```sql
D ATTACH 'project=my_gcp_project' as bq (TYPE bigquery);
D SELECT * FROM bigquery_jobs('bq', maxResults=2);
┌───────────┬──────────────────────┬───────────┬───┬──────────────────────┬──────────────────┐
│   state   │        job_id        │  project  │ … │    configuration     │      status      │
│  varchar  │       varchar        │  varchar  │   │         json         │       json       │
├───────────┼──────────────────────┼───────────┼───┼──────────────────────┼──────────────────┤
│ Completed │ job_zAAv42SdMT51qk…  │ my_gcp_p… │ … │ {"query":{"query":…  │ {"state":"DONE"} │
│ Completed │ job_ro2WURJlGlkXCC…  │ my_gcp_p… │ … │ {"query":{"query":…  │ {"state":"DONE"} │
├───────────┴──────────────────────┴───────────┴───┴──────────────────────┴──────────────────┤
│ 2 rows                                                                16 columns (5 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

The operation supports the following additional named parameters as query arguments:

| Parameter         | Type        | Description                                                                                      |
| ----------------- | ----------- | ------------------------------------------------------------------------------------------------ |
| `jobId`           | `VARCHAR`   | Filters results by job ID. Returns only the matching job, ignoring all other arguments.          |
| `allUsers`        | `BOOLEAN`   | If true, returns jobs for all users in the project. Default is false (only current user's jobs). |
| `maxResults`      | `INTEGER`   | Limits the number of jobs returned.                                                              |
| `minCreationTime` | `TIMESTAMP` | Filters jobs created after the specified time (in milliseconds since the epoch).                 |
| `maxCreationTime` | `TIMESTAMP` | Filters jobs created before the specified time (in milliseconds since the epoch).                |
| `stateFilter`     | `VARCHAR`   | Filters jobs by state (e.g., `PENDING`, `RUNNING`,`DONE`).                                       |
| `parentJobId`     | `VARCHAR`   | Filters results to only include child jobs of the specified parent job ID.                       |

### `bigquery_clear_cache` Function

DuckDB caches schema metadata, such as datasets and table structures, to avoid repeated fetches from BigQuery. If the schema changes externally, use `bigquery_clear_cache` to update the cache and retrieve the latest schema information:

```sql
D CALL bigquery_clear_cache();
```

### Reading Public Datasets

Public datasets can be accessed by specifying your project as a `billing_project`. All queries will then be executed and billed on that project. This works for functions such as `bigquery_scan`, `bigquery_execute`, and the `ATTACH` command.

```sql
D SELECT * FROM bigquery_scan('bigquery-public-data.geo_us_boundaries.cnecta', billing_project='my_gcp_project');
┌─────────┬──────────────────┬──────────────────────┬───┬───────────────────┬───────────────┬───────────────┬──────────────────────┐
│ geo_id  │ cnecta_fips_code │         name         │ … │ area_water_meters │ int_point_lat │ int_point_lon │     cnecta_geom      │
│ varchar │     varchar      │       varchar        │   │       int64       │    double     │    double     │       varchar        │
├─────────┼──────────────────┼──────────────────────┼───┼───────────────────┼───────────────┼───────────────┼──────────────────────┤
│ 710     │ 710              │ Augusta-Waterville…  │ … │         183936850 │    44.4092939 │   -69.6901717 │ POLYGON((-69.79281…  │
│ 775     │ 775              │ Portland-Lewiston-…  │ … │        1537827560 │    43.8562034 │   -70.3192682 │ POLYGON((-70.48007…  │
│ 770     │ 770              │ Pittsfield-North A…  │ … │          24514153 │    42.5337519 │   -73.1678825 │ POLYGON((-73.30698…  │
│ 790     │ 790              │ Springfield-Hartfo…  │ … │         256922043 │    42.0359069 │   -72.6213616 │ POLYGON((-72.63682…  │
│ 715     │ 715              │ Boston-Worcester-P…  │ … │        3004814151 │    42.3307869 │   -71.3296644 │ MULTIPOLYGON(((-71…  │
│ 725     │ 725              │ Lebanon-Claremont,…  │ … │          58476158 │    43.6727226 │   -72.2484543 │ POLYGON((-72.39601…  │
│ 720     │ 720              │ Bridgeport-New Hav…  │ … │         374068423 │    41.3603421 │   -73.1284227 │ MULTIPOLYGON(((-72…  │
├─────────┴──────────────────┴──────────────────────┴───┴───────────────────┴───────────────┴───────────────┴──────────────────────┤
│ 7 rows                                                                                                      11 columns (7 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Working with Geometries

The BigQuery extension supports geospatial data with automatic conversion between DuckDB `GEOMETRY` and BigQuery [`GEOGRAPHY`](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#geography_type) types. By default, BigQuery `GEOGRAPHY` columns are read as `VARCHAR` (WKT format). With the [spatial extension](https://duckdb.org/docs/stable/core_extensions/spatial/overview.html) installed and loaded and the `bq_geography_as_geometry` setting enabled, they can be read as native DuckDB `GEOMETRY` types:

```sql
-- Load spatial extension and enable GEOMETRY support (BEFORE ATTACH)
D INSTALL spatial; LOAD spatial;
D SET bq_geography_as_geometry = true;
D ATTACH 'project=my_gcp_project' AS bq (TYPE bigquery);

-- Read GEOGRAPHY columns as native GEOMETRY
D SELECT name, geography_column FROM bq.dataset.geo_table;
┌──────────────┬──────────────────────────────────────────┐
│     name     │            geography_column              │
│   varchar    │                geometry                  │
├──────────────┼──────────────────────────────────────────┤
│ Location A   │ POINT(-122.4194 37.7749)                 │
│ Location B   │ POLYGON((-122.5 37.7, -122.3 37.8, ...)) │
└──────────────┴──────────────────────────────────────────┘

-- Write GEOMETRY data - automatically converted to BigQuery GEOGRAPHY
D INSERT INTO bq.dataset.geo_table VALUES 
    ('New Point', ST_Point(-122.2, 37.8)),
    ('Buffer Zone', ST_Buffer(ST_Point(-122.0, 37.9), 0.01));
```

> **⚠️ Spatial Extension Loading Order**: The spatial extension must be installed and loaded **before** setting `bq_geography_as_geometry=true` and **before** using `ATTACH`. Otherwise, the internal catalog will be configured for `VARCHAR` types and geometry conversion will not work properly.


### Additional Extension Settings

| Setting                                   | Description                                                                                                                                                                                        | Default |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `bq_bignumeric_as_varchar`                | Read BigQuery `BIGNUMERIC` columns as `VARCHAR` instead of causing a type mapping error. **Note: Only supported with legacy scan.**                                                                | `false` |
| `bq_use_legacy_scan`                      | Use legacy scan implementation instead of optimized Arrow-based scan. Set to `true` for the old scan or `false` for the new optimized implementation.                                              | `false` |
| `bq_geography_as_geometry`                | Return BigQuery `GEOGRAPHY` columns as DuckDB `GEOMETRY` types (requires spatial extension). When `false`, returns WKT strings as `VARCHAR`.                                                       | `false` |
| `bq_query_timeout_ms`                     | Timeout for BigQuery queries in milliseconds. If a query exceeds this time, the operation stops waiting.                                                                                           | `90000` |
| `bq_debug_show_queries`                   | [DEBUG] - whether to print all queries sent to BigQuery to stdout                                                                                                                                  | `false` |
| `bq_experimental_filter_pushdown`         | [EXPERIMENTAL] - Whether or not to use filter pushdown                                                                                                                                             | `true`  |
| `bq_experimental_use_info_schema`         | [EXPERIMENTAL] - Use information schema to fetch catalog info (often faster than REST API)                                                                                                         | `true`  |
| `bq_experimental_enable_bigquery_options` | [EXPERIMENTAL] - Whether to enable BigQuery OPTIONS in CREATE statements                                                                                                                           | `false` |
| `bq_curl_ca_bundle_path`                  | Path to the CA certificates used by cURL for SSL certificate verification                                                                                                                          |         |
| `bq_max_read_streams`                     | Maximum number of read streams for BigQuery Storage Read. Set to 0 to automatically match the number of DuckDB threads. Requires `SET preserve_insertion_order=FALSE` for parallelization to work. | `0`     |
| `bq_arrow_compression`                    | Compression codec for BigQuery Storage Read API. Options: `UNSPECIFIED`, `LZ4_FRAME`, `ZSTD`                                                                                                       | `ZSTD`  |

## Limitations

There are some limitations that arise from the combination of DuckDB and BigQuery. These include:

* **Reading from Views**: This DuckDB extension utilizes the BigQuery Storage Read API to optimize reading results. However, this approach has limitations (see [here](https://cloud.google.com/bigquery/docs/reference/storage#limitations) for more information). First, the Storage Read API does not support direct reading from logical or materialized views. Second, reading external tables is not supported. To mitigate these limitations, you can use the `bigquery_query` function to execute the query directly in BigQuery.

* **Propagation Delay**: After creating a table in BigQuery, there might be a brief propagation delay before the table becomes fully "visible". Therefore, be aware of potential delays when executing `CREATE TABLE ... AS` or `CREATE OR REPLACE TABLE ...` statements followed by immediate inserts. This delay is usually just a matter of seconds, but in rare cases, it can take up to a minute.

* **BIGNUMERIC Type Support**: The `bq_bignumeric_as_varchar` setting is only supported with the legacy scan implementation. If you need to read BIGNUMERIC columns as VARCHAR, ensure you use `use_legacy_scan=true` in scan functions or set `bq_use_legacy_scan=true` globally. The optimized Arrow-based scan does not currently support this conversion.

* **Primary Keys and Foreign Keys**: While BigQuery recently introduced the concept of primary keys and foreign keys constraints, they differ from what you're accustomed to in DuckDB or other traditional RDBMS. Therefore, this extension does not support this concept.

## Install Latest Updates from Custom Repository

Updates may not always be immediately available in the Community Extension repository. However, they can be obtained from a custom repository. To get the latest updates, start DuckDB with [unsigned extensions](https://duckdb.org/docs/extensions/overview.html#unsigned-extensions) setting enabled. Use the `allow_unsigned_extensions` flag for client connections, or start the CLI with `-unsigned` as follows:

```bash
# Example: CLI
duckdb -unsigned

# Example: Python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

Then set the custom repository and install the extension:

```sql
-- Set the custom repository, then install and load the DuckDB BigQuery extension
D SET custom_extension_repository = 'http://storage.googleapis.com/hafenkran';
D FORCE INSTALL 'bigquery';
D LOAD 'bigquery';
```

## Building the Project

This extension uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://github.com/microsoft/vcpkg?tab=readme-ov-file#quick-start-unix) or just run the following:

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Now to build the extension, run:

```bash
make
```

The main binaries that will be built are:

```bash
# the binary for the duckdb shell with the extension code automatically loaded.
./build/release/duckdb

# the test runner of duckdb. Again, the extension is already linked into the binary.
./build/release/test/unittest

# the loadable extension binary as it would be distributed.
./build/release/extension/bigquery/bigquery.duckdb_extension
```

After this step you can either simply start the shell with `./build/release/duckdb` or by installing/loading the extension from inside your duckdb instance with:

```sql
INSTALL './build/release/extension/bigquery/bigquery.duckdb_extension'
LOAD 'bigquery'
```

Now you can use the features from this extension.

## Using Docker

You can also build the project using Docker. This approach simplifies dependencies management and setup. You can build the docker image by using the provided `make` command as follows:

```bash
make docker-build
```

To run the Docker container, you'll need to authenticate with Google Cloud using a service account. Ensure you have a service account JSON file for Google Cloud authentication. The service account credentials must be set up as environment variables and mounted to the container. Use the following command to run the Docker container, replacing `/path/to/my/service-account-credentials.json` with the actual path to your service account JSON file:

```bash
docker run \
    -it \
    -v /path/to/my/service-account-credentials.json:/creds \
    -e GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account-credentials.json \
    duckdb-bigquery:v1.4.1
```

## Important Notes on Using Google BigQuery

> **⚠️ Disclaimer**: This is an independent, community-maintained open-source project and is not affiliated with, endorsed by, or officially supported by Google LLC, or any of their subsidiaries. This extension is provided "as is" without any warranties or guarantees. "DuckDB" and "BigQuery" are trademarks of their respective owners. Users are solely responsible for compliance with applicable terms of service and any costs incurred through usage.

When using this software with Google BigQuery, please ensure your usage complies with the [Google API Terms of Service](https://developers.google.com/terms). Be mindful of the usage limits and quotas, and adhere to Google's Fair Use Policy.

### Billing and Costs

Please be aware that using Google BigQuery through this software can incur costs. Google BigQuery is a paid service, and charges may apply based on the amount of data processed, stored, and the number of queries executed. Users are responsible for any costs associated with their use of Google BigQuery. For detailed information on BigQuery pricing, please refer to the [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing) page. It is recommended to monitor your usage and set up budget alerts in the Google Cloud Console to avoid unexpected charges.

By using this software, you acknowledge and agree that you are solely responsible for any charges incurred from Google BigQuery.
