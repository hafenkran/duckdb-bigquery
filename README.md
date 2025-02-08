# DuckDB BigQuery Extension

This repository contains the [DuckDB](https://duckdb.org) BigQuery Extension. This extension lets DuckDB integrate with Google BigQuery, allowing users to directly access, manage, and manipulate their BigQuery datasets/tables using standard SQL statements. Inspired by official DuckDB RDBMS extensions like [MySQL](https://duckdb.org/docs/extensions/mysql.html), [PostgreSQL](https://github.com/duckdb/postgres_scanner), and [SQLite](https://github.com/duckdb/sqlite_scanner), this extension offers a similar feel.

> This extension only supports the following builds: `linux_amd64`, `linux_amd64_gcc4`, `linux_amd64_musl`, `osx_arm64`, and `windows_amd64`.

## Preliminaries

### Authentication Option 1: Configure ADC with your Google Account

To authenticate using your Google Account, first install the [Google Cloud CLI (gcloud)](https://cloud.google.com/sdk/gcloud). Download the latest version from the [Google Cloud CLI installation page](https://cloud.google.com/sdk/docs/install) and follow the instructions to select and authenticate your Google Cloud project for using BigQuery.

After installation, run the following command to authenticate and follow the steps along:

```bash
gcloud auth application-default login
```

### Authentication Option 2: Configure Service account keys

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

> Note: Windows user require an additional step to configure the gRPC SSL certificates (see [here](#windows-grpc-configuration)).

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
If you would rather query just one table directly instead of attaching all tables, you can achieve this by directly using the `bigquery_scan` functions, such as:

```sql
D SELECT * FROM bigquery_scan('my_gcp_project.quacking_dataset.duck_tbl');
┌───────┬────────────────┐
│   i   │       s        │
│ int32 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

While `bigquery_scan` offers high-speed data retrieval, it does not support reading from views or external tables due to limitations of the Storage Read API. For those cases, consider using the `bigquery_query` function, which allows more complex querying capabilities.

### `bigquery_query` Function

The `bigquery_query` function allows you to run custom [GoogleSQL](https://cloud.google.com/bigquery/docs/introduction-sql) read queries within your BigQuery project. This function is also especially useful to get around the limitations of the BigQuery Storage Read API, such as reading from views or external tables.

> **Note**: If your goal is straightforward table reads, `bigquery_scan` is often more efficient, as it bypasses the SQL layer for direct data access. However, `bigquery_query` is ideal when you need to execute custom SQL that requires the full querying capabilities of BigQuery expressed in GoogleSQL. In this case, BigQuery transparently creates an anonymous temporary result table, which is fetched in exactly the same way as with `bigquery_scan`.

```sql
D SELECT * FROM bigquery_query('my_gcp_project', 'SELECT * FROM `my_gcp_project.quacking_dataset.duck_tbl`');
```

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

### Additional Extension Settings

| Setting                           | Description                                                                                              | Default |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- | ------- |
| `bq_query_timeout_ms`             | Timeout for BigQuery queries in milliseconds. If a query exceeds this time, the operation stops waiting. | `90000` |
| `bq_debug_show_queries`           | [DEBUG] - whether to print all queries sent to BigQuery to stdout                                        | `false` |
| `bq_experimental_filter_pushdown` | [EXPERIMENTAL] - Whether or not to use filter pushdown                                                   | `true`  |
| `bq_experimental_use_info_schema` | [EXPERIMENTAL] - Use information schema to fetch catalog info (often faster than REST API)               | `true`  |
| `bq_curl_ca_bundle_path`          | Path to the CA certificates used by cURL for SSL certificate verification                                |         |

## Limitations

There are some limitations that arise from the combination of DuckDB and BigQuery. These include:

* **Reading from Views**: This DuckDB extension utilizes the BigQuery Storage Read API to optimize reading results. However, this approach has limitations (see [here](https://cloud.google.com/bigquery/docs/reference/storage#limitations) for more information). First, the Storage Read API does not support direct reading from logical or materialized views. Second, reading external tables is not supported. To mitigate these limitations, you can use the `bigquery_query` function to execute the query directly in BigQuery.

* **Propagation Delay**: After creating a table in BigQuery, there might be a brief propagation delay before the table becomes fully "visible". Therefore, be aware of potential delays when executing `CREATE TABLE ... AS` or `CREATE OR REPLACE TABLE ...` statements followed by immediate inserts. This delay is usually just a matter of seconds, but in rare cases, it can take up to a minute.

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
    duckdb-bigquery:v1.2.0
```

## Important Notes on Using Google BigQuery

When using this software with Google BigQuery, please ensure your usage complies with the [Google API Terms of Service](https://developers.google.com/terms). Be mindful of the usage limits and quotas, and adhere to Google's Fair Use Policy.

### Billing and Costs

Please be aware that using Google BigQuery through this software can incur costs. Google BigQuery is a paid service, and charges may apply based on the amount of data processed, stored, and the number of queries executed. Users are responsible for any costs associated with their use of Google BigQuery. For detailed information on BigQuery pricing, please refer to the [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing) page. It is recommended to monitor your usage and set up budget alerts in the Google Cloud Console to avoid unexpected charges.

By using this software, you acknowledge and agree that you are solely responsible for any charges incurred from Google BigQuery.
