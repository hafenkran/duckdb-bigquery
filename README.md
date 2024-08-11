# DuckDB BigQuery Extension

This repository contains the [DuckDB](https://duckdb.org) BigQuery Extension. This extension lets DuckDB integrate with Google BigQuery, allowing users to directly access, manage, and manipulate their BigQuery datasets/tables using standard SQL statements. Inspired by official DuckDB RDBMS extensions like [MySQL](https://duckdb.org/docs/extensions/mysql.html), [PostgreSQL](https://github.com/duckdb/postgres_scanner), and [SQLite](https://github.com/duckdb/sqlite_scanner), this extension offers a similar feel.

Please note that this extension is in its initial release. While it covers most of the essentials, you might run into some limitations, issues, or bugs. So far, this extension has only been tested on Linux systems.

> This extension only works for DuckDB v1.0.0 and only `linux_amd64`, `linux_amd64_gcc4`, `osx_arm64`, and `windows_amd64` builds are currently supported.

## Preliminaries

To get started, you will need to setup the [Google Cloud CLI (gcloud)](https://cloud.google.com/sdk/gcloud). This set of tools enables the interaction with Google Cloud services right from your terminal. Download the latest version from the [Google Cloud CLI installation page](https://cloud.google.com/sdk/docs/install) and follow the instructions to select and authenticate your Google Cloud project for using BigQuery. You have the following two options to authenticate:

### Option 1: Configure ADC with your Google Account

To set up authentication, we use the Application Default Credentials ([ADC](https://cloud.google.com/docs/authentication/provide-credentials-adc)) which offer a simple way to set credentials accessing services in the Google Cloud Platform. To do so, just execute the following command and follow the steps along:

```bash
gcloud auth application-default login
```

### Option 2: Configure Service account keys

Alternatively, you can authenticate using a service account and its keys. To do this, create a service account in the Google Cloud Console, assign the necessary roles, and download the JSON key file. Save the JSON key file securely on your system. Next, set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to this file. For example, on Linux or macOS, use:

```bash
# On Linux or macOS
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/my/service-account-credentials.json"

# On Windows
set GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\my\service-account-credentials.json"
```

### Windows gRPC Configuration

On Windows, gRPC requires an additional environment variable to configure the trust store for SSL certificates. Download and configure it using ([official documentation](https://github.com/googleapis/google-cloud-cpp/blob/f2bd9a9af590f58317a216627ae9e2399c245bab/google/cloud/storage/quickstart/README.md#windows)):

```bash
@powershell -NoProfile -ExecutionPolicy unrestricted -Command ^
    (new-object System.Net.WebClient).Downloadfile( ^
        'https://pki.google.com/roots.pem', 'roots.pem')
set GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=%cd%\roots.pem
```

This downloads the `roots.pem` file and sets the `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` environment variable to its location.

## Quickstart

To use this extension, DuckDB must be started with the [unsigned extensions](https://duckdb.org/docs/extensions/overview.html#unsigned-extensions) setting enabled. Depending on the kind of client you are using, you can achieve this by either using the `allow_unsigned_extensions` flag in your clients or, in the case of the CLI client, by starting with the `-unsigned` flag as follows:

```bash
# Example: Python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})

# Example: CLI
duckdb -unsigned
```

> Note: Windows user require an additional step to configure the gRPC SSL certificates (see [here](#windows-grpc-configuration)).

Once DuckDB is running with unsigned extensions enabled, install and load the previously downloaded DuckDB BigQuery Extension:

```sql
-- Set the custom repository, then install and load the DuckDB BigQuery extension
D SET custom_extension_repository = 'http://storage.googleapis.com/hafenkran';
D FORCE INSTALL 'bigquery';
D LOAD 'bigquery';
```

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

### `bigquery_execute` Function

The `bigquery_execute` function enables you to execute arbitrary queries directly in BigQuery, including specialized functions or options which are unique to BigQuery. These queries can be expressed in native GoogleSQL and are executed without prior parsing or interpretation by DuckDB.

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
```

### `bigquery_clear_cache` Function

To ensure a consistent and efficient interaction with BigQuery, DuckDB employs, similar to other extensions, a caching mechanism. This mechanism stores schema information, such as datasets, table names, their columns, etc. locally to avoid continuously fetching this data from BigQuery. If changes are made to the schema through a different connection, such as new columns being added to a table, the local cache becomes outdated. In this case, the function `bigquery_clear_cache` can be executed to clear the internal caches allowing DuckDB to refetch the most current schema information from BigQuery.

```sql
D CALL bigquery_clear_cache();
```

### Additional Extension Settings

| Settings                        | Description                                                               | Default |
| ------------------------------- | ------------------------------------------------------------------------- | ------- |
| bq_debug_show_queries           | [DEBUG] - whether to print all queries sent to BigQuery to stdout         | `false` |
| bq_experimental_filter_pushdown | [EXPERIMENTAL] - Whether or not to use filter pushdown                    | `true`  |
| bq_curl_ca_bundle_path          | Path to the CA certificates used by cURL for SSL certificate verification |         |

## Limitations

There are some minor limitations that arise from the combination of DuckDB and BigQuery. These include:

* **Reading from Views**: This DuckDB extension utilizes the BigQuery Storage Read API to optimize reading results. However, this approach has limitations (see [here](https://cloud.google.com/bigquery/docs/reference/storage#limitations) for more information). First, the Storage Read API does not support direct reading from logical or materialized views. Second, reading external tables is not supported.

* **Propagation Delay**: After creating a table in BigQuery, there might be a brief propagation delay before the table becomes fully "visible". Therefore, be aware of potential delays when executing `CREATE TABLE ... AS` or `CREATE OR REPLACE TABLE ...` statements followed by immediate inserts. This delay is usually just a matter of seconds, but in rare cases, it can take up to a minute.

* **Primary Keys and Foreign Keys**: While BigQuery recently introduced the concept of primary keys and foreign keys constraints, they differ from what you're accustomed to in DuckDB or other traditional RDBMS. Therefore, this extension does not support this concept.

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
    duckdb-bigquery:v1.0.0
```

## Important Notes on Using Google BigQuery

When using this software with Google BigQuery, please ensure your usage complies with the [Google API Terms of Service](https://developers.google.com/terms). Be mindful of the usage limits and quotas, and adhere to Google's Fair Use Policy.

### Billing and Costs

Please be aware that using Google BigQuery through this software can incur costs. Google BigQuery is a paid service, and charges may apply based on the amount of data processed, stored, and the number of queries executed. Users are responsible for any costs associated with their use of Google BigQuery. For detailed information on BigQuery pricing, please refer to the [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing) page. It is recommended to monitor your usage and set up budget alerts in the Google Cloud Console to avoid unexpected charges.

By using this software, you acknowledge and agree that you are solely responsible for any charges incurred from Google BigQuery.
