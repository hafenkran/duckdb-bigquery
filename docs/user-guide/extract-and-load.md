# Extract and Load

BigQuery provides native batch operations for moving data in both directions:

- **Load:** A
  [BigQuery load job](https://docs.cloud.google.com/bigquery/docs/batch-loading-data)
  imports batch data from local files or Cloud Storage into a BigQuery table.
  The job handles formats such as CSV, JSON, Avro, Parquet, and ORC, including
  schema detection and whether data is appended or overwritten.

- **Export:** BigQuery exports table data to Cloud Storage with an extract
  job—the same operation exposed by the `bq extract` command. Google documents
  this under
  [Export table data to Cloud Storage](https://docs.cloud.google.com/bigquery/docs/exporting-data):
  **export** is the user-facing capability, while **extract job** is the
  underlying BigQuery job type.

Use `bigquery_load` and `bigquery_extract` to run these operations from DuckDB
SQL. Both functions wait for completion and return the job identity, status,
and transfer statistics; the following sections cover their inputs, options,
and operational requirements.

## Load Data into BigQuery {#load-data}

`bigquery_load` creates a BigQuery load job with exactly one source family. A
local file is uploaded directly, Cloud Storage URIs are read by BigQuery, and
a DuckDB table or view is first written to a temporary Parquet file. All three
paths write to a BigQuery `dataset.table`.

Load a local file:

=== "Project ID"

    ```sql
    -- Load a local Parquet file into a BigQuery table.
    SELECT *
      FROM bigquery_load(
          'my-gcp-project',
          'my_dataset.events',
          source_file := '/absolute/path/events.parquet',
          source_format := 'PARQUET',
          billing_project := 'my-billing-project'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_123 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        1250 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the destination dataset with its billing project.
    ATTACH 'project=my-gcp-project dataset=my_dataset billing_project=my-billing-project'
      AS bq (TYPE bigquery);

    -- Load a local Parquet file through the attached catalog.
    SELECT *
      FROM bigquery_load(
          'bq',
          'my_dataset.events',
          source_file := '/absolute/path/events.parquet',
          source_format := 'PARQUET'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_234 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        1250 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

The job ID and row count in the example output depend on the submitted file.

Load one or more Cloud Storage objects. URI wildcards are passed to BigQuery:

=== "Project ID"

    ```sql
    -- Append matching Cloud Storage objects to a BigQuery table.
    SELECT *
      FROM bigquery_load(
          'my-gcp-project',
          'my_dataset.events',
          source_uris := ['gs://my-bucket/events/part-*.parquet'],
          source_format := 'PARQUET',
          write_disposition := 'WRITE_APPEND'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_345 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        5000 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the destination dataset.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery);

    -- Append matching Cloud Storage objects through the attached catalog.
    SELECT *
      FROM bigquery_load(
          'bq',
          'my_dataset.events',
          source_uris := ['gs://my-bucket/events/part-*.parquet'],
          source_format := 'PARQUET',
          write_disposition := 'WRITE_APPEND'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_456 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        5000 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

BigQuery reads Cloud Storage sources itself, so the job identity needs access
to the objects. The bucket must also satisfy BigQuery's location rules for the
destination dataset. Google's
[batch loading documentation](https://cloud.google.com/bigquery/docs/batch-loading-data)
describes supported Cloud Storage layouts, wildcard behavior, permissions,
locations, service limits, and format-specific restrictions.

To load a DuckDB relation, pass its catalog-qualified name through
`source_table`:

=== "Project ID"

    ```sql
    -- Prepare a local DuckDB relation for the load job.
    CREATE TEMP TABLE prepared_events AS
      SELECT *
      FROM read_parquet('/absolute/path/source/*.parquet');

    -- Replace the destination table with the prepared relation.
    SELECT *
      FROM bigquery_load(
          'my-gcp-project',
          'my_dataset.events',
          source_table := 'prepared_events',
          write_disposition := 'WRITE_TRUNCATE'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_567 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        1250 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the destination dataset.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery);

    -- Prepare a local DuckDB relation for the load job.
    CREATE TEMP TABLE prepared_events AS
      SELECT *
      FROM read_parquet('/absolute/path/source/*.parquet');

    -- Replace the destination table through the attached catalog.
    SELECT *
      FROM bigquery_load(
          'bq',
          'my_dataset.events',
          source_table := 'prepared_events',
          write_disposition := 'WRITE_TRUNCATE'
      );
    ┌─────────┬──────────────┬────────────────┬──────────┬──────────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id       │ project_id     │ location │ destination_table                │ output_rows │ status           │
    │ boolean │ varchar      │ varchar        │ varchar  │ varchar                          │ uint64      │ json             │
    ├─────────┼──────────────┼────────────────┼──────────┼──────────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ load_job_678 │ my-gcp-project │ EU       │ my-gcp-project.my_dataset.events │        1250 │ {"state":"DONE"} │
    └─────────┴──────────────┴────────────────┴──────────┴──────────────────────────────────┴─────────────┴──────────────────┘
    ```

The extension stages the relation as a temporary local Parquet file, uploads
it, and removes it after the operation. The temporary directory must have
enough free space for the complete staged relation.

The extension accepts Parquet, CSV, newline-delimited JSON, Avro, and ORC.
Recognized and consistent file suffixes can determine the format; otherwise
specify `source_format`. CSV, JSON, Avro, Parquet, ORC, schema evolution, and
Hive partitioning each have additional options in the
[`bigquery_load` reference](../function-reference/bigquery-load.md). Google's
[loading overview](https://cloud.google.com/bigquery/docs/loading-data)
places batch load jobs alongside streaming, transfer, and federation options
that are outside this function's scope.

!!! warning "Default write disposition"

    `bigquery_load` defaults to `WRITE_TRUNCATE`. Set
    `write_disposition := 'WRITE_APPEND'` to append, or `WRITE_EMPTY` to
    require an empty destination. `create_disposition` independently controls
    whether BigQuery may create a missing table.

## Extract a Table to Cloud Storage {#extract-data}

`bigquery_extract` creates an extract job for one BigQuery table and writes one
or more objects to Cloud Storage:

=== "Project ID"

    ```sql
    -- Export a BigQuery table to compressed Parquet objects.
    SELECT *
      FROM bigquery_extract(
          'my-gcp-project',
          source_table := 'my_dataset.events',
          destination_uris := 'gs://my-bucket/exports/events-*.parquet',
          format := 'PARQUET',
          compression := 'ZSTD',
          billing_project := 'my-billing-project'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬─────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id          │ project_id     │ … │ destination_uri_file_counts │ input_bytes │ status           │
    │ boolean │ varchar         │ varchar        │ … │ bigint[]                    │ bigint      │ json             │
    ├─────────┼─────────────────┼────────────────┼───┼─────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ extract_job_123 │ my-gcp-project │ … │ [2]                         │      983040 │ {"state":"DONE"} │
    └─────────┴─────────────────┴────────────────┴───┴─────────────────────────────┴─────────────┴──────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the source dataset with its billing project.
    ATTACH 'project=my-gcp-project dataset=my_dataset billing_project=my-billing-project'
      AS bq (TYPE bigquery);

    -- Export a table through the attached catalog.
    SELECT *
      FROM bigquery_extract(
          'bq',
          source_table := 'my_dataset.events',
          destination_uris := 'gs://my-bucket/exports/events-*.parquet',
          format := 'PARQUET',
          compression := 'ZSTD'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬─────────────────────────────┬─────────────┬──────────────────┐
    │ success │ job_id          │ project_id     │ … │ destination_uri_file_counts │ input_bytes │ status           │
    │ boolean │ varchar         │ varchar        │ … │ bigint[]                    │ bigint      │ json             │
    ├─────────┼─────────────────┼────────────────┼───┼─────────────────────────────┼─────────────┼──────────────────┤
    │ true    │ extract_job_234 │ my-gcp-project │ … │ [2]                         │      983040 │ {"state":"DONE"} │
    └─────────┴─────────────────┴────────────────┴───┴─────────────────────────────┴─────────────┴──────────────────┘
    ```

Job IDs, file counts, and byte counts vary by execution. DuckDB may collapse
wide result sets with `…`; the
[`bigquery_extract` reference](../function-reference/bigquery-extract.md#result)
lists every result column.

The source is a table identifier, not arbitrary GoogleSQL. To export the
result of a query, either materialize it into a table first or execute
GoogleSQL's
[`EXPORT DATA` statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements)
with `bigquery_execute`.

The destination must use `gs://`. Use a wildcard when BigQuery may need to
produce multiple objects. BigQuery limits a single export file to 1 GB of
logical data; larger exports require multiple files. Output order is not
guaranteed for a table extract. Google's
[table export documentation](https://cloud.google.com/bigquery/docs/exporting-data)
explains destination URI rules, location requirements, export limits, and
Cloud Storage behavior.

The extension supports CSV, newline-delimited JSON, Avro, and Parquet exports.
It can infer a format when every destination URI has a consistent recognized
suffix, or `format` can be set explicitly. Compression is format-specific:

| Format | Supported compression |
| --- | --- |
| CSV | `NONE`, `GZIP` |
| Newline-delimited JSON | `NONE`, `GZIP` |
| Avro | `NONE`, `DEFLATE`, `SNAPPY` |
| Parquet | `NONE`, `GZIP`, `SNAPPY`, `ZSTD` |

CSV cannot represent nested and repeated BigQuery values. Prefer Avro,
newline-delimited JSON, or Parquet for those schemas. The
[`bigquery_extract` reference](../function-reference/bigquery-extract.md)
contains the complete parameters, format inference rules, CSV and Avro
options, and result columns.

## Permissions, Locations, and Job Completion

Load and extract calls require permission to create BigQuery jobs, access to
the source, permission to modify the load destination, and the relevant Cloud
Storage access. The exact IAM set depends on the direction and source type;
start with the project's [required permissions](../getting-started/required-permissions.md)
and the operation-specific permissions in Google's
[load](https://cloud.google.com/bigquery/docs/batch-loading-data) and
[export](https://cloud.google.com/bigquery/docs/exporting-data)
documentation.

The BigQuery job location and Cloud Storage bucket location must be compatible
with the dataset involved in the transfer. Pass `location` when it cannot be
derived or when `bq_default_location` is not appropriate. A mismatched location
can make an otherwise valid transfer fail.

Both functions wait for BigQuery and return the remote job ID, project,
location, status, and operation-specific statistics. A local `timeout_ms`
stops waiting in DuckDB but does not guarantee cancellation: the remote job can
continue reading, writing, or incurring charges. Use the returned `job_id`
with [Executing & Monitoring Jobs](jobs-and-transfers.md#monitor-jobs) to
inspect a timed-out or completed transfer.

Google's [job management guide](https://cloud.google.com/bigquery/docs/managing-jobs)
describes the BigQuery job lifecycle. Load and extract operations are also
subject to BigQuery's current
[quotas and limits](https://cloud.google.com/bigquery/quotas). Review
[Billing and Costs](../index.md#billing-and-costs) before submitting large
transfers.
