# `bigquery_load`

Creates a BigQuery load job from exactly one source family: a local file,
Cloud Storage URIs, or a DuckDB table or view.

For complete import workflows, see
[Load Data](../user-guide/extract-and-load.md#load-data).

## Signature

```sql
SELECT *
  FROM bigquery_load(
      'PROJECT_OR_CATALOG',
      'DATASET.TABLE',
      source_file := '/path/input.parquet'
      -- or source_uris := ['gs://bucket/input-*.parquet']
      -- or source_table := 'duckdb_table'
  );
```

The first two positional arguments are required.

## Positional Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `project_or_catalog` | `VARCHAR` | required | Google Cloud project ID or attached BigQuery catalog. |
| `destination_table` | `VARCHAR` | required | BigQuery destination as `dataset.table` or a qualified identifier. |

## Source Parameters

Specify exactly one source family.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `source_file` | `VARCHAR` | none | Local file. A `gs://` value is treated as `source_uris`. |
| `file` | `VARCHAR` | none | Alias for `source_file`; do not combine them. |
| `source_uris` | `VARCHAR` or `LIST<VARCHAR>` | none | One or more nonempty `gs://` URIs. |
| `source_table` | `VARCHAR` | none | DuckDB table or view to stage as a local Parquet file. |
| `table` | `VARCHAR` | none | Alias for `source_table`; do not combine them. |
| `source_format` | `VARCHAR` | inferred or `PARQUET` | `PARQUET`, `CSV`, `NEWLINE_DELIMITED_JSON`, `AVRO`, or `ORC`. |

The format is inferred from consistent recognized source suffixes when
possible; otherwise it defaults to `PARQUET`. A `source_table` is always staged
as Parquet and therefore requires `source_format='PARQUET'` when the format is
specified.

## Job Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `write_disposition` | `VARCHAR` | `WRITE_TRUNCATE` | `WRITE_TRUNCATE`, `WRITE_APPEND`, or `WRITE_EMPTY`. |
| `create_disposition` | `VARCHAR` | `CREATE_IF_NEEDED` | `CREATE_IF_NEEDED` or `CREATE_NEVER`. |
| `location` | `VARCHAR` | `bq_default_location` | BigQuery job location. |
| `billing_project` | `VARCHAR` | project | Consumer project for a direct project call. |
| `labels` | `MAP(VARCHAR, VARCHAR)` | none | Job labels. |
| `timeout_ms` | `BIGINT` | `bq_query_timeout_ms` | Maximum local wait; `0` waits indefinitely. |
| `autodetect` | `BOOLEAN` | BigQuery default | Enable schema and format-option detection. |
| `schema_update_options` | `VARCHAR` or `LIST<VARCHAR>` | none | `ALLOW_FIELD_ADDITION` and/or `ALLOW_FIELD_RELAXATION`. |
| `max_bad_records` | `BIGINT` | BigQuery default | Nonnegative maximum rejected records. |
| `ignore_unknown_values` | `BOOLEAN` | BigQuery default | Ignore extra input fields when supported. |

For an attached catalog, configure billing in `ATTACH`; a per-call
`billing_project` is rejected. The attachment must be read-write.

## CSV Parameters

These parameters require `source_format='CSV'`.

| Parameter | Type | Default |
| --- | --- | --- |
| `csv_field_delimiter` | `VARCHAR` | BigQuery default |
| `csv_skip_leading_rows` | `BIGINT` | BigQuery default |
| `csv_quote` | `VARCHAR` | BigQuery default |
| `csv_allow_quoted_newlines` | `BOOLEAN` | BigQuery default |
| `csv_allow_jagged_rows` | `BOOLEAN` | BigQuery default |
| `csv_encoding` | `VARCHAR` | BigQuery default |
| `csv_null_marker` | `VARCHAR` | BigQuery default |
| `csv_null_markers` | `VARCHAR` or `LIST<VARCHAR>` | BigQuery default |
| `csv_preserve_ascii_control_characters` | `BOOLEAN` | BigQuery default |

`csv_skip_leading_rows` must be nonnegative. `csv_null_marker` and
`csv_null_markers` cannot be used together.

## Format-Specific Parameters

| Parameter | Type | Default | Applies to |
| --- | --- | --- | --- |
| `date_format` | `VARCHAR` | BigQuery default | CSV or newline-delimited JSON |
| `datetime_format` | `VARCHAR` | BigQuery default | CSV or newline-delimited JSON |
| `time_format` | `VARCHAR` | BigQuery default | CSV or newline-delimited JSON |
| `timestamp_format` | `VARCHAR` | BigQuery default | CSV or newline-delimited JSON |
| `time_zone` | `VARCHAR` | BigQuery default | CSV or newline-delimited JSON |
| `json_extension` | `VARCHAR` | none | Newline-delimited JSON; only `GEOJSON` |
| `avro_use_logical_types` | `BOOLEAN` | BigQuery default | Avro |
| `parquet_enable_list_inference` | `BOOLEAN` | BigQuery default | Parquet |
| `parquet_enum_as_string` | `BOOLEAN` | BigQuery default | Parquet |
| `reference_file_schema_uri` | `VARCHAR` | none | Avro, Parquet, or ORC |
| `decimal_target_types` | `VARCHAR` or `LIST<VARCHAR>` | none | Avro, Parquet, or ORC; `NUMERIC`, `BIGNUMERIC`, `STRING` |

## Hive Partitioning Parameters

These parameters require `source_uris`.

| Parameter | Type | Default | Values |
| --- | --- | --- | --- |
| `hive_partitioning_mode` | `VARCHAR` | none | `AUTO`, `STRINGS`, or `CUSTOM` |
| `hive_partitioning_source_uri_prefix` | `VARCHAR` | none | Cloud Storage URI prefix |

## Example

This example follows the tested `source_table` path. The extension stages the
local relation as Parquet, submits a load job, and reports the number of rows
written.

```sql
-- Attach the destination dataset.
ATTACH 'project=my-gcp-project dataset=quacking_dataset'
  AS bq (TYPE bigquery);

-- Create the DuckDB relation that will be staged for the load job.
CREATE TEMP TABLE local_ducks AS
SELECT *
  FROM (VALUES
      (12, 'quack'),
      (13, 'quack quack')
  ) ducks(i, s);

-- Submit the load job and inspect its stable result fields.
SELECT
      success,
      output_rows
  FROM bigquery_load(
      'bq',
      'quacking_dataset.loaded_ducks',
      source_table := 'local_ducks',
      write_disposition := 'WRITE_TRUNCATE'
  );
┌─────────┬─────────────┐
│ success │ output_rows │
│ boolean │   uint64    │
├─────────┼─────────────┤
│ true    │           2 │
└─────────┴─────────────┘
```

## Result

The function returns one row:

| Column | Type |
| --- | --- |
| `success` | `BOOLEAN` |
| `job_id` | `VARCHAR` |
| `project_id` | `VARCHAR` |
| `location` | `VARCHAR` |
| `destination_table` | `VARCHAR` |
| `output_rows` | `UBIGINT` |
| `status` | `JSON` |

`output_rows` can be `NULL` when BigQuery does not report load statistics.

## Errors and Timeouts

Typical errors include specifying zero or several source families, mixed or
unknown source suffixes without an explicit format, incompatible
format-specific parameters, invalid Cloud Storage URIs, a read-only catalog,
insufficient bucket or destination permissions, and insufficient local
temporary space for `source_table`.

A `source_table` is copied to a temporary local Parquet file before upload and
removed afterward. A timed-out load job can continue remotely.
