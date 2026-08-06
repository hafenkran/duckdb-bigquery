# `bigquery_extract`

Creates an extract job that exports one BigQuery table to one or more Cloud
Storage objects.

For export workflows and examples, see
[Extract Data](../user-guide/extract-and-load.md#extract-data).

## Signature

```sql
SELECT *
  FROM bigquery_extract(
      'PROJECT_OR_CATALOG',
      source_table := 'DATASET.TABLE',
      destination_uris := ['gs://bucket/export-*.parquet'],
      format := 'PARQUET',
      compression := 'ZSTD',
      api_endpoint := 'BIGQUERY_REST_ENDPOINT'
  );
```

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `project_or_catalog` | `VARCHAR` | required | Google Cloud project ID or attached BigQuery catalog. |
| `source_table` | `VARCHAR` | required | `dataset.table` or qualified BigQuery table identifier. |
| `destination_uris` | `VARCHAR` or `LIST<VARCHAR>` | required | One or more nonempty `gs://` destinations. |
| `format` | `VARCHAR` | inferred | `CSV`, `JSON`, `NEWLINE_DELIMITED_JSON`, `AVRO`, or `PARQUET`. |
| `compression` | `VARCHAR` | BigQuery default | Format-compatible compression. |
| `csv_print_header` | `BOOLEAN` | BigQuery default | Include a CSV header; CSV only. |
| `csv_field_delimiter` | `VARCHAR` | BigQuery default | CSV field delimiter; CSV only. |
| `avro_use_logical_types` | `BOOLEAN` | BigQuery default | Use Avro logical types; Avro only. |
| `location` | `VARCHAR` | `bq_default_location` | BigQuery job location. |
| `labels` | `MAP(VARCHAR, VARCHAR)` | none | Job labels. |
| `billing_project` | `VARCHAR` | project | Consumer project for a direct project call. |
| `timeout_ms` | `BIGINT` | `bq_query_timeout_ms` | Maximum local wait; `0` waits indefinitely. |
| `api_endpoint` | `VARCHAR` | Google default | BigQuery REST endpoint override for a direct project call. |

`JSON` is normalized to `NEWLINE_DELIMITED_JSON`. Without `format`, every URI
must imply the same format from `.csv`, `.csv.gz`, `.json`, `.json.gz`,
`.avro`, or `.parquet`.

For an attached catalog, `billing_project` and `api_endpoint` are rejected;
configure them in `ATTACH`. The attachment must be read-write.

## Compression

| Format | Allowed values |
| --- | --- |
| CSV | `NONE`, `GZIP` |
| Newline-delimited JSON | `NONE`, `GZIP` |
| Avro | `NONE`, `DEFLATE`, `SNAPPY` |
| Parquet | `NONE`, `GZIP`, `SNAPPY`, `ZSTD` |

## Example

The example exports the table used on the Home page. BigQuery writes the
objects to Cloud Storage; replace the project and bucket placeholders before
running it.

```sql
-- Attach the source dataset and reuse its connection configuration.
ATTACH
    'project=my-gcp-project dataset=quacking_dataset'
  AS bq (TYPE bigquery);

-- Submit the extract job and inspect stable result fields.
SELECT
      success,
      format,
      input_bytes >= 0 AS has_input_stats
  FROM bigquery_extract(
      'bq',
      source_table := 'quacking_dataset.duck_tbl',
      destination_uris := 'gs://my-bucket/exports/ducks-*.parquet',
      format := 'PARQUET'
  );
┌─────────┬─────────┬─────────────────┐
│ success │ format  │ has_input_stats │
│ boolean │ varchar │     boolean     │
├─────────┼─────────┼─────────────────┤
│ true    │ PARQUET │ true            │
└─────────┴─────────┴─────────────────┘
```

## Result

| Column | Type |
| --- | --- |
| `success` | `BOOLEAN` |
| `job_id` | `VARCHAR` |
| `project_id` | `VARCHAR` |
| `location` | `VARCHAR` |
| `source_table` | `VARCHAR` |
| `destination_uris` | `VARCHAR[]` |
| `format` | `VARCHAR` |
| `destination_uri_file_counts` | `BIGINT[]` |
| `input_bytes` | `BIGINT` |
| `status` | `JSON` |

Job-statistics fields can be `NULL` when BigQuery does not report them.

## Errors and Timeouts

The source must be a table; arbitrary query text is not accepted. Typical
errors include missing or non-GCS destination URIs, incompatible format and
compression combinations, conflicting inferred URI formats, a read-only
catalog, and insufficient source-table or Cloud Storage permissions.

A timed-out extract job can continue remotely and write Cloud Storage objects.
