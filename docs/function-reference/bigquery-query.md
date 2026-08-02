# `bigquery_query`

Runs GoogleSQL in BigQuery and returns the result rows to DuckDB. It is also the
read path for logical views, materialized views, and ordinary external tables.

For workflow examples and help choosing a result path, see
[Execute GoogleSQL](../user-guide/reading-and-queries.md#bigquery-query).

## Signature

```sql
SELECT *
  FROM bigquery_query(
      'PROJECT_OR_CATALOG',
      'GOOGLESQL',
      [POSITIONAL_PARAMETER, ...],
      billing_project := 'BILLING_PROJECT',
      use_rest_api := false,
      dry_run := false,
      timeout_ms := 0,
      api_endpoint := 'BIGQUERY_REST_ENDPOINT',
      grpc_endpoint := 'STORAGE_READ_ENDPOINT'
  );
```

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `project_or_catalog` | `VARCHAR` | required | Google Cloud project ID or attached BigQuery catalog. |
| `sql` | `VARCHAR` | required | [GoogleSQL](https://cloud.google.com/bigquery/docs/introduction-sql) query. |
| positional values | scalar | none | Values for `?` placeholders, in order. |
| `billing_project` | `VARCHAR` | project | Consumer project for a direct project call. |
| `use_rest_api` | `BOOLEAN` | `false` | Decode query results through the REST endpoint. |
| `dry_run` | `BOOLEAN` | `false` | Validate and estimate the query without executing it. |
| `timeout_ms` | `BIGINT` | `bq_query_timeout_ms` | Maximum local wait; `0` waits indefinitely. |
| `api_endpoint` | `VARCHAR` | Google default | BigQuery REST endpoint override for job and REST result requests. |
| `grpc_endpoint` | `VARCHAR` | Google default | Storage Read gRPC endpoint override for the standard result path. |

When `project_or_catalog` names an attached catalog, billing and endpoints are
taken from the attachment. Conflicting per-call values are rejected.

Positional values bind to `?` placeholders. Cast `NULL` to a concrete type.
List values are not supported as query parameters.

## Example

Positional values after the GoogleSQL string bind to `?` placeholders in
order. This example follows the parameter-binding cases in the query
SQLLogicTests and does not require an existing table.

```sql
-- Bind DuckDB values to GoogleSQL parameters by position.
SELECT *
  FROM bigquery_query(
      'my-gcp-project',
      'SELECT ? AS answer, ? AS sound',
      42,
      'quack'
  );
┌────────┬─────────┐
│ answer │  sound  │
│ int64  │ varchar │
├────────┼─────────┤
│     42 │ quack   │
└────────┴─────────┘
```

## Result

Normal result columns and types are derived from the BigQuery query schema.

A dry run returns one row:

| Column | Type | Description |
| --- | --- | --- |
| `total_bytes_processed` | `BIGINT` | BigQuery processed-byte estimate. |
| `cache_hit` | `BOOLEAN` | Whether BigQuery reports a cache hit. |
| `location` | `VARCHAR` | BigQuery job location. |

## Execution Paths

By default, the function creates a query job, materializes its result, and
reads that result through the
[Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage).

With `use_rest_api := true`, results are decoded inline through
[`jobs.query`](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query).
The REST path decodes BigQuery `ARRAY` and `STRUCT` results recursively as
DuckDB `LIST` and `STRUCT` values. BigQuery does not expose native `MAP` or
union result types, and it rejects final query results whose arrays contain
`NULL` elements. BigQuery can still create a job under its
[`JOB_CREATION_OPTIONAL` behavior](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query#JobCreationMode).

## Errors and Timeouts

Typical errors include invalid GoogleSQL, a placeholder count or type mismatch,
missing job or source-object permissions, an unusable billing project,
conflicting catalog options, malformed REST result shapes, and result arrays
containing `NULL` elements.

`timeout_ms` stops local waiting but does not guarantee cancellation. Inspect
timed-out work with [`bigquery_jobs`](bigquery-jobs.md).
