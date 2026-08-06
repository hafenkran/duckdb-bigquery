# `bigquery_execute`

Runs GoogleSQL and returns execution metadata instead of query rows. It is
intended for DDL, DML, scripts, and materializing a query into a destination
table.

For task-oriented examples and alternatives through attached SQL, see
[Execute GoogleSQL](../user-guide/jobs-and-transfers.md#execute-googlesql).

## Signature

```sql
SELECT *
  FROM bigquery_execute(
      'PROJECT_OR_CATALOG',
      'GOOGLESQL',
      dry_run := false,
      timeout_ms := 0,
      destination_table := 'DATASET.TABLE',
      write_disposition := 'WRITE_TRUNCATE',
      create_disposition := 'CREATE_IF_NEEDED',
      api_endpoint := 'BIGQUERY_REST_ENDPOINT',
      grpc_endpoint := 'BIGQUERY_GRPC_ENDPOINT'
  );
```

Use `CALL bigquery_execute(...)` when the result row is not needed.

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `project_or_catalog` | `VARCHAR` | required | Google Cloud project ID or attached BigQuery catalog. |
| `sql` | `VARCHAR` | required | GoogleSQL statement, query, or script. |
| `dry_run` | `BOOLEAN` | `false` | Validate and estimate without executing. |
| `timeout_ms` | `BIGINT` | `bq_query_timeout_ms` | Maximum local wait; `0` waits indefinitely. |
| `destination_table` | `VARCHAR` | none | `dataset.table` or `project.dataset.table` for query materialization. |
| `write_disposition` | `VARCHAR` | `WRITE_TRUNCATE` | `WRITE_TRUNCATE`, `WRITE_APPEND`, or `WRITE_EMPTY`. |
| `create_disposition` | `VARCHAR` | `CREATE_IF_NEEDED` | `CREATE_IF_NEEDED` or `CREATE_NEVER`. |
| `api_endpoint` | `VARCHAR` | Google default | BigQuery REST endpoint override. |
| `grpc_endpoint` | `VARCHAR` | Google default | BigQuery gRPC endpoint override stored in the direct-call configuration. |

`destination_table` cannot be combined with `dry_run`. Disposition values are
used only with a destination.

The function has no `billing_project` parameter. Use a catalog configured with
`billing_project` when billing should use another project. An attached catalog
must be read-write and supplies its own endpoints, so function-level endpoint
overrides are rejected for catalog calls.

## Example

A dry run validates the GoogleSQL and returns an estimate without executing
the statement. Selecting stable checks keeps the output useful even though
the concrete location can vary by project.

```sql
-- Validate a query and inspect its estimated execution metadata.
SELECT
      total_bytes_processed = 0 AS no_bytes_processed,
      NOT cache_hit AS cache_miss,
      location <> '' AS has_location
  FROM bigquery_execute(
      'my-gcp-project',
      'SELECT 1 AS id',
      dry_run := true
  );
┌────────────────────┬────────────┬──────────────┐
│ no_bytes_processed │ cache_miss │ has_location │
│      boolean       │  boolean   │   boolean    │
├────────────────────┼────────────┼──────────────┤
│ true               │ true       │ true         │
└────────────────────┴────────────┴──────────────┘
```

## Result

Normal execution returns one row:

| Column | Type |
| --- | --- |
| `success` | `BOOLEAN` |
| `job_id` | `VARCHAR` |
| `project_id` | `VARCHAR` |
| `location` | `VARCHAR` |
| `total_rows` | `UBIGINT` |
| `total_bytes_processed` | `BIGINT` |
| `num_dml_affected_rows` | `BIGINT` |

Fields BigQuery does not report for a statement can be `NULL`.

A dry run instead returns:

| Column | Type |
| --- | --- |
| `total_bytes_processed` | `BIGINT` |
| `cache_hit` | `BOOLEAN` |
| `location` | `VARCHAR` |

## Errors and Timeouts

Typical errors include invalid GoogleSQL, a read-only catalog, missing job or
destination-table permissions, a disposition without `destination_table`,
combining `destination_table` with `dry_run`, and conflicting endpoints for an
attached catalog.

`timeout_ms` stops local waiting but does not guarantee cancellation. Inspect
submitted work with [`bigquery_jobs`](bigquery-jobs.md).
