# `bigquery_scan`

Reads one native BigQuery table through the BigQuery Storage Read API without
creating a DuckDB catalog.

For a guided comparison with attached table reads, see
[Reading and Queries](../user-guide/reading-and-queries.md#bigquery-scan).

## Signature

```sql
SELECT *
  FROM bigquery_scan(
      'PROJECT.DATASET.TABLE',
      billing_project := 'BILLING_PROJECT',
      filter := 'ROW_RESTRICTION',
      api_endpoint := 'BIGQUERY_REST_ENDPOINT',
      grpc_endpoint := 'STORAGE_READ_ENDPOINT'
  );
```

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `table` | `VARCHAR` | required | Fully qualified `project.dataset.table` identifier. |
| `billing_project` | `VARCHAR` | storage project | Consumer project for billing and quota. |
| `filter` | `VARCHAR` | none | Explicit Storage Read row restriction. |
| `api_endpoint` | `VARCHAR` | Google default | BigQuery REST endpoint override. |
| `grpc_endpoint` | `VARCHAR` | Google default | Storage Read gRPC endpoint override. |

The result columns and DuckDB types are derived from the BigQuery table schema.

## Example

The example uses the `duck_tbl` table introduced in
[Install, Attach, and Query](../getting-started/install-attach-and-query.md).
The projection order and filter behavior mirror the scan SQLLogicTests.

```sql
-- Read and filter the table through the Storage Read API.
SELECT
      i,
      s
  FROM bigquery_scan(
      'my-gcp-project.quacking_dataset.duck_tbl'
  )
  WHERE i >= 12
  ORDER BY i;
┌───────┬────────────────┐
│   i   │       s        │
│ int64 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

## Behavior

DuckDB pushes projected columns into the Storage Read session. With
`bq_experimental_filter_pushdown=true`, eligible DuckDB filters are translated
to Storage Read row restrictions. The explicit `filter` argument is passed as
trusted
[`TableReadOptions.row_restriction`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions)
text and is not a parameterized DuckDB expression.

`bq_max_read_streams`, `bq_arrow_compression`, and
`bq_experimental_filter_pushdown` control the read path. See
[Additional Settings](../user-guide/configuration.md#settings).

## Limitations and Errors

The function reads native BigQuery tables. It does not directly read logical
views, materialized views, or ordinary external tables. Use
[`bigquery_query`](bigquery-query.md) for those relations.

Typical errors include an invalid three-part table name, missing Storage Read
permissions, an invalid row restriction, an unusable billing project, and an
unreachable endpoint override.
Review Google's
[Storage Read API limitations](https://cloud.google.com/bigquery/docs/reference/storage#limitations)
for service-level constraints.
