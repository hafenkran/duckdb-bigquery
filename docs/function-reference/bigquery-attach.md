# `bigquery_attach`

Enumerates the native tables in one BigQuery dataset and creates local DuckDB
views backed by `bigquery_scan`.

!!! note "Compatibility helper"

    Prefer [`ATTACH ... (TYPE bigquery)`](../user-guide/attach.md)
    for catalog, transaction, DDL, and write integration.

## Signature

```sql
CALL bigquery_attach(
    'PROJECT.DATASET',
    overwrite := false
);
```

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `dataset` | `VARCHAR` | required | Project and dataset identifier. |
| `overwrite` | `BOOLEAN` | `false` | Replace colliding local views. |

## Example

The current implementation emits no row. The output below comes from
querying one of the local views it creates. Because the view name is the fully
qualified BigQuery table identifier, quote it as one DuckDB identifier.

```sql
-- Create local compatibility views for the dataset's native tables.
CALL bigquery_attach(
      'my-gcp-project.quacking_dataset',
      overwrite := false
  );

-- Query one of the generated local views.
SELECT
      i,
      s
  FROM "my-gcp-project.quacking_dataset.duck_tbl"
  ORDER BY i;
┌───────┬────────────────┐
│   i   │       s        │
│ int64 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

## Result and Behavior

The function declares one `Success BOOLEAN` result column. The current
implementation performs the view-creation side effects but emits no row, so
invoke it with `CALL`.

Reading the created views uses `bigquery_scan` and therefore inherits its
Storage Read permissions, cost, and native-table limitations. The function
uses the direct project's default credentials and endpoints; it does not reuse
an attached catalog.

Typical errors include an identifier other than `project.dataset`, failed
dataset discovery, missing permissions, and a local view-name collision when
`overwrite=false`.

See [ATTACH](../user-guide/attach.md) for
the recommended attachment workflow.
