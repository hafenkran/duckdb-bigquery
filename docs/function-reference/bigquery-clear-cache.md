# `bigquery_clear_cache`

Clears the extension's local metadata caches for every attached BigQuery
catalog.

## Signature

```sql
CALL bigquery_clear_cache();
```

## Parameters

This function takes no arguments.

## Example

`bigquery_clear_cache` returns one row after clearing every attached BigQuery
catalog's local metadata cache.

```sql
-- Clear local metadata caches for all attached BigQuery catalogs.
CALL bigquery_clear_cache();
┌─────────┐
│ success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

## Result

| Column | Type | Description |
| --- | --- | --- |
| `success` | `BOOLEAN` | `true` after local cache invalidation completes. |

The function does not issue a BigQuery query, delete remote resources, detach
catalogs, or clear BigQuery's query-result cache. Calling it without an
attached BigQuery catalog is valid. Later catalog access can issue fresh
metadata requests.

See [Refresh Metadata](../user-guide/attach.md#refresh-metadata-and-detach)
for the surrounding catalog workflow.
