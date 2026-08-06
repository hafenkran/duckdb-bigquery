# `bigquery_normalize_geography`

Normalizes a DuckDB geometry for BigQuery geography writes, including polygon
winding and touching-hole topology.

## Signature

```text
bigquery_normalize_geography(GEOMETRY) -> GEOMETRY
```

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `geometry` | `GEOMETRY` | Geometry to normalize for a BigQuery write. |

## Example

The polygon starts with BigQuery-incompatible winding. The normalized output
reverses the ring while preserving the same area. This input and result come
from the local geography normalization SQLLogicTest.

```sql
-- Preview the geometry used by the BigQuery write path.
SELECT bigquery_normalize_geography(
      'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'
          ::GEOMETRY('OGC:CRS84')
  ) AS normalized;
┌──────────────────────────────────────────────┐
│                  normalized                  │
│                   geometry                   │
├──────────────────────────────────────────────┤
│ POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))     │
└──────────────────────────────────────────────┘
```

## Result and Behavior

The function returns one `GEOMETRY` value. It runs locally and has no project,
billing, credential, endpoint, or setting parameters. It does not issue a
billable BigQuery request. A non-geometry argument fails during binding or
casting.

See [Working with Geometries](../user-guide/geometry-support.md) for the automatic write path,
CRS requirements, and the difference between geometry and geography.
