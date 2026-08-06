# Data Type Mapping

The extension maps BigQuery schemas to DuckDB types for reads and converts
DuckDB types to BigQuery types for attached table creation and writes.

## BigQuery to DuckDB

| BigQuery | DuckDB | Notes |
| --- | --- | --- |
| `BOOL` | `BOOLEAN` | Direct mapping |
| `INT64` | `BIGINT` | BigQuery has one signed integer type |
| `FLOAT64` | `DOUBLE` | Direct mapping |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` | DuckDB precision must not exceed 38 |
| `BIGNUMERIC` | `VARCHAR` | BigQuery precision exceeds DuckDB `DECIMAL` |
| `STRING` | `VARCHAR` | Parameterized strings are accepted |
| `JSON` | `VARCHAR` | JSON text |
| `BYTES` | `BLOB` | Binary data |
| `DATE` | `DATE` | Direct mapping |
| `TIME` | `TIME` | Microsecond precision |
| `DATETIME` | `TIMESTAMP` | No time-zone component |
| `TIMESTAMP` | `TIMESTAMP` | BigQuery timestamps are UTC |
| `INTERVAL` | `INTERVAL` | Direct semantic mapping |
| `GEOGRAPHY` | `GEOMETRY('OGC:CRS84')` | WKT interchange |
| `ARRAY<T>` | `LIST<T>` | Element types are mapped recursively |
| `STRUCT<...>` | `STRUCT(...)` | Fields are mapped recursively |

## DuckDB to BigQuery

| DuckDB | BigQuery | Notes |
| --- | --- | --- |
| `BOOLEAN` | `BOOL` | Direct mapping |
| signed integers through `BIGINT` | `INT64` | All widths map to `INT64` |
| `UTINYINT`, `USMALLINT`, `UINTEGER` | `INT64` | Values fit in signed `INT64` |
| `FLOAT`, `DOUBLE` | `FLOAT64` | Direct mapping |
| `DECIMAL` | `NUMERIC` | BigQuery `NUMERIC` limits apply |
| `VARCHAR`, `UUID` | `STRING` | UUID is serialized as text |
| `BLOB` | `BYTES` | Binary data |
| `DATE`, `TIME` | `DATE`, `TIME` | Direct mapping |
| `TIMESTAMP`, `TIMESTAMP_S`, `TIMESTAMP_MS` | `TIMESTAMP` | Normalized to BigQuery timestamp precision |
| `INTERVAL` | `INTERVAL` | Serialized as a BigQuery interval |
| `GEOMETRY` | `GEOGRAPHY` | Normalized to BigQuery-compatible winding |
| `LIST<T>`, `ARRAY<T>` | `ARRAY<T>` | Nested arrays are rejected |
| `STRUCT(...)` | `STRUCT<...>` | Fields are mapped recursively |

`HUGEINT`, `UHUGEINT`, `UBIGINT`, `TIMESTAMP_TZ`, and `TIMESTAMP_NS` are not
supported for attached BigQuery table creation. Convert them explicitly before
writing.

## BIGNUMERIC and Arrays

BigQuery `BIGNUMERIC` has precision 76 and scale 38: up to 38 digits before and
38 digits after the decimal point. DuckDB `DECIMAL` supports at most 38 digits
in total, so the full `BIGNUMERIC` range cannot be represented losslessly and
is read as exact `VARCHAR`. For writes, prefer quoted exact decimal text or an
appropriate `DECIMAL` rather than floating-point literals.

BigQuery does not support arrays of arrays. A DuckDB list whose child is
another list or fixed array is rejected during schema conversion.

The `use_rest_api := true` query path decodes BigQuery `ARRAY` and `STRUCT`
results recursively as DuckDB `LIST` and `STRUCT` values. DuckDB-specific
`MAP` and `UNION` types are not part of BigQuery result schemas.

## Geometry and Geography

BigQuery `GEOGRAPHY` maps to DuckDB `GEOMETRY('OGC:CRS84')`, and DuckDB
`GEOMETRY` maps back to BigQuery `GEOGRAPHY`. The extension uses WKT at the API
boundary and normalizes polygon topology before Storage Write operations.

Since DuckDB 1.5.0, `GEOMETRY` is a built-in DuckDB type and does not require
the `spatial` extension. Most spatial analysis and transformation functions
still require `spatial`.

See [Working with Geometries](../user-guide/geometry-support.md) for the read and write flows, CRS
requirements, polygon normalization, and the semantic difference between
DuckDB's Cartesian geometry and BigQuery's spherical geography.

Continue with [Managing Tables & Datasets](../user-guide/managing-tables-and-schemas.md),
or [Writing & Modifying Data](../user-guide/writing.md).
