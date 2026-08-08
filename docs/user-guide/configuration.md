# Additional Settings

The BigQuery extension registers its settings when it is loaded. Use regular
DuckDB `SET` statements to change them. Function parameters and `ATTACH`
options are documented with their respective interfaces; they are not global
extension settings.

List the available BigQuery settings and their current values through
`duckdb_settings()`:

```sql
-- Load the extension and register its settings.
LOAD 'bigquery';

-- Inspect all settings registered by the BigQuery extension.
SELECT
      name,
      value,
      description
  FROM duckdb_settings()
  WHERE name LIKE 'bq_%'
  ORDER BY name;
┌──────────────────────────┬─────────┬────────────────────────────────────────────┐
│ name                     │ value   │ description                                │
│ varchar                  │ varchar │ varchar                                    │
├──────────────────────────┼─────────┼────────────────────────────────────────────┤
│ bq_arrow_compression     │ ZSTD    │ Arrow compression codec                    │
│ bq_auth_timeout_s        │ 10      │ Authentication request timeout in seconds  │
│ bq_default_location      │ US      │ Default BigQuery location                  │
│            ·             │  ·      │                     ·                      │
└──────────────────────────┴─────────┴────────────────────────────────────────────┘
```

For operational errors and known limitations, see
[Troubleshooting & Limitations](../troubleshooting.md).

## Settings

| Setting | Type | Default | Purpose |
| --- | --- | --- | --- |
| `bq_default_location` | `VARCHAR` | `US` | Fallback for location-aware operations without an explicit location |
| `bq_query_timeout_ms` | `BIGINT` | `0` | Maximum local wait for query-job completion; `0` waits indefinitely |
| `bq_auth_timeout_s` | `BIGINT` | `10` | Timeout for authentication token requests |
| `bq_max_read_streams` | `BIGINT` | `0` | Maximum requested Storage Read streams; `0` follows the DuckDB thread count |
| `bq_arrow_compression` | `VARCHAR` | `ZSTD` | Arrow compression for Storage Read: `UNSPECIFIED`, `LZ4_FRAME`, or `ZSTD` |
| `bq_experimental_filter_pushdown` | `BOOLEAN` | `true` | Translate eligible DuckDB filters to Storage Read row restrictions |
| `bq_enable_aggregate_pushdown` | `BOOLEAN` | `false` | Rewrite supported aggregates as BigQuery query jobs |
| `bq_experimental_use_info_schema` | `BOOLEAN` | `true` | Fetch catalog metadata with job-backed `INFORMATION_SCHEMA` queries |
| `bq_experimental_enable_sql_parser` | `BOOLEAN` | `false` | Parse supported BigQuery-specific `CREATE` clauses |
| `bq_enable_inflight_request_windowing` | `BOOLEAN` | `true` | Allow multiple unacknowledged Storage Write requests in flight |
| `bq_bignumeric_as_varchar` | `BOOLEAN` | `true` | Expose BigQuery `BIGNUMERIC` as exact `VARCHAR` values |
| `bq_debug_show_queries` | `BOOLEAN` | `false` | Print generated GoogleSQL to standard output |
| `bq_curl_ca_bundle_path` | `VARCHAR` | empty | Set a readable CA bundle for cURL-based REST requests |

Timeout and stream-count values must be nonnegative and fit in a signed
32-bit integer. BigQuery can return fewer read streams than requested.

## Location and Timeouts

### Default Location

`bq_default_location` provides the fallback for location-aware operations.
When a function accepts an explicit `location` parameter, that value overrides
the setting for that call.

```sql
-- Use EU when an operation does not specify its own location.
SET bq_default_location = 'EU';
```

The effective location, whether explicit or inherited from the setting, must
be compatible with the datasets involved in the operation. A mismatch is
rejected by BigQuery rather than moving data between regions.

### Query and Authentication Timeouts

`bq_query_timeout_ms` limits how long DuckDB waits locally for a BigQuery
query job. The default `0` waits until the job completes. Reaching the timeout
stops the local wait but does not guarantee that BigQuery cancels the remote
job.

`bq_auth_timeout_s` limits authentication token fetches. Increase it only
when the metadata server or OAuth endpoint is expected to respond slowly.

```sql
-- Wait up to one minute for a BigQuery query job.
SET bq_query_timeout_ms = 60000;

-- Wait up to 30 seconds for an authentication token.
SET bq_auth_timeout_s = 30;
```

## Storage Read Settings

### Parallel Read Streams

`bq_max_read_streams` controls the maximum number of streams requested from
the BigQuery Storage Read API. Its default `0` uses DuckDB's configured thread
count. BigQuery may still grant fewer streams based on table size and service
limits.

Parallel reads also require DuckDB's `preserve_insertion_order` setting to be
disabled:

```sql
-- Allow parallel result consumption.
SET preserve_insertion_order = false;

-- Request up to the configured DuckDB thread count.
SET bq_max_read_streams = 0;
```

Setting a value greater than `1` while preserving insertion order produces a
warning and still limits the read path to one stream.

### Arrow Compression

`bq_arrow_compression` selects the compression requested for Arrow record
batches returned by Storage Read. `ZSTD` is the default. `LZ4_FRAME` can be a
useful alternative when lower decompression overhead matters more than wire
size. `UNSPECIFIED` requests no specific codec. Tests with the current service
behavior have not shown additional compression for this value, but the enum
does not guarantee a particular codec or response encoding.

```sql
-- Request LZ4 frame compression for Storage Read responses.
SET bq_arrow_compression = 'LZ4_FRAME';
```

### Filter Pushdown

`bq_experimental_filter_pushdown` is enabled by default. Eligible filters on
native BigQuery tables become Storage Read row restrictions, reducing the
rows transferred to DuckDB. Unsupported filters remain in the local DuckDB
plan.

```sql
-- Keep all filtering in DuckDB for comparison or diagnosis.
SET bq_experimental_filter_pushdown = false;
```

Use `EXPLAIN` as described under
[Filter Pushdown](reading-and-queries.md#filter-pushdown) to inspect
the execution plan.

## Storage Write Settings

`bq_enable_inflight_request_windowing` allows multiple Storage Write
`AppendRows` requests to remain in flight before the extension waits for
acknowledgements. The default is optimized for throughput. Disable it to
reduce the amount of unacknowledged data buffered in memory, at the cost of
lower write throughput.

```sql
-- Wait for each append acknowledgement before sending more data.
SET bq_enable_inflight_request_windowing = false;
```

## Experimental Planning and Parsing

### Aggregate Pushdown

`bq_enable_aggregate_pushdown` is disabled by default. When enabled, the
optimizer can translate supported aggregates, filters, grouping expressions,
and compatible `DISTINCT` aggregates to GoogleSQL and execute them in
BigQuery.

```sql
-- Allow supported aggregate queries to run in BigQuery.
SET bq_enable_aggregate_pushdown = true;
```

Unsupported query shapes fall back to DuckDB before a remote job starts.
Errors from an already started BigQuery job are not retried locally.
GoogleSQL casting, string, and floating-point semantics can also differ from
DuckDB semantics. Use `EXPLAIN` to confirm the selected execution path.

### Catalog Metadata through INFORMATION_SCHEMA

`bq_experimental_use_info_schema` is enabled by default and can make catalog
discovery significantly faster than fetching each table through the REST API.
The extension submits an `INFORMATION_SCHEMA` query job for each dataset
location involved in discovery. This requires permission to create BigQuery
jobs and can incur query costs.

Disable the setting to fetch table metadata through REST requests without
submitting these metadata query jobs. This is also useful when comparing
metadata behavior or diagnosing catalog discovery:

```sql
-- Fetch catalog metadata without the INFORMATION_SCHEMA optimization.
SET bq_experimental_use_info_schema = false;
```

### BigQuery-specific CREATE Clauses

`bq_experimental_enable_sql_parser` enables the parser extension for supported
BigQuery-specific `OPTIONS`, `PARTITION BY`, and `CLUSTER BY` clauses. It is
disabled by default because it changes how the extension preprocesses
`CREATE` statements.

```sql
-- Enable supported BigQuery-specific CREATE clauses.
SET bq_experimental_enable_sql_parser = true;
```

See [Managing Tables & Datasets](managing-tables-and-schemas.md) for
the supported forms and examples.

## Type and Diagnostic Settings

`bq_bignumeric_as_varchar` is retained for compatibility with older
workflows. Current versions expose `BIGNUMERIC` as exact `VARCHAR` values
because its precision exceeds DuckDB's maximum decimal precision. See
[Data Type Mapping](../concepts/data-types.md#bignumeric-and-arrays).

`bq_debug_show_queries` prints generated remote SQL to standard output. Use it
only for short-lived diagnosis because the SQL text can contain sensitive
values:

```sql
-- Print generated GoogleSQL for subsequent remote operations.
SET bq_debug_show_queries = true;
```

`bq_curl_ca_bundle_path` overrides the CA bundle used by cURL-based REST
requests. The file must exist and be readable:

```sql
-- Use an explicit CA bundle for REST requests.
SET bq_curl_ca_bundle_path = '/path/to/ca-bundle.pem';
```
