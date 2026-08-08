# Troubleshooting & Limitations

The extension connects systems with different execution models, SQL semantics,
type systems, and release cycles. Use the troubleshooting guidance for
unexpected failures, then review the known limitations and alternatives.

## Troubleshooting

### Permission Failures

If [authentication](getting-started/authentication-and-secrets.md) succeeds but
a later API request fails, the active identity usually lacks dataset, table,
job, Storage Read, or Storage Write permissions.

Check:

1. which storage and billing projects are configured;
2. whether the identity can use the billing project;
3. the operation-specific
   [IAM permissions](getting-started/required-permissions.md).

### TLS and Certificates

Set a readable CA bundle for cURL-based REST clients:

```sql
-- Use an explicit CA bundle for REST requests.
SET bq_curl_ca_bundle_path = '/path/to/ca-bundle.pem';
```

See
[`bq_curl_ca_bundle_path`](user-guide/configuration.md#type-and-diagnostic-settings)
for the setting details. Keep the root bundle current and do not disable
certificate verification.

### Windows gRPC Configuration

On Windows, gRPC requires an additional environment variable to configure the
trust store for SSL certificates. Download the root certificates and configure
the variable as described in the Google Cloud C++ client's
[official documentation](https://github.com/googleapis/google-cloud-cpp/blob/f2bd9a9af590f58317a216627ae9e2399c245bab/google/cloud/storage/quickstart/README.md#windows):

```batch title="Command Prompt"
@powershell -NoProfile -ExecutionPolicy unrestricted -Command ^
    (new-object System.Net.WebClient).Downloadfile( ^
        'https://pki.google.com/roots.pem', 'roots.pem')
set GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=%cd%\roots.pem
```

This downloads `roots.pem` into the current directory and sets
`GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` to that file for the current Command Prompt
session. Start DuckDB from the same session so gRPC can use the configured trust
store.

### A View or External Table Does Not Scan

Storage Read cannot directly scan logical views, materialized views, or
ordinary external tables. Use `bigquery_query` as described under
[Reading and Queries](user-guide/reading-and-queries.md#bigquery-query).

### Schema Changes Are Not Visible

Clear attached metadata caches:

```sql
-- Invalidate cached metadata for attached BigQuery catalogs.
CALL bigquery_clear_cache();
```

BigQuery metadata can also have a short propagation delay.

### Reads Are Not Parallel

Parallel Storage Read streams require insertion-order preservation to be
disabled:

```sql
-- Allow parallel result consumption.
SET preserve_insertion_order = false;

-- Request up to the configured DuckDB thread count.
SET bq_max_read_streams = 0;
```

BigQuery can return fewer streams than requested. See
[Storage Read Settings](user-guide/configuration.md#storage-read-settings) for
details.

### A Function Rejects Billing Options

When the first argument names an attached catalog, configure billing in
`ATTACH`. The catalog is authoritative, so conflicting per-call parameters
are rejected. `bigquery_execute` has no `billing_project` parameter.

### A Timed-Out Job Is Still Running

Timeouts stop local waiting but do not guarantee cancellation. Inspect the job
with `bigquery_jobs` or the Google Cloud console. See
[Executing & Monitoring Jobs](user-guide/jobs-and-transfers.md#timeouts-and-costs)
for job timeout semantics.

### Inspect Generated GoogleSQL

```sql
-- Print generated GoogleSQL for subsequent remote operations.
SET bq_debug_show_queries = true;
```

This prints remote SQL to standard output. Avoid enabling it where SQL text
could contain sensitive values. See
[Type and Diagnostic Settings](user-guide/configuration.md#type-and-diagnostic-settings)
for details.

## Limitations

- **DuckDB version support**<br>
  Current development targets DuckDB 1.5.x. The latest extension changes are
  not currently backported to older DuckDB release branches.

- **Community build platforms**<br>
  Community builds are available for `linux_amd64`, `linux_arm64`, `osx_amd64`,
  `osx_arm64`, and `windows_amd64`. WebAssembly and Windows MinGW builds are not
  supported.

- **[Native table scans](user-guide/reading-and-queries.md#bigquery-scan)**<br>
  Storage Read cannot directly scan logical views, materialized views, or
  ordinary external tables. Use `bigquery_query` to execute their logic in
  BigQuery.

- **[REST query results](user-guide/reading-and-queries.md#bigquery-query)**<br>
  The optional `use_rest_api := true` path is intended for small, simple
  results. BigQuery rejects result arrays containing `NULL` elements, and
  optional job creation can still create a query job.

- **[BIGNUMERIC](concepts/data-types.md#bignumeric-and-arrays)**<br>
  BigQuery `BIGNUMERIC` exceeds DuckDB's maximum decimal precision and is read
  as exact `VARCHAR`.

- **[Write types and nested arrays](concepts/data-types.md)**<br>
  Several wide, unsigned, and timestamp types cannot be written to BigQuery.
  BigQuery also rejects arrays of arrays.

- **[Table constraints](user-guide/managing-tables-and-schemas.md#create-a-table)**<br>
  Attached table creation supports column defaults and `NOT NULL`, but not
  primary keys, foreign keys, `UNIQUE`, `CHECK`, or indexes.

- **[Attached DDL and DML](user-guide/writing.md#limitations-and-safety)**<br>
  Attached catalogs support only the documented statement forms. Unsupported
  clauses, incompatible columns, and filters that cannot be translated safely
  are rejected.

- **[BigQuery-specific CREATE clauses](user-guide/configuration.md#bigquery-specific-create-clauses)**<br>
  `OPTIONS`, `PARTITION BY`, and `CLUSTER BY` require the additional
  experimental parser setting.

- **[Metadata caching and propagation](user-guide/attach.md#refresh-metadata)**<br>
  Attached metadata is cached, and BigQuery changes can take a short time to
  propagate. Refresh the cache when a long-lived connection exposes stale
  metadata.

- **[Job timeouts](user-guide/jobs-and-transfers.md#timeouts-and-costs)**<br>
  A local timeout stops waiting but does not guarantee remote cancellation. A
  BigQuery job can continue running and incurring charges.

- **[Transaction boundaries](concepts/architecture.md#attached-catalog-and-planning)**<br>
  Completed BigQuery DDL, DML, and Storage Write operations are not part of a
  multi-statement transaction that DuckDB can roll back.

- **[DuckDB and GoogleSQL semantics](user-guide/reading-and-queries.md#experimental-aggregate-pushdown)**<br>
  Casts, floating-point values, strings, and timestamps can behave differently
  depending on whether DuckDB or BigQuery executes the operation.
