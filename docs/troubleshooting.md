# Troubleshooting

Use this page for operational failures and current extension limitations.
For tuning and the complete settings reference, see
[Additional Settings](user-guide/configuration.md).

## Permission Failures

If [authentication](getting-started/authentication-and-secrets.md) succeeds but a later API
request fails, the active identity usually lacks dataset, table, job, Storage
Read, or Storage Write permissions.

Check:

1. which storage and billing projects are configured;
2. whether the identity can use the billing project;
3. the operation-specific [IAM permissions](getting-started/required-permissions.md).

## TLS and Certificates

Set a readable CA bundle for cURL-based REST clients:

```sql
-- Use an explicit CA bundle for REST requests.
SET bq_curl_ca_bundle_path = '/path/to/ca-bundle.pem';
```

On Windows, gRPC may additionally need
`GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` before DuckDB starts. Follow the Google
Cloud C++ client's
[Windows TLS setup](https://github.com/googleapis/google-cloud-cpp/blob/f2bd9a9af590f58317a216627ae9e2399c245bab/google/cloud/storage/quickstart/README.md#windows)
and keep the root bundle current. Do not disable certificate verification.

See [`bq_curl_ca_bundle_path`](user-guide/configuration.md#type-and-diagnostic-settings)
for the setting details.

## Common Problems

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
[Storage Read Settings](user-guide/configuration.md#storage-read-settings) for details.

### A Function Rejects Billing Options

When the first argument names an attached catalog, configure billing in
`ATTACH`. The catalog is authoritative, so conflicting per-call parameters
are rejected. `bigquery_execute` has no `billing_project` parameter.

### A Timed-Out Job Is Still Running

Timeouts stop local waiting but do not guarantee cancellation. Inspect the job
with `bigquery_jobs` or the Google Cloud console. See
[Executing & Monitoring Jobs](user-guide/jobs-and-transfers.md#timeouts-and-costs) for job timeout
semantics.

### Inspect Generated GoogleSQL

```sql
-- Print generated GoogleSQL for subsequent remote operations.
SET bq_debug_show_queries = true;
```

This prints remote SQL to standard output. Avoid enabling it where SQL text
could contain sensitive values. See
[Type and Diagnostic Settings](user-guide/configuration.md#type-and-diagnostic-settings)
for details.

## Known Limitations

| Area | Current limitation | Alternative or details |
| --- | --- | --- |
| Platforms | Community builds do not support WebAssembly or Windows MinGW | Use one of the platforms listed on [Home](index.md#what-you-can-do) |
| Native scans | Views and ordinary external tables cannot be scanned directly | Use [`bigquery_query`](user-guide/reading-and-queries.md#bigquery-query) |
| REST results | BigQuery rejects result arrays containing `NULL` elements and may still create a job in optional job-creation mode | See [`bigquery_query`](user-guide/reading-and-queries.md#bigquery-query) |
| Type reads | `BIGNUMERIC` is exposed as exact `VARCHAR` | See [Data Type Mapping](concepts/data-types.md#bignumeric-and-arrays) |
| Type writes | Several very wide, unsigned, and timestamp types are unsupported; nested arrays are rejected | See [Data Type Mapping](concepts/data-types.md) |
| Attached SQL | Only documented DDL and row-mutation forms are supported | See [Managing Tables & Datasets](user-guide/managing-tables-and-schemas.md) and [Writing & Modifying Data](user-guide/writing.md) |
| Table clauses | `PARTITION BY`, `CLUSTER BY`, and `OPTIONS` require the experimental parser | See [table options](user-guide/managing-tables-and-schemas.md#create-a-table-with-options) and [partitioning and clustering](user-guide/managing-tables-and-schemas.md#partition-and-cluster-a-table) |
| Timeouts | Stopping the local wait does not cancel a remote job | See [Executing & Monitoring Jobs](user-guide/jobs-and-transfers.md#timeouts-and-costs) |
| Metadata | Attached metadata is cached and remote changes can propagate slowly | See [Refresh Metadata](user-guide/attach.md#refresh-metadata-and-detach) |
| Transactions | Remote API operations are not part of a multi-statement cross-system transaction | Keep execution boundaries explicit |

GoogleSQL and DuckDB can differ in casting, floating-point, string, and
timestamp semantics. Make the execution boundary explicit when exact semantics
matter.
