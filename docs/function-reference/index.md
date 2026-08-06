# Function Reference

This chapter is the canonical reference for every SQL function registered by
the BigQuery extension. Each function page contains its complete signature,
parameters, defaults, result columns, configuration rules, and local
validation behavior.

Use the capability pages for end-to-end workflows and examples. Use this
chapter when you need to look up a function or configure a specific call.

Each function page includes commented DuckDB SQL and its result when the
statement returns rows. The examples use the placeholder projects, datasets,
and tables introduced on the Home page. Job examples select stable result
fields because generated IDs, timestamps, locations, and byte counts vary
between executions.

## Read Functions

- **[`bigquery_scan`](bigquery-scan.md)**<br>
  Read one native BigQuery table through the Storage Read API.
  [Reading and Queries →](../user-guide/reading-and-queries.md#bigquery-scan)

- **[`bigquery_query`](bigquery-query.md)**<br>
  Run GoogleSQL and return its rows.
  [Reading and Queries →](../user-guide/reading-and-queries.md#bigquery-query)

## Job and Transfer Functions

- **[`bigquery_execute`](bigquery-execute.md)**<br>
  Run GoogleSQL and return execution metadata.
  [Execute GoogleSQL →](../user-guide/jobs-and-transfers.md#execute-googlesql)

- **[`bigquery_load`](bigquery-load.md)**<br>
  Load local files, Cloud Storage objects, or DuckDB relations into BigQuery.
  [Load Data →](../user-guide/extract-and-load.md#load-data)

- **[`bigquery_extract`](bigquery-extract.md)**<br>
  Export a BigQuery table to Cloud Storage.
  [Extract Data →](../user-guide/extract-and-load.md#extract-data)

- **[`bigquery_jobs`](bigquery-jobs.md)**<br>
  List BigQuery jobs or retrieve one job.
  [Monitor Jobs →](../user-guide/jobs-and-transfers.md#monitor-jobs)

## Utilities and Compatibility

- **[`bigquery_attach`](bigquery-attach.md)**<br>
  Create local views for the native tables in one dataset.
  [ATTACH Compatibility Helper →](../user-guide/attach.md#compatibility-helper)

- **[`bigquery_clear_cache`](bigquery-clear-cache.md)**<br>
  Clear metadata caches for attached BigQuery catalogs.
  [Refresh Metadata →](../user-guide/attach.md#refresh-metadata-and-detach)

- **[`bigquery_normalize_geography`](bigquery-normalize-geography.md)**<br>
  Normalize a geometry for BigQuery geography semantics.
  [Data Type Mapping →](../concepts/data-types.md#geometry-and-geography)

Many functions accept either a Google Cloud project ID or the name of an
attached BigQuery catalog as their first argument. A catalog reuses its
project, billing project, endpoint, credentials, access mode, and transaction
context. Per-call options that would conflict with catalog-owned configuration
are rejected. Each function page documents the exact rule.

For non-function interfaces, use the topic pages in the main navigation.
