# Attaching Projects

`ATTACH ... (TYPE bigquery)` makes a BigQuery project or dataset available as
a DuckDB catalog. BigQuery datasets appear as schemas, and tables can be
addressed with three-part names such as `bq.my_dataset.my_table`.

Complete [Install, Attach, and Query](../getting-started/install-attach-and-query.md)
first if the extension is not installed yet. Before attaching, configure one
of the supported [authentication methods](../getting-started/authentication-and-secrets.md).
Authentication identifies the caller; the
[Required Permissions](../getting-started/required-permissions.md) guide covers the Google Cloud
IAM access required by each operation.

## Attach a Project

The standard form attaches all accessible datasets in a project. The name
after `AS` is the local DuckDB catalog name; `bq` does not rename or create
anything in BigQuery:

```sql
-- Attach every accessible dataset in the project as a read-only catalog.
ATTACH 'project=my-gcp-project' AS bq (TYPE bigquery, READ_ONLY);

-- List tables from the attached datasets.
SHOW ALL TABLES;
┌──────────┬────────────┬────────────────────┬──────────────┬───────────────────────────┬───────────┐
│ database │   schema   │        name        │ column_names │       column_types        │ temporary │
│ varchar  │ varchar    │ varchar            │ varchar[]    │ varchar[]                 │ boolean   │
├──────────┼────────────┼────────────────────┼──────────────┼───────────────────────────┼───────────┤
│ bq       │ my_dataset │ function_scan_test │ [a, b, c]    │ [BIGINT, BIGINT, VARCHAR] │ false     │
│ bq       │ archive    │ old_events         │ [event_id]   │ [BIGINT]                   │ false     │
└──────────┴────────────┴────────────────────┴──────────────┴───────────────────────────┴───────────┘

-- Inspect one BigQuery table's columns.
DESCRIBE TABLE bq.my_dataset.function_scan_test;
┌─────────────┬─────────────┬──────┬──────┬─────────┬───────┐
│ column_name │ column_type │ null │ key  │ default │ extra │
├─────────────┼─────────────┼──────┼──────┼─────────┼───────┤
│ a           │ BIGINT      │ YES  │ NULL │ NULL    │ NULL  │
│ b           │ BIGINT      │ YES  │ NULL │ NULL    │ NULL  │
│ c           │ VARCHAR     │ YES  │ NULL │ NULL    │ NULL  │
└─────────────┴─────────────┴──────┴──────┴─────────┴───────┘
```

`SHOW ALL TABLES` lists relations from every accessible dataset in the
attached project. The `my_dataset` and `archive` rows make that project-wide
scope visible. `DESCRIBE TABLE` retrieves the schema of one relation.

## Attach One Dataset

Add `dataset` to restrict discovery to one dataset. This is useful when a
project contains many datasets or the connection should have an unambiguous
default schema:

```sql
-- Attach only my_dataset as a read-only catalog.
ATTACH 'project=my-gcp-project dataset=my_dataset'
  AS bq (TYPE bigquery, READ_ONLY);

-- List tables from the attached dataset.
SHOW ALL TABLES;
┌──────────┬────────────┬────────────────────┬──────────────┬───────────────────────────┬───────────┐
│ database │   schema   │        name        │ column_names │       column_types        │ temporary │
│ varchar  │ varchar    │ varchar            │ varchar[]    │ varchar[]                 │ boolean   │
├──────────┼────────────┼────────────────────┼──────────────┼───────────────────────────┼───────────┤
│ bq       │ my_dataset │ function_scan_test │ [a, b, c]    │ [BIGINT, BIGINT, VARCHAR] │ false     │
└──────────┴────────────┴────────────────────┴──────────────┴───────────────────────────┴───────────┘
```

Unlike the project attachment, this result contains no relations from
`archive`. The shorthand `ATTACH 'my-gcp-project.my_dataset' AS bq (TYPE
bigquery, READ_ONLY)` is equivalent. After a dataset-scoped attachment,
`USE bq` selects that dataset as the current schema.

## Attach Public or Cross-Project Data

Use `billing_project` when the project storing the data differs from the
project that supplies quota and receives applicable charges. This is commonly
required for public datasets:

```sql
-- Read public data while billing queries to my-gcp-project.
ATTACH 'project=bigquery-public-data dataset=geo_us_boundaries billing_project=my-gcp-project'
  AS bq (TYPE bigquery, READ_ONLY);

-- List tables in the public dataset.
SHOW ALL TABLES;
┌──────────┬───────────────────┬───────────────────┬───┬───────────┐
│ database │      schema       │       name        │ … │ temporary │
│ varchar  │ varchar           │ varchar           │ … │ boolean   │
├──────────┼───────────────────┼───────────────────┼───┼───────────┤
│ bq       │ geo_us_boundaries │ adjacent_counties │ … │ false     │
│ bq       │ geo_us_boundaries │ cnecta            │ … │ false     │
│ bq       │ geo_us_boundaries │ coastline         │ … │ false     │
│    ·     │         ·         │         ·         │ · │     ·     │
│    ·     │         ·         │         ·         │ · │     ·     │
│ bq       │ geo_us_boundaries │ zip_codes         │ … │ false     │
└──────────┴───────────────────┴───────────────────┴───┴───────────┘

-- Query a table through the attached catalog.
SELECT count(*)
  FROM bq.geo_us_boundaries.cnecta;
┌──────────────┐
│ count_star() │
│ int64        │
├──────────────┤
│ 7            │
└──────────────┘
```

Here, `project` identifies the project containing the data and
`billing_project` identifies the project used for billing and quota. The
setting does not grant access; the active credentials still need the required
IAM [permissions](../getting-started/required-permissions.md).

## Access Mode

Use `READ_ONLY` for exploration and analytics unless the connection is
intended to modify BigQuery:

```sql
-- Prevent operations that modify BigQuery.
ATTACH 'project=my-gcp-project dataset=my_dataset'
  AS bq (TYPE bigquery, READ_ONLY);
```

A read-only catalog rejects attached DDL and DML, Storage Write operations,
load and extract jobs, and `bigquery_execute` calls made through that catalog.
Omit `READ_ONLY` to permit supported mutations:

```sql
-- Allow supported write operations through the catalog.
ATTACH 'project=my-gcp-project dataset=my_dataset'
  AS bq (TYPE bigquery);
```

This only removes the local read-only guard; IAM still controls every remote
operation. DuckDB external access must also be enabled. Start with
[Writing & Modifying Data](writing.md), then use
[Managing Tables & Datasets](managing-tables-and-schemas.md) and
[Executing & Monitoring Jobs](jobs-and-transfers.md) for the supported
operations.

Several projects can be attached at the same time by giving each one a
different local catalog name.

## Use the Attached Catalog

In `bq.my_dataset.my_table`, `bq` is the attached DuckDB catalog,
`my_dataset` is the BigQuery dataset, and `my_table` is the relation.
`USE` can shorten subsequent names:

```sql
-- Select the attached dataset as the current schema.
USE bq.my_dataset;

-- Query a table without its catalog and schema prefixes.
SELECT *
  FROM function_scan_test
  ORDER BY a;
┌───────┬───────┬─────────┐
│ a     │ b     │ c       │
│ int64 │ int64 │ varchar │
├───────┼───────┼─────────┤
│     1 │     2 │ alpha   │
│     3 │     4 │ beta    │
└───────┴───────┴─────────┘
```

For a dataset-scoped attachment, `USE bq` selects its configured dataset
automatically. Functions that accept a project or attached catalog can also
receive `'bq'`, reusing its credentials, billing project, access mode, and
transaction context. Conflicting function-level billing options are rejected.

For the available read paths and their differences, see
[Reading and Queries](reading-and-queries.md).

<a id="refresh-metadata-and-detach"></a>

## Refresh Metadata

Attached catalogs cache dataset and table metadata. The catalog reloads
relations that are missing from the cache and can rebind after stale column
metadata. If a longer-lived connection still sees outdated metadata after an
external schema change, clear all attached BigQuery metadata explicitly:

```sql
-- Clear cached metadata for all attached BigQuery catalogs.
CALL bigquery_clear_cache();
┌─────────┐
│ success │
│ boolean │
├─────────┤
│ true    │
└─────────┘
```

This clears metadata for every attached BigQuery catalog. It does not detach a
catalog, delete a remote resource, or clear BigQuery's query-result cache. See
the [`bigquery_clear_cache` reference](../function-reference/bigquery-clear-cache.md)
for the result contract.

## Detach the Catalog

`DETACH` removes the local DuckDB catalog. It does not delete or modify the
BigQuery project, datasets, or tables:

```sql
-- Remove the local catalog without changing BigQuery resources.
DETACH bq;
```

<a id="compatibility-helper"></a>

## Legacy Compatibility Helper

The older `bigquery_attach` helper enumerates one dataset and creates local
DuckDB views backed by `bigquery_scan`:

```sql
-- Create compatibility views for one BigQuery dataset.
CALL bigquery_attach(
    'my-gcp-project.my_dataset',
    overwrite := false
);
```

Prefer storage `ATTACH` for catalog lookup, configuration reuse, transactions,
DDL, and writes. Use the helper only when compatibility with its local-view
workflow is required; its complete contract is documented in the
[`bigquery_attach` reference](../function-reference/bigquery-attach.md).
