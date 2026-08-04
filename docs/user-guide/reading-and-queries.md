# Reading and Queries

The extension provides three ways to read data from BigQuery:

- **[`bigquery_scan`](#bigquery-scan)**<br>
  Read one native BigQuery table directly through the BigQuery Storage Read
  API without creating a catalog.

- **[`bigquery_query`](#bigquery-query)**<br>
  Run custom GoogleSQL in BigQuery, including queries over views, materialized
  views, and external tables.

- **[Attached project](#select-from-an-attached-project)**<br>
  Reuse a BigQuery catalog whose native tables can be queried with regular
  DuckDB SQL.

`bigquery_scan` and attached table reads share the same Storage Read execution
model. `bigquery_query` submits a query to BigQuery and returns its result rows.
There is no automatic switch between these execution models.

For inserts, updates, deletes, and query-result writes, continue with
[Writing & Modifying Data](writing.md). Table management and job-based
transfers are documented separately under
[Managing Tables & Datasets](managing-tables-and-schemas.md) and
[Extract & Load](extract-and-load.md), as well as
[Executing & Monitoring Jobs](jobs-and-transfers.md).

## Read a Native Table with `bigquery_scan` {#bigquery-scan}

`bigquery_scan` targets one fully qualified native BigQuery table without
creating a persistent catalog:

```sql
-- Read selected columns from one native BigQuery table.
SELECT a, b, c
  FROM bigquery_scan('my-gcp-project.my_dataset.function_scan_test')
  ORDER BY a;
┌───────┬───────┬─────────┐
│ a     │ b     │ c       │
│ int64 │ int64 │ varchar │
├───────┼───────┼─────────┤
│     1 │     2 │ alpha   │
│     3 │     4 │ beta    │
└───────┴───────┴─────────┘
```

DuckDB executes the surrounding SQL and reads the table through the BigQuery
Storage Read API. Projected columns are pushed into the read session.
Eligible DuckDB filters can be pushed down with
`bq_experimental_filter_pushdown`. The function also accepts an explicit
trusted Storage Read `filter` string:

```sql
-- Apply an explicit Storage Read row restriction.
SELECT a, c
  FROM bigquery_scan(
      'my-gcp-project.my_dataset.function_scan_test',
      filter := 'b = 4'
  );
┌───────┬─────────┐
│ a     │ c       │
│ int64 │ varchar │
├───────┼─────────┤
│     3 │ beta    │
└───────┴─────────┘
```

The explicit filter is
[`TableReadOptions.row_restriction`](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions)
text, not a parameterized DuckDB expression. Do not construct it from
untrusted input.

Choose `bigquery_scan` when a one-off native table is the only remote object
needed and connection reuse, catalog discovery, DDL, or writes are irrelevant.
It does not execute GoogleSQL or read view-like relations; use
`bigquery_query` for that. The complete parameters and error behavior are
listed in the
[`bigquery_scan` reference](../function-reference/bigquery-scan.md).

## Run GoogleSQL with `bigquery_query` {#bigquery-query}

`bigquery_query` is the standard query function when BigQuery should execute a
[GoogleSQL](https://cloud.google.com/bigquery/docs/introduction-sql)
statement. It supports BigQuery syntax and functions, resolves logical views,
materialized views, and external tables, and can aggregate or join data inside
BigQuery before transferring the result. This example queries a logical view:

=== "Project ID"

    ```sql
    -- Run GoogleSQL against a BigQuery view.
    SELECT *
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT name
           FROM `my-gcp-project.my_dataset.test_cities_view`
           ORDER BY name
           LIMIT 1'
      );
    ┌───────────┐
    │   name    │
    │  varchar  │
    ├───────────┤
    │ Amsterdam │
    └───────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the dataset containing the view.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Run the same GoogleSQL using the attached configuration.
    SELECT *
      FROM bigquery_query(
          'bq',
          'SELECT name
           FROM `my-gcp-project.my_dataset.test_cities_view`
           ORDER BY name
           LIMIT 1'
      );
    ┌───────────┐
    │   name    │
    │  varchar  │
    ├───────────┤
    │ Amsterdam │
    └───────────┘
    ```

The SQL string is GoogleSQL, not DuckDB SQL. The outer `SELECT` is DuckDB SQL
and can continue processing the returned rows. Passing an attached catalog
name instead of a project ID reuses all connection configuration.

For a catalog call, the attachment supplies its project, credentials, billing
project, access mode, and transaction context. Conflicting per-call billing
options are rejected.

Bind values with `?` placeholders rather than interpolating them into the SQL
text:

=== "Project ID"

    ```sql
    -- Bind values to GoogleSQL parameters.
    SELECT *
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT ? AS x, ? AS y',
          42,
          'abc'
      );
    ┌───────┬─────────┐
    │ x     │ y       │
    │ int64 │ varchar │
    ├───────┼─────────┤
    │    42 │ abc     │
    └───────┴─────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the project whose configuration should be reused.
    ATTACH 'project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Bind the same values through the attached catalog.
    SELECT *
      FROM bigquery_query(
          'bq',
          'SELECT ? AS x, ? AS y',
          42,
          'abc'
      );
    ┌───────┬─────────┐
    │ x     │ y       │
    │ int64 │ varchar │
    ├───────┼─────────┤
    │    42 │ abc     │
    └───────┴─────────┘
    ```

Cast `NULL` to a concrete type. List values are not supported as query
parameters. The
[`bigquery_query` reference](../function-reference/bigquery-query.md) contains
the complete signature, named options, defaults, dry-run result, and validation
rules.

By default, `bigquery_query` determines the result schema, creates a BigQuery
query job, materializes its result, and streams that result to DuckDB through
the Storage Read API. This is the normal path and supports large or complex
results. `use_rest_api := true` is an optional inline REST result path for small,
simple result sets:

=== "Project ID"

    ```sql
    -- Return a small, simple result through the REST API.
    SELECT *
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT 42 AS x',
          use_rest_api := true
      );
    ┌───────┐
    │ x     │
    │ int64 │
    ├───────┤
    │    42 │
    └───────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the project whose REST configuration should be reused.
    ATTACH 'project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Return the same result through the attached catalog.
    SELECT *
      FROM bigquery_query(
          'bq',
          'SELECT 42 AS x',
          use_rest_api := true
      );
    ┌───────┐
    │ x     │
    │ int64 │
    ├───────┤
    │    42 │
    └───────┘
    ```

The REST path decodes BigQuery `ARRAY` and `STRUCT` results recursively as
DuckDB `LIST` and `STRUCT` values. BigQuery does not expose native `MAP` or
union result types, and it rejects final query results whose arrays contain
`NULL` elements. With `JOB_CREATION_OPTIONAL`, BigQuery prioritizes returning
results inline but may still create a job, for example for long-running
queries or large results. A local timeout stops waiting in DuckDB but does not
guarantee remote cancellation. Inspect submitted work with `bigquery_jobs` as
described under
[Executing & Monitoring Jobs](jobs-and-transfers.md#monitor-jobs), and review
[Billing and Costs](../index.md#billing-and-costs) before running expensive
GoogleSQL.

## SELECT from an Attached Project

After [attaching a project or dataset](attach.md), native BigQuery tables
behave like remote DuckDB relations in a reusable catalog:

```sql
-- Attach one dataset as a read-only catalog.
ATTACH 'project=my-gcp-project dataset=my_dataset'
  AS bq (TYPE bigquery, READ_ONLY);

-- Query a native table through the attached catalog.
SELECT c, a, b
  FROM bq.my_dataset.function_scan_test
  ORDER BY a;
┌─────────┬───────┬───────┐
│ c       │ a     │ b     │
│ varchar │ int64 │ int64 │
├─────────┼───────┼───────┤
│ alpha   │     1 │     2 │
│ beta    │     3 │     4 │
└─────────┴───────┴───────┘
```

DuckDB binds and plans this as DuckDB SQL. The extension obtains the table
schema from the attached catalog, requests the necessary columns through the
Storage Read API, and translates eligible predicates into BigQuery row
restrictions. DuckDB evaluates the remaining expressions and performs local
joins, sorting, grouping, and aggregation. This makes attached reads a good
fit when BigQuery tables need to participate in a larger DuckDB query.

Projection pushdown is automatic. Filter pushdown is controlled by
`bq_experimental_filter_pushdown`; filters that cannot be represented as
Storage Read row restrictions remain in DuckDB. Parallel reads and other
Storage Read settings are documented under
[Additional Settings](configuration.md#settings).

The direct Storage Read boundary matters: attached scans support native
BigQuery tables, but they do not transparently execute logical views,
materialized views, or ordinary external tables. If a relation needs BigQuery
query semantics, express the operation as GoogleSQL with `bigquery_query`.

### Filter Pushdown

Attached native-table reads can translate supported DuckDB filters to BigQuery
Storage Read row restrictions. This reduces the number of rows transferred to
DuckDB. Filter pushdown is enabled by default through
`bq_experimental_filter_pushdown`:

```sql
-- Inspect whether the filter is pushed into the BigQuery scan.
EXPLAIN
  SELECT i
  FROM bq.my_dataset.filter_pushdown
  WHERE i > 5000 AND i <= 5006;
┌───────────────────────────┐
│       BIGQUERY_SCAN       │
│    ────────────────────   │
│           Table:          │
│      my-gcp-project.      │
│        my_dataset.        │
│      filter_pushdown      │
│                           │
│         Read Mode:        │
│        Storage Read       │
│                           │
│      Projections: i       │
│                           │
│          Filters:         │
│   i>5000 AND i<=5006      │
└───────────────────────────┘
```

The `Filters:` entry inside `BIGQUERY_SCAN` confirms that the condition is sent
to BigQuery. A separate `FILTER` operator means that DuckDB still evaluates
that part locally. Supported filters include:

- Comparisons with Boolean, numeric, string, `DATE`, `TIME`, or `TIMESTAMP`
  literals: `=`, `!=`, `<`, `<=`, `>`, and `>=`
- `IN (...)`
- `IS NULL` and `IS NOT NULL`
- Combinations with `AND` and `OR`
- Comparisons on nested `STRUCT` fields

### Experimental Aggregate Pushdown

The `bq_enable_aggregate_pushdown` setting enables an experimental optimizer
rewrite for supported aggregate queries over BigQuery sources.

Instead of reading all source rows through the Storage Read API and aggregating
them in DuckDB, the optimizer can translate supported aggregates, filters, and
grouping expressions to GoogleSQL and execute them through `bigquery_query`.
Use `EXPLAIN` to check the execution path for a particular query:

```sql
-- Enable experimental aggregate pushdown.
SET bq_enable_aggregate_pushdown = true;

-- Inspect where the aggregation will be executed.
EXPLAIN
  SELECT i, COUNT(*)
  FROM bq.my_dataset.aggregate_pushdown
  GROUP BY i;
┌───────────────────────────┐
│       BIGQUERY_QUERY      │
│    ────────────────────   │
│           Query:          │
│  SELECT `i` AS            │
│  __duckdb_bq_group_0,     │
│  COUNT(*) AS              │
│  __duckdb_bq_aggr_0 FROM  │
│  `my-gcp-project.         │
│  my_dataset.              │
│  aggregate_pushdown`      │
│  GROUP BY `i`             │
│                           │
│         Type: REST        │
│                           │
│           ~1 row          │
└───────────────────────────┘
```

When pushdown succeeds, the physical plan contains a `BIGQUERY_QUERY` operator
and shows the generated GoogleSQL. The corresponding `BIGQUERY_SCAN` and local
`GROUP_BY` operators are absent because BigQuery performs the aggregation.
If the query shape is unsupported, the optimizer leaves the local DuckDB plan
in place before starting a remote query.

The setting is disabled by default. Runtime errors from a started BigQuery job
are not retried with a local plan, and GoogleSQL expression semantics can
differ from DuckDB semantics. See
[Additional Settings](configuration.md#settings) for the
setting reference.

## Reading from Public and External Datasets

Public datasets and datasets owned by another Google Cloud project separate
the storage project from the project that supplies quota and receives
applicable charges. In an attachment, `project` identifies where the data
lives, while `billing_project` identifies the project used for billing and
quota:

=== "Project ID"

    ```sql
    -- Read the public table directly and bill the configured project.
    SELECT count(*)
      FROM bigquery_scan(
          'bigquery-public-data.geo_us_boundaries.cnecta',
          billing_project := 'my-gcp-project'
      );
    ┌──────────────┐
    │ count_star() │
    │ int64        │
    ├──────────────┤
    │            7 │
    └──────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the public dataset and specify the billing project.
    ATTACH 'project=bigquery-public-data dataset=geo_us_boundaries billing_project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Query the public dataset through the attached catalog.
    SELECT count(*)
      FROM bq.geo_us_boundaries.cnecta;
    ┌──────────────┐
    │ count_star() │
    │ int64        │
    ├──────────────┤
    │            7 │
    └──────────────┘
    ```

The attachment stores this configuration, so subsequent reads through
`bq` reuse the same billing project. For a one-off
`bigquery_scan`, pass the same value with the `billing_project` named
parameter. `billing_project` does not grant access: the active credentials
still need the required IAM permissions.

Here, “external” means a dataset in another project. Ordinary BigQuery
external tables are not supported by direct Storage Read scans; query them
with `bigquery_query`.
