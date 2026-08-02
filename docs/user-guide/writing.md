# Writing & Modifying Data

Use regular DuckDB SQL against a writable BigQuery attachment to add, change,
or remove rows. `INSERT` appends rows, `UPDATE` changes existing rows, and
`DELETE` removes rows. CTAS (`CREATE TABLE ... AS SELECT`) creates or replaces
a table from a query result.

The execution path depends on the operation:

- [`INSERT`](#insert) and CTAS stream DuckDB rows through the BigQuery Storage
  Write API. The extension finalizes and commits the write before the statement
  returns.
- [`UPDATE`](#update) and [`DELETE`](#delete) are translated to GoogleSQL and
  submitted as BigQuery query jobs. Matching rows stay in BigQuery, and DuckDB
  receives the affected-row count when the job completes.

The examples assume that `my-gcp-project.my_dataset` is already attached as
the writable catalog `bq`. See [Attaching Projects](attach.md) for
attachment and access-mode configuration. Attachments created with `READ_ONLY`
reject every operation on this page.

## INSERT

### Insert Values

Use `INSERT ... VALUES` to append literal rows:

```sql
-- Append two events to an existing BigQuery table.
INSERT INTO bq.my_dataset.events (
      event_id,
      event_name,
      event_date
  )
  VALUES
      (10001, 'signup', DATE '2026-07-01'),
      (10002, 'purchase', DATE '2026-07-01');

-- Verify the inserted rows.
SELECT
      event_id,
      event_name,
      event_date
  FROM bq.my_dataset.events
  WHERE event_id >= 10001
  ORDER BY event_id;
┌──────────┬────────────┬────────────┐
│ event_id │ event_name │ event_date │
│  int64   │  varchar   │    date    │
├──────────┼────────────┼────────────┤
│    10001 │ signup     │ 2026-07-01 │
│    10002 │ purchase   │ 2026-07-01 │
└──────────┴────────────┴────────────┘
```

An explicit column list makes the mapping clear and allows columns with
defaults to be omitted. A successful attached `INSERT` does not emit a result
row. Run a separate query, as shown above, when the written data or row count
needs to be verified.

### Insert a Query Result

`INSERT ... SELECT` appends the result of a DuckDB query:

```sql
-- Read local Parquet files and append their rows to BigQuery.
INSERT INTO bq.my_dataset.events
  SELECT
      event_id::BIGINT,
      event_name::VARCHAR,
      event_date::DATE
  FROM read_parquet('/absolute/path/events/new/*.parquet')
  WHERE event_date = DATE '2026-07-02';

-- Count the rows written for that date.
SELECT count(*) AS written_rows
  FROM bq.my_dataset.events
  WHERE event_date = DATE '2026-07-02';
┌──────────────┐
│ written_rows │
│    int64     │
├──────────────┤
│          250 │
└──────────────┘
```

Source and destination columns are matched by position unless an explicit
target column list is provided. Their types must be compatible.

## UPDATE

Use `UPDATE` to change values in existing rows:

```sql
-- Rename signup events.
UPDATE bq.my_dataset.events
  SET event_name = 'registered'
  WHERE event_name = 'signup';
┌───────┐
│ Count │
│ int64 │
├───────┤
│     1 │
└───────┘
```

The statement returns the number of updated rows as a `BIGINT`. Assignments,
scalar predicates, and supported nested struct paths are translated to
GoogleSQL:

```sql
-- Mark customers whose nested address contains New York.
UPDATE bq.my_dataset.customers
  SET marker = 9
  WHERE customer_info.address.city = 'New York';
┌───────┐
│ Count │
│ int64 │
├───────┤
│     2 │
└───────┘
```

An `UPDATE` without a `WHERE` clause applies to every row. The generated
GoogleSQL uses `WHERE true` because BigQuery requires an explicit condition.

## DELETE

Use `DELETE` to remove rows:

```sql
-- Remove events older than 2025.
DELETE FROM bq.my_dataset.events
  WHERE created_at < TIMESTAMP '2025-01-01';
┌───────┐
│ Count │
│ int64 │
├───────┤
│    42 │
└───────┘
```

The statement returns the number of deleted rows as a `BIGINT`. A `DELETE`
without a `WHERE` clause removes every row; the generated GoogleSQL again uses
`WHERE true`.

## Write a Query Result with CTAS

CTAS combines table creation with `INSERT ... SELECT` and infers the
destination schema from the query result.

For example, read Parquet files with DuckDB, select the required columns, and
write the result directly to BigQuery:

```sql
-- Create or replace a BigQuery table from local Parquet files.
CREATE OR REPLACE TABLE bq.my_dataset.events AS
  SELECT
      event_id::BIGINT AS event_id,
      event_name::VARCHAR AS event_name,
      event_date::DATE AS event_date
  FROM read_parquet('/absolute/path/events/*.parquet')
  WHERE event_date >= DATE '2026-01-01';

-- Count all rows written to the destination table.
SELECT count(*) AS written_rows
  FROM bq.my_dataset.events;
┌──────────────┐
│ written_rows │
│    int64     │
├──────────────┤
│        10000 │
└──────────────┘
```

The source is a regular DuckDB query. It can read local tables, files, attached
catalogs, or a combination of them, and can filter, join, aggregate, or cast
the rows before they are written.

Both `CREATE TABLE ... AS` and `CREATE OR REPLACE TABLE ... AS` are supported.
The latter replaces an existing destination, so use it only when overwriting
the table is intentional. BigQuery-specific
[table options](managing-tables-and-schemas.md#create-a-table-with-options) and
[partitioning and clustering](managing-tables-and-schemas.md#partition-and-cluster-a-table)
are documented under [Managing Tables & Datasets](managing-tables-and-schemas.md).

## Type Conversion

CTAS derives the BigQuery schema from the DuckDB result types. `INSERT`
converts each source column to the corresponding destination type. Cast source
columns explicitly when inference would produce an unsupported or unintended
type.

For example, DuckDB returns `HUGEINT` for `sum(INTEGER)`, while attached
BigQuery table creation does not support `HUGEINT`:

```sql
-- Cast an aggregate to a supported destination type.
CREATE OR REPLACE TABLE bq.my_dataset.daily_totals AS
  SELECT
      event_date,
      sum(event_count)::BIGINT AS event_count
  FROM local_event_counts
  GROUP BY event_date;
```

Common scalar types, lists, structs, and geography values are supported. See
[Data Type Mapping](../concepts/data-types.md) for the complete mappings and
[Working with Geometries](geometry-support.md) for CRS and spherical-geography
considerations.

## Limitations and Safety

The attached write surface does not support:

- `RETURNING` on `INSERT`, `UPDATE`, or `DELETE`;
- `INSERT ... ON CONFLICT`;
- complex `UPDATE` or `DELETE` filters that cannot be translated without
  changing their meaning;
- incompatible `INSERT` source and destination columns;
- unsupported DuckDB write types such as `HUGEINT`, `UHUGEINT`, `UBIGINT`,
  `TIMESTAMP_TZ`, and `TIMESTAMP_NS`.

Use [`bigquery_execute`](jobs-and-transfers.md#execute-googlesql) for
compatible BigQuery operations that are not represented by the attached
catalog. Use [`bigquery_load`](extract-and-load.md#load-data) for a BigQuery
load job from local files, Cloud Storage objects, or a DuckDB table.

Remote writes are not part of a multi-statement transaction that can be rolled
back in DuckDB. Use explicit, selective conditions for updates and deletes,
and review [Billing and Costs](../index.md#billing-and-costs) before running
broad operations. See
[Required Permissions](../getting-started/required-permissions.md) for IAM
guidance.
