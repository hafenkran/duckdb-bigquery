# Managing Tables & Datasets

BigQuery datasets appear as schemas in an attached DuckDB catalog. The
attached DDL surface uses `SCHEMA` statements to create and delete BigQuery
datasets, and `TABLE` statements to create, alter, and delete tables.

The examples assume that `my-gcp-project` is attached as the writable catalog
`bq`. See [Attaching Projects](attach.md) for attachment and
access-mode configuration. The active identity also needs the corresponding
[permissions](../getting-started/required-permissions.md).

## Managing Datasets

### Create a Dataset

Use DuckDB's `CREATE SCHEMA` syntax to create a BigQuery dataset:

```sql
-- Create my-gcp-project.my_dataset.
CREATE SCHEMA bq.my_dataset;
```

The dataset is created in `my-gcp-project`. Its location is controlled by
`bq_default_location`. `CREATE OR REPLACE SCHEMA` and `ALTER SCHEMA` are not
supported by the attached catalog.

A project-scoped attachment is the most useful form for dataset management
because the newly created dataset can then be discovered through the same
catalog.

### Create a Dataset with Options

`CREATE SCHEMA ... OPTIONS` is supported only when the additional experimental
flag `bq_experimental_enable_sql_parser` is enabled. BigQuery defines the
available names, value types, and restrictions in its
[schema option list](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#schema_option_list).

The `OPTIONS` clause configures dataset properties as part of the
`CREATE SCHEMA` statement. Specify each property as `name=value` and separate
multiple properties with commas:

```sql
-- Run the DDL job in the same location as the new dataset.
SET bq_default_location = 'EU';

-- Enable BigQuery-specific CREATE SCHEMA clauses.
SET bq_experimental_enable_sql_parser = true;

-- Create a BigQuery dataset with several string-valued options.
CREATE SCHEMA bq.my_options_dataset
  OPTIONS (
      location='EU',
      friendly_name='DuckDB analytics',
      description='Dataset managed through DuckDB',
      default_rounding_mode='ROUND_HALF_EVEN',
      storage_billing_model='LOGICAL'
  );
```

The options in this example have these effects:

| Option | Effect |
| --- | --- |
| `location` | Creates the dataset in `EU`. For attached DDL, keep this equal to `bq_default_location`, which controls the location of the query job. |
| `friendly_name` | Sets a human-readable dataset name without changing the dataset ID used in SQL. |
| `description` | Stores descriptive metadata for the dataset. |
| `default_rounding_mode` | Sets the rounding mode inherited by supported numeric fields in newly created tables. It does not change existing tables. |
| `storage_billing_model` | Selects logical or physical storage billing. This example keeps BigQuery's `LOGICAL` model. |

The extension first extracts the `OPTIONS` clause so DuckDB can parse the
remaining statement. It then attaches the parsed key-value pairs to the schema
create operation, reconstructs a BigQuery `CREATE SCHEMA` statement, and sends
it to the attached project. BigQuery performs the final validation and returns
an error for an unknown option, an invalid value, a location mismatch, or
insufficient permissions.

The attached DDL parser currently represents option values as strings.
Consequently, use this form for string-valued options such as those shown
above. BigQuery also supports options with other value types, including numeric
expiration periods, booleans, and arrays. Send those as unmodified GoogleSQL
with [`bigquery_execute`](../function-reference/bigquery-execute.md):

=== "Project ID"

    ```sql
    -- Use raw GoogleSQL when option values are not strings.
    SELECT *
      FROM bigquery_execute(
          'my-gcp-project',
          'CREATE SCHEMA `my-gcp-project.advanced_dataset`
           OPTIONS (
               location="EU",
               default_table_expiration_days=30,
               default_partition_expiration_days=7,
               is_case_insensitive=TRUE,
               labels=[
                   ("environment", "development"),
                   ("owner", "data_team")
               ]
           )'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬───────────────────────┐
    │ success │ job_id          │ project_id     │ … │ num_dml_affected_rows │
    │ boolean │ varchar         │ varchar        │ … │ bigint                │
    ├─────────┼─────────────────┼────────────────┼───┼───────────────────────┤
    │ true    │ execute_job_123 │ my-gcp-project │ … │                     0 │
    └─────────┴─────────────────┴────────────────┴───┴───────────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Forward the same GoogleSQL through the attached catalog.
    SELECT *
      FROM bigquery_execute(
          'bq',
          'CREATE SCHEMA `my-gcp-project.advanced_dataset`
           OPTIONS (
               location="EU",
               default_table_expiration_days=30,
               default_partition_expiration_days=7,
               is_case_insensitive=TRUE,
               labels=[
                   ("environment", "development"),
                   ("owner", "data_team")
               ]
           )'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬───────────────────────┐
    │ success │ job_id          │ project_id     │ … │ num_dml_affected_rows │
    │ boolean │ varchar         │ varchar        │ … │ bigint                │
    ├─────────┼─────────────────┼────────────────┼───┼───────────────────────┤
    │ true    │ execute_job_234 │ my-gcp-project │ … │                     0 │
    └─────────┴─────────────────┴────────────────┴───┴───────────────────────┘
    ```

`bigquery_execute` forwards the SQL string without interpreting its
`OPTIONS` clause, so BigQuery receives the numeric, boolean, and array values
with their original types.

### Delete a Dataset

By default, dropping a schema uses `RESTRICT` and fails while the dataset still
contains tables or other objects:

```sql
-- Delete only an empty dataset.
DROP SCHEMA bq.my_dataset RESTRICT;
```

Use `CASCADE` only when the dataset and all of its contents should be removed:

```sql
-- Delete the dataset and all of its contents.
DROP SCHEMA IF EXISTS bq.my_dataset CASCADE;
```

`CASCADE` is destructive and cannot be undone by DuckDB.

## Managing Tables

### Create a Table

Create an empty table when its columns should be defined explicitly:

```sql
-- Create an empty BigQuery table with an explicit schema.
CREATE TABLE bq.my_dataset.events (
      event_id BIGINT NOT NULL,
      event_name VARCHAR DEFAULT 'unknown',
      event_date DATE,
      created_at TIMESTAMP
  );

-- Inspect the created table's columns.
DESCRIBE TABLE bq.my_dataset.events;
┌─────────────┬─────────────┬──────┬──────┬───────────┬───────┐
│ column_name │ column_type │ null │ key  │  default  │ extra │
├─────────────┼─────────────┼──────┼──────┼───────────┼───────┤
│ event_id    │ BIGINT      │ NO   │ NULL │ NULL      │ NULL  │
│ event_name  │ VARCHAR     │ YES  │ NULL │ 'unknown' │ NULL  │
│ event_date  │ DATE        │ YES  │ NULL │ NULL      │ NULL  │
│ created_at  │ TIMESTAMP   │ YES  │ NULL │ NULL      │ NULL  │
└─────────────┴─────────────┴──────┴──────┴───────────┴───────┘
```

The supported conflict forms are:

```sql
-- Keep the existing table when it already exists.
CREATE TABLE IF NOT EXISTS bq.my_dataset.events (
      event_id BIGINT
  );

-- Drop the existing table and create its replacement.
CREATE OR REPLACE TABLE bq.my_dataset.events (
      event_id BIGINT,
      event_name VARCHAR
  );
```

`CREATE OR REPLACE TABLE` first drops the existing table and then creates the
replacement. Existing rows are deleted, and the two remote operations are not
atomic. Use `IF NOT EXISTS` when an existing table should be retained.

Column defaults and `NOT NULL` are supported. `PRIMARY KEY`, foreign keys,
`UNIQUE`, `CHECK`, indexes, and other DuckDB constraint forms are not part of
the attached table creation surface. Review
[Data Type Mapping](../concepts/data-types.md) before creating columns with nested,
unsigned, high-precision, or geography types.

To create a table and write a query result in one operation, use
[CTAS](writing.md#write-a-query-result-with-ctas).

### Create a Table with Options

`CREATE TABLE ... OPTIONS` is supported only when the additional experimental
flag `bq_experimental_enable_sql_parser` is enabled. BigQuery defines the
available names, value types, and restrictions in its
[table option list](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list).

The `OPTIONS` clause configures table properties as part of the
`CREATE TABLE` statement. Specify each property as `name=value` and separate
multiple properties with commas:

```sql
-- Enable BigQuery-specific CREATE TABLE options.
SET bq_experimental_enable_sql_parser = true;

-- Create a table with descriptive metadata.
CREATE TABLE bq.my_dataset.events_with_options (
      event_id BIGINT,
      event_name VARCHAR,
      created_at TIMESTAMP
  )
  OPTIONS (
      friendly_name='DuckDB events',
      description='Events created through DuckDB'
  );
```

The extension extracts the `OPTIONS` clause before DuckDB parses the remaining
statement, reattaches the parsed key-value pairs, and sends the reconstructed
`CREATE TABLE` statement to BigQuery. BigQuery validates every option and
value.

As with schema options, the attached DDL parser currently represents option
values as strings. Use it for string-valued options such as `friendly_name`,
`description`, and `kms_key_name`. Use
[`bigquery_execute`](../function-reference/bigquery-execute.md) when an option
requires a timestamp expression, number, boolean, or array so that BigQuery
receives the original GoogleSQL value type.

### Partition and Cluster a Table

`PARTITION BY` and `CLUSTER BY` are also supported through the additional
experimental flag `bq_experimental_enable_sql_parser`. They control how
BigQuery organizes table data rather than setting table metadata:

```sql
-- Enable BigQuery-specific CREATE TABLE clauses.
SET bq_experimental_enable_sql_parser = true;

-- Partition events by date and cluster each partition by customer.
CREATE TABLE bq.my_dataset.partitioned_events (
      event_date DATE,
      customer_id BIGINT,
      event_name VARCHAR
  )
  PARTITION BY event_date
  CLUSTER BY customer_id;

```

Verify the resulting metadata through either connection form:

=== "Project ID"

    ```sql
    -- Query the table metadata using a project ID.
    SELECT
          column_name,
          is_partitioning_column,
          clustering_ordinal_position
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT
             column_name,
             is_partitioning_column,
             clustering_ordinal_position
           FROM
             `my-gcp-project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
           WHERE table_name = "partitioned_events"
           ORDER BY column_name'
      );
    ┌─────────────┬────────────────────────┬─────────────────────────────┐
    │ column_name │ is_partitioning_column │ clustering_ordinal_position │
    │ varchar     │ varchar                │ int64                       │
    ├─────────────┼────────────────────────┼─────────────────────────────┤
    │ customer_id │ NO                     │                           1 │
    │ event_date  │ YES                    │                        NULL │
    │ event_name  │ NO                     │                        NULL │
    └─────────────┴────────────────────────┴─────────────────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Query the same metadata through the attached catalog.
    SELECT
          column_name,
          is_partitioning_column,
          clustering_ordinal_position
      FROM bigquery_query(
          'bq',
          'SELECT
             column_name,
             is_partitioning_column,
             clustering_ordinal_position
           FROM
             `my-gcp-project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
           WHERE table_name = "partitioned_events"
           ORDER BY column_name'
      );
    ┌─────────────┬────────────────────────┬─────────────────────────────┐
    │ column_name │ is_partitioning_column │ clustering_ordinal_position │
    │ varchar     │ varchar                │ int64                       │
    ├─────────────┼────────────────────────┼─────────────────────────────┤
    │ customer_id │ NO                     │                           1 │
    │ event_date  │ YES                    │                        NULL │
    │ event_name  │ NO                     │                        NULL │
    └─────────────┴────────────────────────┴─────────────────────────────┘
    ```

The parser extracts both clauses and adds them to the BigQuery-targeted
`CREATE TABLE` or CTAS statement after DuckDB has parsed the remaining SQL.
BigQuery performs the final validation of the partition expression and
clustering columns. Ingestion-time partitioning with `_PARTITIONDATE` is also
accepted.

### Alter a Table

The attached catalog supports these `ALTER TABLE` forms:

| Form | Effect |
| --- | --- |
| `RENAME COLUMN old TO new` | Rename one column |
| `RENAME TO new_table` | Rename the table within its dataset |
| `ADD COLUMN [IF NOT EXISTS] name type` | Add one column |
| `DROP COLUMN [IF EXISTS] name` | Remove one column |
| `ALTER COLUMN name TYPE type` | Change the type when BigQuery considers it assignment-compatible |
| `ALTER COLUMN name SET DEFAULT expression` | Set the column default |
| `ALTER COLUMN name DROP NOT NULL` | Relax a required column |

The operations can be applied sequentially:

```sql
-- Rename a column.
ALTER TABLE bq.my_dataset.events
  RENAME COLUMN event_name TO event_type;

-- Add a nullable column.
ALTER TABLE bq.my_dataset.events
  ADD COLUMN source VARCHAR;

-- Change to an assignment-compatible type.
ALTER TABLE bq.my_dataset.events
  ALTER COLUMN event_id TYPE DOUBLE;

-- Define a new default for future inserts.
ALTER TABLE bq.my_dataset.events
  ALTER COLUMN event_type SET DEFAULT 'unknown';

-- Remove a column.
ALTER TABLE bq.my_dataset.events
  DROP COLUMN source;

-- Rename the table within my_dataset.
ALTER TABLE bq.my_dataset.events
  RENAME TO archived_events;

-- Inspect the altered table's columns.
DESCRIBE TABLE bq.my_dataset.archived_events;
┌─────────────┬─────────────┬──────┬──────┬───────────┬───────┐
│ column_name │ column_type │ null │ key  │  default  │ extra │
├─────────────┼─────────────┼──────┼──────┼───────────┼───────┤
│ event_id    │ DOUBLE      │ NO   │ NULL │ NULL      │ NULL  │
│ event_type  │ VARCHAR     │ YES  │ NULL │ 'unknown' │ NULL  │
│ event_date  │ DATE        │ YES  │ NULL │ NULL      │ NULL  │
│ created_at  │ TIMESTAMP   │ YES  │ NULL │ NULL      │ NULL  │
└─────────────┴─────────────┴──────┴──────┴───────────┴───────┘
```

BigQuery rejects incompatible type changes. Other DuckDB `ALTER TABLE` forms
raise an unsupported-operation error instead of being translated.

### Delete a Table

Drop a table to permanently remove it and its rows. `IF EXISTS` suppresses the
error when the table is already absent:

```sql
-- Fail when the table does not exist.
DROP TABLE bq.my_dataset.archived_events;

-- Do nothing when the table does not exist.
DROP TABLE IF EXISTS bq.my_dataset.archived_events;
```

## Metadata and Execution Boundaries

Successful creates and drops update the corresponding local catalog entry.
After `ALTER TABLE`, the extension invalidates table metadata for the schema so
that later lookups retrieve the changed definition. If a table or dataset was
changed outside DuckDB and remains stale, use
[`bigquery_clear_cache`](attach.md#refresh-metadata-and-detach).

Each DDL statement runs as a remote BigQuery operation. A later DuckDB
`ROLLBACK` does not undo a completed create, alter, or drop. `READ_ONLY`
attachments reject every operation in this chapter.
