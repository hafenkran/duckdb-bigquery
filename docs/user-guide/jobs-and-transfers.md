# Executing & Monitoring Jobs

Use the job functions when BigQuery should execute a statement, estimate a
query, or expose job metadata. These functions can accept a project ID or an
attached catalog as documented in their individual references.

- [`bigquery_execute`](#execute-googlesql): Run GoogleSQL as a query job and
  return execution metadata.
- [`bigquery_jobs`](#monitor-jobs): List or inspect jobs through the Jobs REST
  API.

For data transfers with `bigquery_load` and `bigquery_extract`, see
[Extract & Load](extract-and-load.md).

Use [`bigquery_query`](reading-and-queries.md#bigquery-query) when query rows are
the desired result. Prefer attached
[table and dataset management](managing-tables-and-schemas.md) or
[Writing & Modifying Data](writing.md) when the attached DuckDB SQL
surface is sufficient.

## Execute GoogleSQL

`bigquery_execute` returns execution metadata instead of query rows. Use it for
GoogleSQL DDL, DML, scripts, or materializing a query into a destination table:

=== "Project ID"

    ```sql
    -- Create a BigQuery table and return the DDL job metadata.
    SELECT *
      FROM bigquery_execute(
          'my-gcp-project',
          'CREATE TABLE IF NOT EXISTS
           `my-gcp-project.my_dataset.example` (id INT64)'
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
    -- Attach the writable dataset.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery);

    -- Run the same DDL job through the attached catalog.
    SELECT *
      FROM bigquery_execute(
          'bq',
          'CREATE TABLE IF NOT EXISTS
           `my-gcp-project.my_dataset.example` (id INT64)'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬───────────────────────┐
    │ success │ job_id          │ project_id     │ … │ num_dml_affected_rows │
    │ boolean │ varchar         │ varchar        │ … │ bigint                │
    ├─────────┼─────────────────┼────────────────┼───┼───────────────────────┤
    │ true    │ execute_job_234 │ my-gcp-project │ … │                     0 │
    └─────────┴─────────────────┴────────────────┴───┴───────────────────────┘
    ```

Use `SELECT * FROM bigquery_execute(...)` when metadata such as the job ID or
affected-row count is needed.

Materialize a query result with destination options:

=== "Project ID"

    ```sql
    -- Materialize an aggregated query result in a destination table.
    SELECT *
      FROM bigquery_execute(
          'my-gcp-project',
          'SELECT event_date, COUNT(*) AS event_count
           FROM `my-gcp-project.my_dataset.events`
           GROUP BY event_date',
          destination_table := 'my_dataset.daily_event_counts',
          write_disposition := 'WRITE_TRUNCATE'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬────────────┬───────────────────────┐
    │ success │ job_id          │ project_id     │ … │ total_rows │ num_dml_affected_rows │
    │ boolean │ varchar         │ varchar        │ … │ uint64     │ bigint                │
    ├─────────┼─────────────────┼────────────────┼───┼────────────┼───────────────────────┤
    │ true    │ execute_job_345 │ my-gcp-project │ … │        365 │ NULL                  │
    └─────────┴─────────────────┴────────────────┴───┴────────────┴───────────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the writable dataset.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery);

    -- Materialize the query result through the attached catalog.
    SELECT *
      FROM bigquery_execute(
          'bq',
          'SELECT event_date, COUNT(*) AS event_count
           FROM `my-gcp-project.my_dataset.events`
           GROUP BY event_date',
          destination_table := 'my_dataset.daily_event_counts',
          write_disposition := 'WRITE_TRUNCATE'
      );
    ┌─────────┬─────────────────┬────────────────┬───┬────────────┬───────────────────────┐
    │ success │ job_id          │ project_id     │ … │ total_rows │ num_dml_affected_rows │
    │ boolean │ varchar         │ varchar        │ … │ uint64     │ bigint                │
    ├─────────┼─────────────────┼────────────────┼───┼────────────┼───────────────────────┤
    │ true    │ execute_job_456 │ my-gcp-project │ … │        365 │ NULL                  │
    └─────────┴─────────────────┴────────────────┴───┴────────────┴───────────────────────┘
    ```

Destination materialization and dry runs cannot be combined. See the
[`bigquery_execute` reference](../function-reference/bigquery-execute.md) for
dispositions, defaults, result columns, and validation rules.

## Dry Runs

`bigquery_query` and `bigquery_execute` accept `dry_run := true`:

=== "Project ID"

    ```sql
    -- Estimate the bytes processed without executing the query.
    SELECT *
      FROM bigquery_query(
          'my-gcp-project',
          'SELECT * FROM `my-gcp-project.my_dataset.events`',
          dry_run := true
      );
    ┌───────────────────────┬───────────┬──────────┐
    │ total_bytes_processed │ cache_hit │ location │
    │ bigint                │ boolean   │ varchar  │
    ├───────────────────────┼───────────┼──────────┤
    │               1048576 │ false     │ EU       │
    └───────────────────────┴───────────┴──────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the dataset whose query should be estimated.
    ATTACH 'project=my-gcp-project dataset=my_dataset'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Reuse the attached catalog configuration for the dry run.
    SELECT *
      FROM bigquery_query(
          'bq',
          'SELECT * FROM `my-gcp-project.my_dataset.events`',
          dry_run := true
      );
    ┌───────────────────────┬───────────┬──────────┐
    │ total_bytes_processed │ cache_hit │ location │
    │ bigint                │ boolean   │ varchar  │
    ├───────────────────────┼───────────┼──────────┤
    │               1048576 │ false     │ EU       │
    └───────────────────────┴───────────┴──────────┘
    ```

The result reports BigQuery's processed-byte estimate, cache status, and job
location. The estimate describes BigQuery query processing, not the result
data later transferred to DuckDB.

## Monitor Jobs

List recent jobs:

=== "Project ID"

    ```sql
    -- List the 100 most recent completed jobs.
    SELECT
          job_id,
          state,
          job_type,
          creation_time,
          bytes_processed
      FROM bigquery_jobs(
          'my-gcp-project',
          stateFilter := 'DONE',
          maxResults := 100
      )
      ORDER BY creation_time DESC;
    ┌─────────────┬───────────┬──────────┬─────────────────────┬─────────────────┐
    │ job_id      │ state     │ job_type │ creation_time       │ bytes_processed │
    │ varchar     │ varchar   │ varchar  │ timestamp           │ bigint          │
    ├─────────────┼───────────┼──────────┼─────────────────────┼─────────────────┤
    │ query_job_1 │ Completed │ QUERY    │ 2026-07-01 12:00:00 │         1048576 │
    └─────────────┴───────────┴──────────┴─────────────────────┴─────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the project whose jobs should be listed.
    ATTACH 'project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- List jobs using the attached catalog configuration.
    SELECT
          job_id,
          state,
          job_type,
          creation_time,
          bytes_processed
      FROM bigquery_jobs(
          'bq',
          stateFilter := 'DONE',
          maxResults := 100
      )
      ORDER BY creation_time DESC;
    ┌─────────────┬───────────┬──────────┬─────────────────────┬─────────────────┐
    │ job_id      │ state     │ job_type │ creation_time       │ bytes_processed │
    │ varchar     │ varchar   │ varchar  │ timestamp           │ bigint          │
    ├─────────────┼───────────┼──────────┼─────────────────────┼─────────────────┤
    │ query_job_1 │ Completed │ QUERY    │ 2026-07-01 12:00:00 │         1048576 │
    └─────────────┴───────────┴──────────┴─────────────────────┴─────────────────┘
    ```

Inspect one job:

=== "Project ID"

    ```sql
    -- Inspect one BigQuery job by its ID.
    SELECT
          state,
          job_id,
          project,
          location,
          job_type,
          status
      FROM bigquery_jobs(
          'my-gcp-project',
          jobId := 'my_job_id'
      );
    ┌───────────┬───────────┬────────────────┬──────────┬──────────┬──────────────────┐
    │ state     │ job_id    │ project        │ location │ job_type │ status           │
    │ varchar   │ varchar   │ varchar        │ varchar  │ varchar  │ json             │
    ├───────────┼───────────┼────────────────┼──────────┼──────────┼──────────────────┤
    │ Completed │ my_job_id │ my-gcp-project │ EU       │ QUERY    │ {"state":"DONE"} │
    └───────────┴───────────┴────────────────┴──────────┴──────────┴──────────────────┘
    ```

=== "Attached catalog"

    ```sql
    -- Attach the project that owns the job.
    ATTACH 'project=my-gcp-project'
      AS bq (TYPE bigquery, READ_ONLY);

    -- Inspect the job using the attached catalog configuration.
    SELECT
          state,
          job_id,
          project,
          location,
          job_type,
          status
      FROM bigquery_jobs(
          'bq',
          jobId := 'my_job_id'
      );
    ┌───────────┬───────────┬────────────────┬──────────┬──────────┬──────────────────┐
    │ state     │ job_id    │ project        │ location │ job_type │ status           │
    │ varchar   │ varchar   │ varchar        │ varchar  │ varchar  │ json             │
    ├───────────┼───────────┼────────────────┼──────────┼──────────┼──────────────────┤
    │ Completed │ my_job_id │ my-gcp-project │ EU       │ QUERY    │ {"state":"DONE"} │
    └───────────┴───────────┴────────────────┴──────────┴──────────┴──────────────────┘
    ```

Parameter names mirror the official `jobs.list` API and intentionally use
camel case. See the
[`bigquery_jobs` reference](../function-reference/bigquery-jobs.md) for filters,
defaults, result columns, and endpoint rules.

## Timeouts and Costs

Function-level `timeout_ms` and `bq_query_timeout_ms` control local waiting.
They do not guarantee cancellation: a timed-out query, load, or extract can
continue running and incurring charges.

The storage project identifies data, while the billing project consumes quota
and receives applicable charges. Direct Storage Read and GoogleSQL query jobs
have different cost models. Use restrictive predicates, dry runs,
dataset-scoped attachments, and dedicated billing projects where appropriate.

Review the canonical [Billing and Costs](../index.md#billing-and-costs) warning
before submitting work.
