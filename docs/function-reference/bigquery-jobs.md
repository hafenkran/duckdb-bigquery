# `bigquery_jobs`

Lists BigQuery jobs or retrieves one job by ID. The function performs metadata
requests and does not create a query job.

For dry-run and monitoring workflows, see
[Monitor Jobs](../user-guide/jobs-and-transfers.md#monitor-jobs).

## Signature

```sql
SELECT *
  FROM bigquery_jobs(
      'PROJECT_OR_CATALOG',
      jobId := 'JOB_ID',
      allUsers := false,
      maxResults := 1000,
      minCreationTime := '2026-01-01 00:00:00',
      maxCreationTime := '2026-02-01 00:00:00',
      stateFilter := 'DONE',
      parentJobId := 'PARENT_JOB_ID',
      api_endpoint := 'BIGQUERY_REST_ENDPOINT'
  );
```

Parameter names mirror the official
[`jobs.list` API](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list)
and intentionally use camel case.

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `project_or_catalog` | `VARCHAR` | required | Google Cloud project ID or attached BigQuery catalog. |
| `jobId` | `VARCHAR` | none | Retrieve one job instead of listing jobs. |
| `allUsers` | `BOOLEAN` | BigQuery default | Include other users' jobs when permitted. |
| `maxResults` | `INTEGER` | `1000` | Maximum rows to collect. |
| `minCreationTime` | `VARCHAR` | none | Timestamp-like lower creation-time bound. |
| `maxCreationTime` | `VARCHAR` | none | Timestamp-like upper creation-time bound. |
| `stateFilter` | `VARCHAR` | none | `DONE`, `PENDING`, or `RUNNING`, case-insensitive. |
| `parentJobId` | `VARCHAR` | none | List child jobs for a script or parent job. |
| `api_endpoint` | `VARCHAR` | Google default | BigQuery REST endpoint override for a direct project call. |

An attached catalog supplies its own REST endpoint. Do not combine a catalog
name with the function-level `api_endpoint` parameter.

## Example

Store the metadata returned by `bigquery_execute`, then use its job ID to find
the corresponding job. This is the same list-and-filter flow covered by the
jobs SQLLogicTest.

```sql
-- Submit a query and retain its returned job ID.
CREATE TEMP TABLE submitted_job AS
SELECT *
  FROM bigquery_execute(
      'my-gcp-project',
      'SELECT 1 AS result'
  );

-- Locate the submitted job in recent job metadata.
SELECT
      state,
      job_type,
      status
  FROM bigquery_jobs(
      'my-gcp-project',
      maxResults := 10
  )
  WHERE job_id = (SELECT job_id FROM submitted_job);
┌───────────┬──────────┬──────────────────┐
│   state   │ job_type │      status      │
│  varchar  │ varchar  │       json       │
├───────────┼──────────┼──────────────────┤
│ Completed │ QUERY    │ {"state":"DONE"} │
└───────────┴──────────┴──────────────────┘
```

## Result

| Column | Type | Description |
| --- | --- | --- |
| `state` | `VARCHAR` | Mapped state such as `Completed`, `Error`, `Queued`, or `Active`. |
| `job_id` | `VARCHAR` | Job ID. |
| `project` | `VARCHAR` | Job project. |
| `location` | `VARCHAR` | Job location. |
| `creation_time` | `TIMESTAMP` | Creation time. |
| `start_time` | `TIMESTAMP` | Start time. |
| `end_time` | `TIMESTAMP` | End time. |
| `duration_ms` | `INTERVAL` | Elapsed interval. |
| `bytes_processed` | `BIGINT` | Processed bytes when available. |
| `total_slot_time_ms` | `BIGINT` | Total slot milliseconds when available. |
| `user_email` | `VARCHAR` | Job owner email when visible. |
| `principal_subject` | `VARCHAR` | Principal subject when visible. |
| `job_type` | `VARCHAR` | Query, load, extract, or another job type. |
| `statistics` | `JSON` | Full BigQuery statistics object. |
| `configuration` | `JSON` | Full job configuration object. |
| `status` | `JSON` | Full job status object. |

Fields that BigQuery omits can be `NULL`. Listing other users' jobs requires
the corresponding IAM permission.

## Errors

Typical errors include an unknown `jobId`, invalid state or time filters,
missing `jobs.list` or `jobs.listAll` permissions, and a function-level
endpoint combined with an attached catalog.
