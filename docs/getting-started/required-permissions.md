# Required Permissions

Authentication identifies the caller; Google Cloud IAM determines which
projects, datasets, tables, jobs, and write operations that identity may use.
Configure an [authentication method](authentication-and-secrets.md) first,
then grant only the permissions required by the intended workflow.

## Where Permissions Apply

BigQuery separates the project that creates a job from the datasets and tables
that the job accesses. Cloud Storage permissions are managed on the source or
destination bucket. A `billing_project` can supply quota and receive charges,
but it does not grant access to data in another project.

## Common Roles

| Scope | Role | Typical use |
| --- | --- | --- |
| Project | `roles/bigquery.jobUser` | Create query, load, and extract jobs |
| Project | `roles/bigquery.readSessionUser` | Create Storage Read sessions |
| Dataset or table | `roles/bigquery.dataViewer` | Read table and view data |
| Dataset or table | `roles/bigquery.dataEditor` | Create and modify table data |
| Project | `roles/bigquery.resourceViewer` | List jobs created by all users in the project |
| Bucket | `roles/storage.objectViewer` | Read Cloud Storage objects used by a load job |
| Bucket | `roles/storage.objectAdmin` | Create and, when necessary, replace extract objects |

Prefer least-privilege custom roles when the predefined roles grant more access
than the workload requires.

## Permissions by Operation

| Operation | Required access | Scope |
| --- | --- | --- |
| Attached native-table read or `bigquery_scan` | `roles/bigquery.dataViewer` and `roles/bigquery.readSessionUser` | Data role on the dataset or table; read-session role on the project that owns the read session |
| `bigquery_query` | `roles/bigquery.jobUser`, read access to every referenced relation, and `roles/bigquery.readSessionUser` for the standard result path | Job and read-session roles on the job project; data role on source datasets or tables |
| `bigquery_execute` | `roles/bigquery.jobUser` plus permissions required by the submitted GoogleSQL | Job project and every referenced source or destination |
| Attached `INSERT`, `UPDATE`, `DELETE`, DDL, or CTAS | `roles/bigquery.dataEditor` or equivalent narrower permissions | Destination dataset or table; job permissions are additionally required for job-backed operations |
| List the caller's jobs | `bigquery.jobs.list` | Project containing the jobs |
| List all users' jobs | `bigquery.jobs.listAll`, included in `roles/bigquery.resourceViewer` | Project containing the jobs |

The standard `bigquery_query` path materializes a result and reads it with the
Storage Read API. With `use_rest_api := true`, result rows use the REST path,
but the query still requires job and source-data permissions.

## Load Jobs

Every load job needs `bigquery.jobs.create` on its job project and write access
to the destination dataset or table. The additional source permissions depend
on the selected source:

- A local file is uploaded by the extension and does not require caller access
  to an existing Cloud Storage object.
- A DuckDB table or view is staged as a local Parquet file and follows the
  local-file upload path.
- A `gs://` source requires `storage.objects.get` on every object. URI
  wildcards additionally require `storage.objects.list` on the bucket. The
  predefined `roles/storage.objectViewer` role contains both permissions.

See Google's [batch loading permissions](https://cloud.google.com/bigquery/docs/batch-loading-data)
for the exact permissions and service restrictions.

## Extract Jobs

An extract job needs `bigquery.jobs.create` on the job project, read access to
the source table, and Cloud Storage permissions on the destination bucket.
Writing to an existing bucket requires `storage.objects.create`; replacing
objects can also require `storage.objects.delete`. The predefined
`roles/storage.objectAdmin` role contains both permissions.

See Google's [export permissions](https://cloud.google.com/bigquery/docs/exporting-data)
for the current role recommendations.

## Billing and Quota Projects

When `billing_project` differs from the data project, job creation and quota
permissions apply to the billing project while data permissions remain on the
source and destination resources. Organizations using service-usage controls
can also require `serviceusage.services.use` on the quota project.

IAM failures identify the missing permission and resource in the BigQuery or
Cloud Storage error. Grant the narrowest role at the lowest appropriate scope,
then continue with [Attaching Projects](../user-guide/attach.md).
