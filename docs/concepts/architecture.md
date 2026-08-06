# Architecture

The extension connects DuckDB's SQL engine and catalog to several BigQuery
services. It is therefore not a single REST wrapper: DuckDB plans and executes
the local parts of a statement, while the extension selects the appropriate
BigQuery API for metadata, remote computation, or data transfer.

A single operation may use more than one API. For example,
`bigquery_query` starts a query through the BigQuery REST API and normally reads
the resulting temporary table through the BigQuery Storage Read API.

```text
DuckDB SQL
    |
    +-- binder, planner, and attached BigQuery catalog
    |       |
    |       +-- BigQuery REST API -------- metadata, jobs, DDL, and DML
    |       +-- Storage Read API (gRPC) --- BigQuery to DuckDB, as Arrow
    |       +-- Storage Write API (gRPC) -- DuckDB to BigQuery, as protobuf
    |
    +-- DuckDB execution engine ---------- local operators and result vectors
```

The extension uses
[`google-cloud-cpp`](https://cloud.google.com/cpp/docs/reference) to construct
authenticated clients, configure REST and gRPC transports, apply retry policies,
and work with the generated BigQuery request and response types. The extension
itself remains responsible for DuckDB integration, SQL translation, catalog
caching, parallel scan scheduling, and conversion between DuckDB, Arrow,
protobuf, and BigQuery types.

## Responsibilities by Layer

| Layer | Responsibility |
| --- | --- |
| DuckDB | Parses SQL, binds names and types, builds plans, runs local operators, and exposes results as DuckDB vectors. |
| Extension catalog and hooks | Maps BigQuery resources into DuckDB, intercepts supported DDL and DML, and chooses a remote execution path. |
| `BigqueryClient` | Holds project, billing project, endpoints, and credentials; creates the required `google-cloud-cpp` clients for each operation. |
| BigQuery REST API | Manages datasets and tables, submits and monitors jobs, executes GoogleSQL, and optionally returns query rows directly. |
| BigQuery Storage Read API | Transfers table data and materialized query results to DuckDB using parallel gRPC streams and Arrow. |
| BigQuery Storage Write API | Appends DuckDB rows to BigQuery using a bidirectional gRPC stream and protobuf messages. |

## `google-cloud-cpp` and the Transports

`google-cloud-cpp` is the client-library layer between the extension and Google
Cloud. It does not decide whether work belongs in DuckDB or BigQuery; the
extension makes that decision and then creates the appropriate library client:

- the generated `bigquerycontrol_v2` dataset, table, and job clients use the
  library's REST transport;
- the generated `bigquery_storage_v1` read and write clients use gRPC; and
- the common library supplies credentials, endpoints, retry and backoff
  policies, status handling, and protobuf request and response types.

This lets all API paths use the same extension configuration and authentication
model without the extension implementing HTTP, OAuth, or gRPC framing itself.
The REST and Storage APIs remain distinct BigQuery services with different
purposes, permissions, quotas, and network behavior.

## BigQuery APIs

### BigQuery REST API

The extension uses the [BigQuery v2 REST API](https://cloud.google.com/bigquery/docs/reference/rest)
as its control plane. Calls are made through the REST transport of the
`google-cloud-cpp` BigQuery control clients.

The REST path handles:

- listing and reading dataset and table metadata;
- submitting GoogleSQL DDL jobs that create, alter, and delete datasets and
  tables;
- submitting and polling query, load, and extract jobs;
- running translated DDL, `UPDATE`, and `DELETE` statements as GoogleSQL jobs;
- listing jobs and retrieving their status and statistics; and
- returning paginated query rows when `bigquery_query(..., use_rest_api := true)`
  is selected.

REST is used for coordination even when it does not carry the result data. A
query can be submitted and monitored over REST before its result is transferred
through Storage Read.

### BigQuery Storage Read API

The [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage)
is the main high-throughput path from BigQuery to DuckDB. The extension creates
a read session for a BigQuery table, requests Arrow as the wire format, and asks
BigQuery for one or more read streams.

At session creation, the extension can pass:

- the columns required by the DuckDB plan;
- supported filters as a BigQuery row restriction; and
- a maximum number of streams based on the available DuckDB execution threads.

DuckDB workers consume the returned streams in parallel. Arrow record batches
are exposed through the Arrow C Data Interface and converted into DuckDB
vectors. Any type-specific post-processing is applied after the batches enter
DuckDB.

Storage Read reads table storage; it does not execute arbitrary GoogleSQL or
manage datasets, tables, and jobs. Consequently, a direct native-table scan can
use Storage Read without creating a query job, while a GoogleSQL query must be
executed first and produces a table that Storage Read can consume.

### BigQuery Storage Write API

The extension uses the [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api)
for attached `INSERT` and `CREATE TABLE AS` data transfer. DuckDB produces
`DataChunk` values, and the extension builds a protobuf schema and serializes
the rows into protobuf messages.

Writes use an application-created pending stream:

1. `CreateWriteStream` creates the pending stream.
2. `AppendRows` sends batches over a bidirectional gRPC connection.
3. `FinalizeWriteStream` closes the stream for further appends.
4. `BatchCommitWriteStreams` makes the pending rows visible in BigQuery.

The extension can keep several append requests in flight while preserving their
order. Pending streams provide an atomic commit boundary for the rows sent to
that stream. This boundary belongs to BigQuery, however, and is not a rollback
mechanism for a surrounding DuckDB transaction.

## Connection and Authentication

Direct functions such as `bigquery_scan` build a connection configuration from
their arguments. Operations on an attached database reuse the configuration of
its `BigqueryCatalog`, including the project, optional dataset, billing project,
API endpoints, and access mode.

Each DuckDB transaction on an attached BigQuery catalog owns a shared
`BigqueryClient`. This keeps metadata calls, jobs, and storage sessions within
that transaction on the same configuration. It does not turn remote BigQuery
operations into DuckDB transactions: once a remote DDL, DML, or committed write
has succeeded, a later DuckDB rollback cannot undo it.

For both REST and gRPC, the client first looks for a matching DuckDB BigQuery
secret. If none is available, it uses Google Application Default Credentials.
The resulting Google Cloud credentials are shared by the API paths, although
REST and gRPC can have separate endpoint settings. See
[Authentication & Secrets](../getting-started/authentication-and-secrets.md) for the supported
credential providers.

## Attached Catalog and Planning

An attached BigQuery database presents remote resources as DuckDB catalog
objects:

| BigQuery | DuckDB |
| --- | --- |
| Project or attached dataset scope | Attached database |
| Dataset | Schema |
| Table | Table catalog entry |
| BigQuery column type | DuckDB logical type |

During binding, the extension resolves the remote object and retrieves enough
metadata to expose its columns and types to DuckDB. Metadata is cached in the
attached catalog. Catalog discovery can fetch individual resources through
REST, or use the optional `INFORMATION_SCHEMA` optimization, which submits a
metadata query job for a dataset and caches the returned table information.

DDL and DML statements pass through extension hooks. The extension converts
supported DuckDB statements and expressions to GoogleSQL, while ordinary local
operators remain in the DuckDB plan. A connection callback can clear stale
metadata and request one rebind when a remote schema change causes a missing
column error.

## Execution Paths

The operation determines which services participate in execution:

| DuckDB operation | Remote execution | Data transfer |
| --- | --- | --- |
| Attached table `SELECT` or `bigquery_scan` | No query job for a native table scan | Storage Read API |
| `bigquery_query` | GoogleSQL query job through REST | Storage Read API by default |
| `bigquery_query(..., use_rest_api := true)` | GoogleSQL through REST with optional job creation | Paginated REST rows |
| Attached `INSERT` | DuckDB executes the input plan | Storage Write API |
| `CREATE TABLE AS` | A GoogleSQL DDL job through REST creates the table; DuckDB executes the input plan | Storage Write API |
| Attached DDL, `UPDATE`, or `DELETE` | Translated GoogleSQL job through REST | Job status and affected-row statistics through REST |
| `bigquery_load` or `bigquery_extract` | BigQuery load or extract job through REST | BigQuery or Cloud Storage performs the bulk transfer |
| `bigquery_jobs` | Job listing or lookup through REST | REST metadata |

### Native Table Read

For `bigquery_scan` and a scan of an attached native table:

1. The binder obtains table metadata and maps the schema to DuckDB types.
2. DuckDB determines the required columns and applicable filters.
3. The extension creates a Storage Read session with projection and row
   restrictions.
4. BigQuery returns one or more Arrow streams.
5. DuckDB schedules those streams across workers and consumes the resulting
   vectors in the rest of the local plan.

Logical views and external tables require BigQuery to execute their defining
logic, so they are not equivalent to a direct native-table Storage Read scan.

### GoogleSQL Query

The default `bigquery_query` flow has a control phase and a transfer phase:

1. The extension submits GoogleSQL through the REST API with job creation
   required.
2. BigQuery executes the query and materializes its result in a destination
   table.
3. The extension retrieves the destination table reference from the completed
   job.
4. The normal Storage Read path reads that table into DuckDB.

With `use_rest_api := true`, the extension instead requests optional job
creation and decodes rows from the REST response and any following result
pages. This avoids Storage Read session setup, but is intended for result sets
that fit the REST-oriented path.

### Insert and Create Table As

For an attached `INSERT`, DuckDB runs the statement's input plan locally. The
extension receives the produced chunks, maps the destination schema to
protobuf, and appends the encoded rows through Storage Write. `CREATE TABLE AS`
first creates the remote table with a GoogleSQL DDL job submitted through REST
and then uses the same insert sink.

This is deliberately different from `UPDATE` and `DELETE`: those statements
are translated as whole statements and executed by BigQuery as GoogleSQL jobs,
rather than streaming changed rows from DuckDB.

### Jobs, Load, and Extract

Query, DDL, DML, load, and extract operations use BigQuery jobs. The extension
constructs the job configuration, submits it over REST, polls its state, maps
errors to DuckDB exceptions, and returns selected statistics or status fields.

For load and extract jobs, the bulk bytes do not pass through DuckDB result
vectors. BigQuery reads the configured source or writes the destination in
Cloud Storage. A local file is uploaded with the load-job request. A DuckDB
table source is first materialized as a temporary Parquet file and then follows
that upload path.

## Pushdown and Execution Ownership

The boundary between DuckDB and BigQuery depends on which parts of a plan can
be represented remotely:

- Column projection and supported filters can be pushed into a Storage Read
  session, reducing the data sent to DuckDB.
- Supported aggregate plans can be rewritten as GoogleSQL. In that case,
  BigQuery performs the aggregation as a query job and DuckDB reads the result.
- Operators that are not pushed down continue to run in DuckDB after the remote
  scan.
- DDL, `UPDATE`, and `DELETE` are translated and owned by BigQuery as complete
  remote statements.

This distinction matters for performance and observability. A native scan may
show Storage Read activity without a BigQuery query job, whereas an aggregate
pushdown or `bigquery_query` creates a job before any result transfer begins.

## Data Conversion Boundaries

There are three primary representations at the DuckDB–BigQuery boundary:

- **Arrow** for Storage Read responses and the efficient conversion of
  columnar batches into DuckDB vectors.
- **Protocol buffers** for rows sent through Storage Write.
- **BigQuery REST response messages** for metadata, job information, and the
  optional paginated query-result path.

The extension also converts DuckDB expressions and statements to GoogleSQL for
remote execution. Type mappings are not always one-to-one; nested values,
high-precision numerics, and `GEOGRAPHY` require specific handling. See
[Data Type Mapping](data-types.md) for the general conversion rules and
[Working with Geometries](../user-guide/geometry-support.md) for the `GEOGRAPHY` boundary.

For task-oriented examples, continue with
[Reading & Queries](../user-guide/reading-and-queries.md),
[Writing & Modifying Data](../user-guide/writing.md), or
[Executing & Monitoring Jobs](../user-guide/jobs-and-transfers.md).
