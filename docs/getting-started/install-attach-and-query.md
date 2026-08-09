# Install, Attach, and Query

Install the BigQuery extension from the official
[Community Extension Repository](https://duckdb.org/community_extensions/extensions/bigquery).
The extension does not require unsigned-extension mode.

!!! note "DuckDB version compatibility"

    Current development targets DuckDB 1.5.x. The latest extension changes are
    available only in builds for DuckDB 1.5.x and are not currently backported
    to older DuckDB release branches.

    Older DuckDB releases can use their corresponding compatible extension
    builds, but those builds may not contain the latest features.

Before continuing, configure one of the supported
[authentication methods](authentication-and-secrets.md). The following
example then installs the extension, attaches a BigQuery project, and queries a
table:

```sql
-- Install the BigQuery extension from the Community Extension Repository.
FORCE INSTALL 'bigquery' FROM community;

-- Load the extension.
LOAD 'bigquery';

-- Attach every accessible dataset in the project as a read-only catalog.
ATTACH 'project=my-gcp-project' AS bq (TYPE bigquery, READ_ONLY);

-- List tables from the attached datasets.
SHOW ALL TABLES;
┌──────────┬──────────────────┬──────────┬──────────────┬───────────────────┬───────────┐
│ database │      schema      │   name   │ column_names │   column_types    │ temporary │
│ varchar  │     varchar      │ varchar  │  varchar[]   │     varchar[]     │  boolean  │
├──────────┼──────────────────┼──────────┼──────────────┼───────────────────┼───────────┤
│ bq       │ quacking_dataset │ duck_tbl │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
│ bq       │ barking_dataset  │ dog_tbl  │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
└──────────┴──────────────────┴──────────┴──────────────┴───────────────────┴───────────┘

-- Query a table through the attached catalog.
SELECT *
  FROM bq.quacking_dataset.duck_tbl;
┌───────┬────────────────┐
│   i   │       s        │
│ int64 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

For attachment options such as dataset scope, billing projects, endpoints, and
write access, continue with
[Attaching Projects](../user-guide/attach.md).

!!! warning "Windows gRPC configuration"

    Windows requires an additional gRPC trust-store configuration. Follow
    [Windows gRPC Configuration](../troubleshooting.md#windows-grpc-configuration)
    before starting DuckDB.
