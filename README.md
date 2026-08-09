# DuckDB BigQuery Extension

<p align="center">
  <a href="https://duckdb.org/community_extensions/extensions/bigquery"><img alt="DuckDB Community Extension" src="https://img.shields.io/badge/DuckDB-Community%20Extension-FFF000?logo=duckdb&amp;logoColor=000"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/releases/latest"><img alt="Latest Release" src="https://img.shields.io/github/v/release/hafenkran/duckdb-bigquery?label=Latest%20Release"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/actions/workflows/MainDistributionPipeline.yml"><img alt="Build and Test" src="https://img.shields.io/github/actions/workflow/status/hafenkran/duckdb-bigquery/MainDistributionPipeline.yml?branch=main&amp;label=Build"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/actions/workflows/docs.yml"><img alt="Documentation" src="https://img.shields.io/github/actions/workflow/status/hafenkran/duckdb-bigquery/docs.yml?branch=main&amp;label=Documentation"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/github/license/hafenkran/duckdb-bigquery?label=License"></a>
</p>

This community extension allows [DuckDB](https://duckdb.org) to query data
from Google BigQuery using a mix of BigQuery Storage (Read/Write) and REST APIs.
You can explore, query, create, and modify BigQuery tables and datasets
directly from DuckDB using standard SQL queries. Dedicated functions cover
direct table scans, GoogleSQL queries, load and extract jobs, and job
inspection.

Inspired by official DuckDB storage extensions like
[MySQL](https://duckdb.org/docs/current/core_extensions/mysql),
[PostgreSQL](https://github.com/duckdb/duckdb-postgres), and
[SQLite](https://github.com/duckdb/duckdb-sqlite), this extension offers a
similar feel.

With the extension, you can:

- Attach a project or dataset as a DuckDB catalog and explore it with `SHOW`
  and `DESCRIBE`.
- Read native tables through the BigQuery Storage Read API with projection and
  filter pushdown.
- Run GoogleSQL and read views, materialized views, and external tables.
- Create and alter datasets, tables, and views with DuckDB SQL.
- Insert, update, and delete data, or write DuckDB query results to BigQuery.
- Submit and inspect query, load, and extract jobs.
- Read and write BigQuery `GEOGRAPHY` as DuckDB `GEOMETRY`.

Read the full **[documentation](https://hafenkran.github.io/duckdb-bigquery/)** for more details.

> Community builds are available for `linux_amd64`, `linux_arm64`, `osx_amd64`,
> `osx_arm64`, and `windows_amd64`. The builds `wasm_mvp`, `wasm_eh`,
> `wasm_threads`, and `windows_amd64_mingw` are not supported.
>
> Current development targets **DuckDB 1.5**. The latest extension changes are
> available only in builds for **DuckDB 1.5** and are not backported
> to older DuckDB release branches.

## Quickstart

Configure one of the supported
[authentication methods](https://hafenkran.github.io/duckdb-bigquery/getting-started/authentication-and-secrets/)
before connecting. Local development can use Google
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
The selected identity also needs the appropriate
[BigQuery permissions](https://hafenkran.github.io/duckdb-bigquery/getting-started/required-permissions/).

> **Note**: Windows users require an additional step to configure the gRPC SSL
> certificates. See
> [Windows gRPC Configuration](https://hafenkran.github.io/duckdb-bigquery/troubleshooting/#windows-grpc-configuration).

Install and load the extension, then attach your BigQuery project. Replace
`my-gcp-project` with the corresponding Google Cloud project ID:

```sql
-- Install and load the DuckDB BigQuery extension from the Community Repository
FORCE INSTALL 'bigquery' FROM community;
LOAD 'bigquery';

-- Attach to your BigQuery Project
ATTACH 'project=my-gcp-project' AS bq (TYPE bigquery, READ_ONLY);

-- Show all tables in all datasets in the attached BigQuery project
SHOW ALL TABLES;
┌──────────┬──────────────────┬──────────┬──────────────┬───────────────────┬───────────┐
│ database │      schema      │   name   │ column_names │   column_types    │ temporary │
│ varchar  │     varchar      │  varchar │  varchar[]   │     varchar[]     │  boolean  │
├──────────┼──────────────────┼──────────┼──────────────┼───────────────────┼───────────┤
│ bq       │ quacking_dataset │ duck_tbl │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
│ bq       │ barking_dataset  │ dog_tbl  │ [i, s]       │ [BIGINT, VARCHAR] │ false     │
└──────────┴──────────────────┴──────────┴──────────────┴───────────────────┴───────────┘

-- Select data from a specific table in BigQuery
SELECT * FROM bq.quacking_dataset.duck_tbl;
┌───────┬────────────────┐
│   i   │       s        │
│ int32 │    varchar     │
├───────┼────────────────┤
│    12 │ quack 🦆       │
│    13 │ quack quack 🦆 │
└───────┴────────────────┘
```

Continue with the full
[Install, Attach, and Query](https://hafenkran.github.io/duckdb-bigquery/getting-started/install-attach-and-query/)
guide or review all
[attachment options](https://hafenkran.github.io/duckdb-bigquery/user-guide/attach/).

For direct table scans, GoogleSQL execution, load and extract jobs, and job
inspection, see the
[Function Reference](https://hafenkran.github.io/duckdb-bigquery/function-reference/),
including `bigquery_scan`, `bigquery_query`, `bigquery_execute`,
`bigquery_load`, `bigquery_extract`, and `bigquery_jobs`.

## Building from Source

The extension uses VCPKG for dependency management. The following example
creates sibling checkouts of the extension and VCPKG on a Unix-like system:

```bash
git clone --recurse-submodules https://github.com/hafenkran/duckdb-bigquery.git
git clone https://github.com/microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh

cd duckdb-bigquery
GEN=ninja \
VCPKG_TOOLCHAIN_PATH="$(pwd)/../vcpkg/scripts/buildsystems/vcpkg.cmake" \
make release
```

Use `make debug` instead of `make release` for a debug build. The primary
release outputs are:

- `build/release/duckdb`
- `build/release/extension/bigquery/bigquery.duckdb_extension`

BigQuery integration tests require credentials, can modify live cloud
resources, and may incur charges. Do not run them against an unintended project
or dataset.

## Important Notes on Using Google BigQuery

> **⚠️ Disclaimer**: This is an independent, community-maintained open-source project and is not affiliated with, endorsed by, or officially supported by Google LLC, or any of their subsidiaries. This extension is provided "as is" without any warranties or guarantees. "DuckDB" and "BigQuery" are trademarks of their respective owners. Users are solely responsible for compliance with applicable terms of service and any costs incurred through usage.

When using this software with Google BigQuery, please ensure your usage complies with the [Google API Terms of Service](https://developers.google.com/terms). Be mindful of the usage limits and quotas, and adhere to Google's Fair Use Policy.

### Billing and Costs

Please be aware that using Google BigQuery through this software can incur costs. Google BigQuery is a paid service, and charges may apply based on the amount of data processed, stored, and the number of queries executed. Users are responsible for any costs associated with their use of Google BigQuery. For detailed information on BigQuery pricing, please refer to the [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing) page. It is recommended to monitor your usage and set up budget alerts in the Google Cloud Console to avoid unexpected charges.

By using this software, you acknowledge and agree that you are solely responsible for any charges incurred from Google BigQuery.
## License

This project is available under the [MIT License](LICENSE).
