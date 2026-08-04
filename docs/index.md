# DuckDB BigQuery Extension

<p align="center">
  <a href="https://duckdb.org/community_extensions/extensions/bigquery"><img alt="DuckDB Community Extension" src="https://img.shields.io/badge/DuckDB-Community%20Extension-FFF000?logo=duckdb&amp;logoColor=000"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/releases/latest"><img alt="Latest Release" src="https://img.shields.io/github/v/release/hafenkran/duckdb-bigquery?label=Latest%20Release"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/actions/workflows/MainDistributionPipeline.yml"><img alt="Build and Test" src="https://img.shields.io/github/actions/workflow/status/hafenkran/duckdb-bigquery/MainDistributionPipeline.yml?branch=main&amp;label=Build"></a>
  <a href="https://github.com/hafenkran/duckdb-bigquery/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/github/license/hafenkran/duckdb-bigquery?label=License"></a>
</p>

This community extension allows [DuckDB](https://duckdb.org) to query data
from Google BigQuery using a mix of BigQuery Storage (Read/Write) and REST APIs.
You can explore, query, create, and modify BigQuery tables and datasets
directly from DuckDB using standard SQL queries. Inspired by official DuckDB
storage extensions like
[MySQL](https://duckdb.org/docs/current/core_extensions/mysql),
[PostgreSQL](https://github.com/duckdb/duckdb-postgres), and
[SQLite](https://github.com/duckdb/duckdb-sqlite), this extension offers a
similar feel. Dedicated functions cover direct table scans, GoogleSQL queries,
load and extract jobs, and job inspection.

See [Important Notes](#important-notes) for disclaimers and usage information.

!!! warning "Disclaimer: Independent community project"

    This project is independently maintained and is not affiliated with,
    endorsed by, or officially supported by Google LLC or DuckDB Labs.
    BigQuery and DuckDB are trademarks of their respective owners. The
    extension is provided without warranties; users are responsible for their
    use, compliance, and incurred charges.

## What You Can Do

- Attach a project or dataset as a DuckDB catalog and explore it with `SHOW`
  and `DESCRIBE`.
- Read native tables through the BigQuery Storage Read API with projection and
  filter pushdown.
- Run GoogleSQL and read views, materialized views, and external tables.
- Create and alter datasets, tables, and views with DuckDB SQL.
- Insert, update, and delete data, or write DuckDB query results to BigQuery.
- Submit and inspect query, load, and extract jobs.
- Read and write BigQuery `GEOGRAPHY` as DuckDB
  [`GEOMETRY('OGC:CRS84')`](user-guide/geometry-support.md).

!!! info "Supported builds"

    Community builds are available for `linux_amd64`, `linux_arm64`,
    `osx_amd64`, `osx_arm64`, and `windows_amd64`.
    The builds `wasm_mvp`, `wasm_eh`, `wasm_threads`, and `windows_amd64_mingw` are not supported.

To make your first connection, follow
[Install, Attach, and Query](getting-started/install-attach-and-query.md).

## Important Notes

When using this software with Google BigQuery, please ensure your usage complies with the [Google API Terms of Service](https://developers.google.com/terms). Be mindful of the usage limits and quotas, and adhere to Google's Fair Use Policy.

### Billing and Costs

Please be aware that using Google BigQuery through this software can incur costs. Google
BigQuery is a paid service, and charges may apply based on the amount of data processed,
stored, and the number of queries executed. Users are responsible for any costs associated
with their use of Google BigQuery. For detailed information on BigQuery pricing, please
refer to the [Google BigQuery Pricing](https://cloud.google.com/bigquery/pricing) page.
It is recommended to monitor your usage and set up budget alerts in the Google Cloud Console
to avoid unexpected charges.

By using this software, you acknowledge and agree that you are solely responsible for any
charges incurred from Google BigQuery.
