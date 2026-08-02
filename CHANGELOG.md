# Changelog

Notable user-facing changes to the DuckDB BigQuery extension are recorded here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

No user-facing changes have been recorded since v0.12.0.

## [0.12.0] - 2026-07-22 ([compare][0.12.0 diff])

### Added

- Added `bigquery_extract` for BigQuery table extract jobs ([#199]).
- Expanded `bigquery_load` with CSV, newline-delimited JSON, Avro, ORC, Hive
  partitioning, schema-update, and format-specific options ([#202]).
- Added experimental aggregate pushdown, including grouped and distinct
  aggregates and selected scalar expressions ([#204], [#206], [#207], [#208],
  [#209], [#210]).

### Changed

- An attached dataset now becomes the default schema for its BigQuery catalog
  ([#201]).
- Updated the extension toolchain to DuckDB v1.5.4 ([#205]).

### Fixed

- Avoided a redundant Storage Read request at stream EOF ([#200]).
- Fixed billing-project handling for `bigquery_query` dry runs ([#211]).

## [0.11.0] - 2026-06-04 ([compare][0.11.0 diff])

### Added

- Added `destination_table`, `write_disposition`, and `create_disposition` to
  `bigquery_execute` for query-to-table jobs ([#196]).
- Added per-call `timeout_ms` support for BigQuery query polling ([#195]).

### Changed

- `ATTACH` now validates that configured credentials can obtain a token
  ([#193]).
- Updated the extension toolchain to DuckDB v1.5.3 ([#192]).

### Fixed

- DML operations return affected-row counts and avoid unnecessary scans
  ([#188], [#189]).
- Improved filter rendering, stale catalog rebinding, query polling, and
  parallel Storage Read behavior ([#190], [#191], [#195], [#197]).

## [0.10.0] - 2026-05-19 ([compare][0.10.0 diff])

### Added

- Added GCS URI sources, billing projects, and labels to `bigquery_load`
  ([#178], [#179], [#182]).
- Added OAuth authorized-user credentials to BigQuery secrets ([#181]).

### Changed

- Upgraded `google-cloud-cpp` to 2.47.1 ([#180]).

### Fixed

- Improved ADC metadata authentication, authentication timeouts, CA retry,
  read-only catalog enforcement, catalog-cache stability, and Storage Write
  startup retry ([#183], [#184], [#185], [#186], [#187]).

## Earlier Releases

### [0.9.0] - 2026-05-06 ([compare][0.9.0 diff])

Introduced the `bigquery_load` job function and improved permission error
handling ([#176], [#177]).

### [0.8.1] - 2026-04-22 ([compare][0.8.1 diff])

Improved protobuf write throughput with fail-fast error handling and windowed
in-flight `AppendRows` requests ([#173], [#174], [#175]).

### [0.8.0] - 2026-04-16 ([compare][0.8.0 diff])

Removed the legacy scan implementation, added the REST fast path for
`bigquery_query`, improved GEOGRAPHY normalization, and updated to DuckDB
v1.5.2 ([#158], [#159], [#160], [#161], [#167], [#168], [#169], [#172]).

### [0.7.4] - 2026-03-30 ([compare][0.7.4 diff])

Improved GEOGRAPHY query handling, standardized BIGNUMERIC reads as `VARCHAR`,
and updated to DuckDB v1.5.1 ([#153], [#154], [#156]).

### [0.7.3] - 2026-03-28 ([compare][0.7.3 diff])

Improved Storage Write throughput through buffered requests and cached column
bindings ([#150], [#151]).

### [0.7.2] - 2026-03-13 ([compare][0.7.2 diff])

Updated to DuckDB v1.5.0, mapped BigQuery `GEOGRAPHY` directly to DuckDB
`GEOMETRY('OGC:CRS84')`, and added positional parameters to `bigquery_query`
([#138], [#146], [#148]).

### [0.7.2_andium] - 2026-03-13 ([compare][0.7.2_andium diff])

Published the positional-query-parameter work with a Windows build backport for
the Andium compatibility line ([#146], [#148]).

### [0.7.1] - 2026-02-21 ([compare][0.7.1 diff])

Fixed polygon ring winding for GEOGRAPHY inserts ([#143]).

### [0.7.0] - 2026-02-08 ([compare][0.7.0 diff])

Added experimental `PARTITION BY`, `CLUSTER BY`, and `OPTIONS` parsing, fixed
GEOGRAPHY table creation, and updated to DuckDB v1.4.4 ([#140], [#141],
[#142]).

### [0.6.3] - 2025-12-11 ([compare][0.6.3 diff])

Updated to DuckDB v1.4.3 ([#137]).

### [0.6.2] - 2025-11-16 ([compare][0.6.2 diff])

Updated to DuckDB v1.4.2 and improved `AppendRows` request sizing and
post-insert scans ([#135], [#136]).

### [0.6.1] - 2025-11-02 ([compare][0.6.1 diff])

Allowed catalog-load retries, fixed projected-column ordering, and added
code-quality CI checks ([#127], [#129], [#131]).

### [0.6.0] - 2025-10-18 ([compare][0.6.0 diff])

Added DuckDB Secrets integration and clarified BIGNUMERIC limitations
([#124], [#126]).

### [0.5.1] - 2025-10-10 ([compare][0.5.1 diff])

Added `dry_run` to `bigquery_query` and `bigquery_execute` and updated to
DuckDB v1.4.1 ([#119], [#122]).

### [0.5.0] - 2025-09-17 ([compare][0.5.0 diff])

Added BigQuery GEOGRAPHY read/write support, expanded platform builds, and
updated to DuckDB v1.4.0 ([#110], [#111], [#114], [#115]).

### [0.4.2] - 2025-08-24 ([compare][0.4.2 diff])

Made the Arrow-based reader the default `bigquery_scan` implementation and
fixed billing-project job lookup ([#105], [#108]).

### [0.4.1] - 2025-07-10 ([compare][0.4.1 diff])

Updated to DuckDB v1.3.2 ([#100]).

### [0.4.0] - 2025-06-20 ([compare][0.4.0 diff])

Introduced the Arrow reader, parallel read streams, Arrow compression, and
explicit Storage Read filters, and updated to DuckDB v1.3.1 ([#88], [#89],
[#90], [#92], [#93], [#97]).

### [0.3.1] - 2025-05-25 ([compare][0.3.1 diff])

Updated to DuckDB v1.3.0 ([#87]).

### [0.3.0] - 2025-04-29 ([compare][0.3.0 diff])

Improved nested-structure support, removed inconsistent HUGEINT support,
refactored complex type mapping, and added experimental BigQuery `OPTIONS`
support ([#79], [#81], [#82], [#85]).

### [0.2.2] - 2025-04-09 ([compare][0.2.2 diff])

Updated to DuckDB v1.2.2 and refreshed extension CI integration ([#72], [#76],
[#78]).

### [0.2.1] - 2025-03-12 ([compare][0.2.1 diff])

Updated to DuckDB v1.2.1 ([#71]).

### [0.2.0] - 2025-02-06 ([compare][0.2.0 diff])

Updated the BigQuery Control client, fixed catalog pagination and dataset
selection, improved CA handling, and added configurable query timeouts ([#57],
[#62], [#63], [#65], [#68]).

### [0.1.2] - 2025-01-02 ([compare][0.1.2 diff])

Improved NUMERIC and BIGNUMERIC support and optimized information-schema
catalog queries ([#55], [#56]).

### [0.1.1] - 2024-12-31 ([compare][0.1.1 diff])

Updated to DuckDB v1.1.3, added parameterized data types, and improved CA
bundle detection ([#49], [#51], [#52], [#53]).

### [0.1.0] - 2024-11-03 (initial release)

Introduced `bigquery_query`, `bigquery_jobs`, and faster BigQuery catalog
loading.

[Unreleased]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.12.0
[0.11.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.11.0
[0.10.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.10.0
[0.9.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.9.0
[0.8.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.8.1
[0.8.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.8.0
[0.7.4]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.4
[0.7.3]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.3
[0.7.2]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.2
[0.7.2_andium]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.2_andium
[0.7.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.1
[0.7.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.7.0
[0.6.3]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.6.3
[0.6.2]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.6.2
[0.6.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.6.1
[0.6.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.6.0
[0.5.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.5.1
[0.5.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.5.0
[0.4.2]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.4.2
[0.4.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.4.1
[0.4.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.4.0
[0.3.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.3.1
[0.3.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.3.0
[0.2.2]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.2.2
[0.2.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.2.1
[0.2.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.2.0
[0.1.2]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.1.2
[0.1.1]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.1.1
[0.1.0]: https://github.com/hafenkran/duckdb-bigquery/releases/tag/v0.1.0

[0.12.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.11.0...v0.12.0
[0.11.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.10.0...v0.11.0
[0.10.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.9.0...v0.10.0
[0.9.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.8.1...v0.9.0
[0.8.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.8.0...v0.8.1
[0.8.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.4...v0.8.0
[0.7.4 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.3...v0.7.4
[0.7.3 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.2...v0.7.3
[0.7.2 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.1...v0.7.2
[0.7.2_andium diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.2...v0.7.2_andium
[0.7.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.7.0...v0.7.1
[0.7.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.6.3...v0.7.0
[0.6.3 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.6.2...v0.6.3
[0.6.2 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.6.1...v0.6.2
[0.6.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.6.0...v0.6.1
[0.6.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.5.1...v0.6.0
[0.5.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.5.0...v0.5.1
[0.5.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.4.2...v0.5.0
[0.4.2 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.4.1...v0.4.2
[0.4.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.4.0...v0.4.1
[0.4.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.3.1...v0.4.0
[0.3.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.3.0...v0.3.1
[0.3.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.2.2...v0.3.0
[0.2.2 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.2.1...v0.2.2
[0.2.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.2.0...v0.2.1
[0.2.0 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.1.2...v0.2.0
[0.1.2 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.1.1...v0.1.2
[0.1.1 diff]: https://github.com/hafenkran/duckdb-bigquery/compare/v0.1.0...v0.1.1

[#49]: https://github.com/hafenkran/duckdb-bigquery/pull/49
[#51]: https://github.com/hafenkran/duckdb-bigquery/pull/51
[#52]: https://github.com/hafenkran/duckdb-bigquery/pull/52
[#53]: https://github.com/hafenkran/duckdb-bigquery/pull/53
[#55]: https://github.com/hafenkran/duckdb-bigquery/pull/55
[#56]: https://github.com/hafenkran/duckdb-bigquery/pull/56
[#57]: https://github.com/hafenkran/duckdb-bigquery/pull/57
[#62]: https://github.com/hafenkran/duckdb-bigquery/pull/62
[#63]: https://github.com/hafenkran/duckdb-bigquery/pull/63
[#65]: https://github.com/hafenkran/duckdb-bigquery/pull/65
[#68]: https://github.com/hafenkran/duckdb-bigquery/pull/68
[#71]: https://github.com/hafenkran/duckdb-bigquery/pull/71
[#72]: https://github.com/hafenkran/duckdb-bigquery/pull/72
[#76]: https://github.com/hafenkran/duckdb-bigquery/pull/76
[#78]: https://github.com/hafenkran/duckdb-bigquery/pull/78
[#79]: https://github.com/hafenkran/duckdb-bigquery/pull/79
[#81]: https://github.com/hafenkran/duckdb-bigquery/pull/81
[#82]: https://github.com/hafenkran/duckdb-bigquery/pull/82
[#85]: https://github.com/hafenkran/duckdb-bigquery/pull/85
[#87]: https://github.com/hafenkran/duckdb-bigquery/pull/87
[#88]: https://github.com/hafenkran/duckdb-bigquery/pull/88
[#89]: https://github.com/hafenkran/duckdb-bigquery/pull/89
[#90]: https://github.com/hafenkran/duckdb-bigquery/pull/90
[#92]: https://github.com/hafenkran/duckdb-bigquery/pull/92
[#93]: https://github.com/hafenkran/duckdb-bigquery/pull/93
[#97]: https://github.com/hafenkran/duckdb-bigquery/pull/97
[#100]: https://github.com/hafenkran/duckdb-bigquery/pull/100
[#105]: https://github.com/hafenkran/duckdb-bigquery/pull/105
[#108]: https://github.com/hafenkran/duckdb-bigquery/pull/108
[#110]: https://github.com/hafenkran/duckdb-bigquery/pull/110
[#111]: https://github.com/hafenkran/duckdb-bigquery/pull/111
[#114]: https://github.com/hafenkran/duckdb-bigquery/pull/114
[#115]: https://github.com/hafenkran/duckdb-bigquery/pull/115
[#119]: https://github.com/hafenkran/duckdb-bigquery/pull/119
[#122]: https://github.com/hafenkran/duckdb-bigquery/pull/122
[#124]: https://github.com/hafenkran/duckdb-bigquery/pull/124
[#126]: https://github.com/hafenkran/duckdb-bigquery/pull/126
[#127]: https://github.com/hafenkran/duckdb-bigquery/pull/127
[#129]: https://github.com/hafenkran/duckdb-bigquery/pull/129
[#131]: https://github.com/hafenkran/duckdb-bigquery/pull/131
[#135]: https://github.com/hafenkran/duckdb-bigquery/pull/135
[#136]: https://github.com/hafenkran/duckdb-bigquery/pull/136
[#137]: https://github.com/hafenkran/duckdb-bigquery/pull/137
[#138]: https://github.com/hafenkran/duckdb-bigquery/pull/138
[#140]: https://github.com/hafenkran/duckdb-bigquery/pull/140
[#141]: https://github.com/hafenkran/duckdb-bigquery/pull/141
[#142]: https://github.com/hafenkran/duckdb-bigquery/pull/142
[#143]: https://github.com/hafenkran/duckdb-bigquery/pull/143
[#146]: https://github.com/hafenkran/duckdb-bigquery/pull/146
[#148]: https://github.com/hafenkran/duckdb-bigquery/pull/148
[#150]: https://github.com/hafenkran/duckdb-bigquery/pull/150
[#151]: https://github.com/hafenkran/duckdb-bigquery/pull/151
[#153]: https://github.com/hafenkran/duckdb-bigquery/pull/153
[#154]: https://github.com/hafenkran/duckdb-bigquery/pull/154
[#156]: https://github.com/hafenkran/duckdb-bigquery/pull/156
[#158]: https://github.com/hafenkran/duckdb-bigquery/pull/158
[#159]: https://github.com/hafenkran/duckdb-bigquery/pull/159
[#160]: https://github.com/hafenkran/duckdb-bigquery/pull/160
[#161]: https://github.com/hafenkran/duckdb-bigquery/pull/161
[#167]: https://github.com/hafenkran/duckdb-bigquery/pull/167
[#168]: https://github.com/hafenkran/duckdb-bigquery/pull/168
[#169]: https://github.com/hafenkran/duckdb-bigquery/pull/169
[#172]: https://github.com/hafenkran/duckdb-bigquery/pull/172
[#173]: https://github.com/hafenkran/duckdb-bigquery/pull/173
[#174]: https://github.com/hafenkran/duckdb-bigquery/pull/174
[#175]: https://github.com/hafenkran/duckdb-bigquery/pull/175
[#176]: https://github.com/hafenkran/duckdb-bigquery/pull/176
[#177]: https://github.com/hafenkran/duckdb-bigquery/pull/177
[#178]: https://github.com/hafenkran/duckdb-bigquery/pull/178
[#179]: https://github.com/hafenkran/duckdb-bigquery/pull/179
[#180]: https://github.com/hafenkran/duckdb-bigquery/pull/180
[#181]: https://github.com/hafenkran/duckdb-bigquery/pull/181
[#182]: https://github.com/hafenkran/duckdb-bigquery/pull/182
[#183]: https://github.com/hafenkran/duckdb-bigquery/pull/183
[#184]: https://github.com/hafenkran/duckdb-bigquery/pull/184
[#185]: https://github.com/hafenkran/duckdb-bigquery/pull/185
[#186]: https://github.com/hafenkran/duckdb-bigquery/pull/186
[#187]: https://github.com/hafenkran/duckdb-bigquery/pull/187
[#188]: https://github.com/hafenkran/duckdb-bigquery/pull/188
[#189]: https://github.com/hafenkran/duckdb-bigquery/pull/189
[#190]: https://github.com/hafenkran/duckdb-bigquery/pull/190
[#191]: https://github.com/hafenkran/duckdb-bigquery/pull/191
[#192]: https://github.com/hafenkran/duckdb-bigquery/pull/192
[#193]: https://github.com/hafenkran/duckdb-bigquery/pull/193
[#195]: https://github.com/hafenkran/duckdb-bigquery/pull/195
[#196]: https://github.com/hafenkran/duckdb-bigquery/pull/196
[#197]: https://github.com/hafenkran/duckdb-bigquery/pull/197
[#200]: https://github.com/hafenkran/duckdb-bigquery/pull/200
[#199]: https://github.com/hafenkran/duckdb-bigquery/pull/199
[#201]: https://github.com/hafenkran/duckdb-bigquery/pull/201
[#202]: https://github.com/hafenkran/duckdb-bigquery/pull/202
[#204]: https://github.com/hafenkran/duckdb-bigquery/pull/204
[#205]: https://github.com/hafenkran/duckdb-bigquery/pull/205
[#206]: https://github.com/hafenkran/duckdb-bigquery/pull/206
[#207]: https://github.com/hafenkran/duckdb-bigquery/pull/207
[#208]: https://github.com/hafenkran/duckdb-bigquery/pull/208
[#209]: https://github.com/hafenkran/duckdb-bigquery/pull/209
[#210]: https://github.com/hafenkran/duckdb-bigquery/pull/210
[#211]: https://github.com/hafenkran/duckdb-bigquery/pull/211
