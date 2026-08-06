# Concepts

This section explains the ideas behind the extension rather than individual
SQL workflows. Use it to understand how DuckDB connects its catalog and
execution engine to BigQuery, which BigQuery APIs handle reads and writes, and
how values cross the boundary between both type systems.

- [Architecture](architecture.md) explains how DuckDB, the attached catalog,
  `google-cloud-cpp`, the BigQuery REST API, and the Storage Read and Write APIs
  work together.
- [Data Type Mapping](data-types.md) documents bidirectional
  schema conversion, nested and unsupported types, `BIGNUMERIC`, and
  `GEOGRAPHY` interchange.

For task-oriented instructions, return to the [User Guide](../user-guide/attach.md). For
complete SQL signatures and options, use the
[Function Reference](../function-reference/index.md).
