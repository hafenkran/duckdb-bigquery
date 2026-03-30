#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
namespace bigquery {

// Register cast from VARCHAR alias GEOGRAPHY -> GEOMETRY (WKT parsing via core cast)
void RegisterGeographyCast(DatabaseInstance &db);

/// Normalizes BigQuery GEOGRAPHY-compatible polygon topology on DuckDB
/// GEOMETRY values. Exterior rings are forced to CCW, interior rings (holes)
/// to CW, and touching holes with duplicate shared edges are merged when this
/// can be done unambiguously, including split collinear boundary segments.
/// Non-polygon geometry types are passed through unchanged.
bool NormalizeGeography(const string_t &input_geom, string_t &result_geom, Vector &result_vector);

/// DuckDB scalar function entry point for
/// bigquery_normalize_geography(GEOMETRY) -> GEOMETRY
void BqNormalizeGeographyFunction(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace bigquery
} // namespace duckdb
