#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
namespace bigquery {

/// Ensures OGC-compliant polygon ring winding on DuckDB GEOMETRY values.
/// Exterior rings are forced to CCW, interior rings (holes) to CW.
/// Non-polygon geometry types are passed through unchanged.
bool ForcePolygonCCW(const string_t &input_geom, string_t &result_geom, Vector &result_vector);

/// DuckDB scalar function entry point for
/// bigquery_force_polygon_ccw(GEOMETRY) -> GEOMETRY
void BqForcePolygonCCWFunction(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace bigquery
} // namespace duckdb
