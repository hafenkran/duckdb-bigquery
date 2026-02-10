#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
namespace bigquery {

/// Ensures OGC-compliant polygon ring winding in a WKT string.
/// Exterior rings are forced to CCW, interior rings (holes) to CW.
/// Non-polygon geometry types are passed through unchanged.
string ForcePolygonCCW(const string &wkt);

/// DuckDB scalar function entry point for bq_force_polygon_ccw(VARCHAR) -> VARCHAR
void BqForcePolygonCCWFunction(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace bigquery
} // namespace duckdb
