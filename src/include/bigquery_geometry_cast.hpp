#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

// Register cast from VARCHAR alias WKT -> BLOB alias GEOMETRY (runtime lookup of ST_GeomFromText during execution)
void RegisterWKTGeometryCast(DatabaseInstance &db);

} // namespace bigquery
} // namespace duckdb
