#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

// Register cast from VARCHAR alias GEOGRAPHY -> GEOMETRY (WKT parsing via core cast)
void RegisterWKTGeometryCast(DatabaseInstance &db);

} // namespace bigquery
} // namespace duckdb
