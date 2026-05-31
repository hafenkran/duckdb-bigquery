#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryExportFunction : public TableFunction {
public:
    BigQueryExportFunction();
};

} // namespace bigquery
} // namespace duckdb
