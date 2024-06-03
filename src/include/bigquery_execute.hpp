#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryExecuteFunction : public TableFunction {
public:
    BigQueryExecuteFunction();
};

} // namespace bigquery
} // namespace duckdb
