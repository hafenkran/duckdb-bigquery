#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryLoadFunction : public TableFunction {
public:
    BigQueryLoadFunction();
};

} // namespace bigquery
} // namespace duckdb
