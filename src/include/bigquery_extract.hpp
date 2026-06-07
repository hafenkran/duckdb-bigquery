#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryExtractFunction : public TableFunction {
public:
    BigQueryExtractFunction();
};

} // namespace bigquery
} // namespace duckdb
