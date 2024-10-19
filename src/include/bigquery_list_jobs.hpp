#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryListJobsFunction : public TableFunction {
public:
    BigQueryListJobsFunction();
};


} // namespace bigquery
} // namespace duckdb
