#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigQueryListJobsFunction : public TableFunction {
public:
    BigQueryListJobsFunction();
};

class BigQueryGetJobFunction : public TableFunction {
public:
	BigQueryGetJobFunction();
};

} // namespace bigquery
} // namespace duckdb
