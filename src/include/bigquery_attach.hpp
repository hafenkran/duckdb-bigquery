#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryAttachFunction : public TableFunction {
public:
    BigqueryAttachFunction();
};

} // namespace bigquery
} // namespace duckdb
