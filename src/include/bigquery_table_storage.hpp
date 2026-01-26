#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryTableStorageFunction : public TableFunction {
public:
    BigqueryTableStorageFunction();
};

} // namespace bigquery
} // namespace duckdb
