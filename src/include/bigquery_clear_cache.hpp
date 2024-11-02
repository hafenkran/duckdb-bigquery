#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryClearCacheFunction : public TableFunction {
public:
    BigqueryClearCacheFunction();

    static void ClearCache(ClientContext &context, SetScope scope, Value &parameter);
};

} // namespace bigquery
} // namespace duckdb
