#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryClearCacheFunctionData : public TableFunctionData {
    bool finished = false;
};

class BigqueryClearCacheFunction : public TableFunction {
public:
    BigqueryClearCacheFunction();

    static void ClearCache(ClientContext &context, SetScope scope, Value &parameter);
};

} // namespace bigquery
} // namespace duckdb
