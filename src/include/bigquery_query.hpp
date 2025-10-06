#pragma once

#include "duckdb.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryQueryDryRunBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    string query;
    bool finished = false;
};

class BigqueryQueryFunction : public TableFunction {
public:
    BigqueryQueryFunction();
};

} // namespace bigquery
} // namespace duckdb
