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
    vector<Value> query_parameters;
    bool finished = false;
};

//! Bind data for REST-only query path (opt-in via use_rest_api=true)
struct BigqueryQueryRestBindData : public TableFunctionData {
    BigqueryConfig config;
    shared_ptr<BigqueryClient> bq_client;
    string query;
    vector<Value> query_parameters;

    vector<string> names;
    vector<LogicalType> types;
    idx_t estimated_row_count = 1;
};

class BigqueryQueryFunction : public TableFunction {
public:
    BigqueryQueryFunction();
};

} // namespace bigquery
} // namespace duckdb
