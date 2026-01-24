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

unique_ptr<FunctionData> BigqueryQueryBindInternal(ClientContext &context,
                                                   const string &dbname_or_project_id,
                                                   const string &query_string,
                                                   const BigQueryCommonParameters &params,
                                                   vector<LogicalType> &return_types,
                                                   vector<string> &names,
                                                   bool allow_dry_run);

unique_ptr<GlobalTableFunctionState> BigqueryQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> BigqueryQueryInitLocal(ExecutionContext &context,
                                                           TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state);
void BigqueryQueryExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
BindInfo BigqueryQueryGetBindInfo(const optional_ptr<FunctionData> bind_data_p);
InsertionOrderPreservingMap<string> BigqueryQueryToString(TableFunctionToStringInput &input);
unique_ptr<NodeStatistics> BigqueryQueryCardinality(ClientContext &context, const FunctionData *bind_data_p);
double BigqueryQueryProgress(ClientContext &context,
                             const FunctionData *bind_data_p,
                             const GlobalTableFunctionState *global_state);

class BigqueryQueryFunction : public TableFunction {
public:
    BigqueryQueryFunction();
};

} // namespace bigquery
} // namespace duckdb
