#pragma once

#include "duckdb.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryClient;

struct BigqueryGlobalFunctionState : public GlobalTableFunctionState {
    explicit BigqueryGlobalFunctionState(shared_ptr<BigqueryArrowReader> arrow_reader, idx_t max_threads)
        : position(0), max_threads(max_threads), arrow_reader(std::move(arrow_reader)) {
    }

    mutable mutex lock;
    atomic<idx_t> position;
    idx_t max_threads;

    // The index of the next stream to read (i.e., current file + 1)
    atomic<idx_t> next_stream = 0;
    shared_ptr<BigqueryArrowReader> arrow_reader;

    idx_t MaxThreads() const override {
        return max_threads;
    }

    idx_t NextStreamIndex() {
        return next_stream.fetch_add(1);
    }
};

struct BigqueryLegacyScanBindData : public TableFunctionData {
    BigqueryConfig config;
    BigqueryTableRef table_ref;

    string query;
    string filter_condition;

    shared_ptr<BigqueryClient> bq_client;
    optional_ptr<BigqueryCatalog> bq_catalog;
    optional_ptr<BigqueryTableEntry> bq_table_entry;

    vector<string> names;
    vector<LogicalType> types;
    idx_t estimated_row_count = 1;

    string ParentString() const {
        return BigqueryUtils::FormatParentString(table_ref.project_id);
    }

    string TableString() const {
        return BigqueryUtils::FormatTableStringSimple(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
    }

    bool RequiresQueryExec() const {
        return !query.empty();
    }
};

struct BigqueryScanFunction : public TableFunction {
public:
    BigqueryScanFunction();

public:
    static unique_ptr<FunctionData> BigqueryScanBind(ClientContext &context,
                                                     TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types,
                                                     vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> BigqueryScanInitGlobalState(ClientContext &context,
                                                                            TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState> BigqueryScanInitLocalState(ExecutionContext &context,
                                                                          TableFunctionInitInput &input,
                                                                          GlobalTableFunctionState *global_state);
    static void BigqueryScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

struct BigqueryLegacyScanFunction : public TableFunction {
public:
    BigqueryLegacyScanFunction();

public:
    static unique_ptr<FunctionData> BigqueryLegacyScanBind(ClientContext &context,
                                                           TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types,
                                                           vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> BigqueryLegacyScanInitGlobalState(ClientContext &context,
                                                                                  TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState> BigqueryLegacyScanInitLocalState(ExecutionContext &context,
                                                                                TableFunctionInitInput &input,
                                                                                GlobalTableFunctionState *global_state);
    static void BigqueryLegacyScanExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output);
};

} // namespace bigquery
} // namespace duckdb
