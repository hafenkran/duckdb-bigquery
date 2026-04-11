#pragma once

#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

#include <atomic>
#include <memory>

namespace duckdb {
namespace bigquery {

struct FactoryDependency final : public DependencyItem {
    explicit FactoryDependency(shared_ptr<BigqueryStreamFactory> ptr) : DependencyItem(), factory(std::move(ptr)) {
    }

    shared_ptr<BigqueryStreamFactory> factory;
};

struct BigqueryScanBindData : public ArrowScanFunctionData {
    BigqueryScanBindData()
        : ArrowScanFunctionData(&BigqueryStreamFactory::Produce, 0, make_shared_ptr<FactoryDependency>(nullptr)) {
    }

    BigqueryScanBindData(const BigqueryScanBindData &) = delete;
    BigqueryScanBindData &operator=(const BigqueryScanBindData &) = delete;

    BigqueryTableRef table_ref;
    string query;
    vector<Value> query_parameters;
    string filter_condition;

    BigqueryConfig bq_config;
    shared_ptr<BigqueryClient> bq_client;
    optional_ptr<BigqueryCatalog> bq_catalog;
    optional_ptr<BigqueryTableEntry> bq_table_entry;

    vector<string> names;
    vector<LogicalType> mapped_bq_types;

    idx_t estimated_row_count = 1;
    bool requires_cast = false;

    shared_ptr<FactoryDependency> GetFactoryDependency() {
        return dependency ? shared_ptr_cast<DependencyItem, FactoryDependency>(dependency) : nullptr;
    }

    bool RequiresQueryExec() const {
        return !query.empty();
    }

    string ParentString() const {
        return BigqueryUtils::FormatParentString(table_ref.project_id);
    }

    string TableString() const {
        return BigqueryUtils::FormatTableStringSimple(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
    }
};

struct BigqueryScanGlobalState : public ArrowScanGlobalState {
    mutable mutex lock;
    atomic<idx_t> position = 0;
};

struct BigqueryScanLocalState : public ArrowScanLocalState {
    BigqueryScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &context)
        : ArrowScanLocalState(std::move(current_chunk), context) {
    }

    DataChunk tmp_bq_chunk;
};

struct BigqueryScanFunction : public TableFunction {
    BigqueryScanFunction();

    static unique_ptr<FunctionData> BigqueryScanBind(ClientContext &context,
                                                     TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types,
                                                     vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> BigqueryScanInitGlobalState(ClientContext &context,
                                                                            TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState> BigqueryScanInitLocalState(ExecutionContext &context,
                                                                          TableFunctionInitInput &input,
                                                                          GlobalTableFunctionState *global_state_p);
    static void BigqueryScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
};

} // namespace bigquery
} // namespace duckdb
