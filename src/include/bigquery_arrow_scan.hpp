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
// Forward-delcaration
unique_ptr<ArrowArrayStreamWrapper> ProduceArrowScan(const ArrowScanFunctionData &function,
                                                     const vector<column_t> &column_ids,
                                                     TableFilterSet *filters);


namespace bigquery {

struct FactoryDependency final : public DependencyItem {
    explicit FactoryDependency(shared_ptr<BigqueryStreamFactory> ptr) : DependencyItem(), factory(std::move(ptr)) {
    }
    shared_ptr<BigqueryStreamFactory> factory;
};

struct BigqueryArrowScanBindData : public ArrowScanFunctionData {
    explicit BigqueryArrowScanBindData()
        : ArrowScanFunctionData(&BigqueryStreamFactory::Produce, 0, make_shared_ptr<FactoryDependency>(nullptr)) {
    }
    explicit BigqueryArrowScanBindData(stream_factory_produce_t prod, uintptr_t ptr, shared_ptr<DependencyItem> dep)
        : ArrowScanFunctionData(prod, ptr, std::move(dep)) {
    }
    BigqueryArrowScanBindData(const BigqueryArrowScanBindData &) = delete;
    BigqueryArrowScanBindData &operator=(const BigqueryArrowScanBindData &) = delete;

public:
    // The BigQuery table reference for this scan
    BigqueryTableRef table_ref;
    //! The query string for the scan (used for bigquery_query function)
    string query;
    //! The filter string for the scan
    string filter_condition;

    //! The BigQuery configuration used for this scan
    BigqueryConfig bq_config;
    //! The BigQuery client used for this scan
    shared_ptr<BigqueryClient> bq_client;
    //! The BigQuery catalog used for this scan
    optional_ptr<BigqueryCatalog> bq_catalog; // TODO necessary?
    //! The BigQuery table entry used for this scan
    optional_ptr<BigqueryTableEntry> bq_table_entry;

    //! Names of the columns in the table
    vector<string> names;
    //! Types of the columns in the table
    vector<LogicalType> types;
    vector<LogicalType> mapped_bq_types;

    //! Estimated row count of the table
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

struct BigqueryArrowScanGlobalState : public ArrowScanGlobalState {
    mutable mutex lock;
    atomic<idx_t> position;
};

struct BigqueryArrowScanLocalState : public ArrowScanLocalState {
public:
    BigqueryArrowScanLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &context)
        : ArrowScanLocalState(std::move(current_chunk), context) {
    }

public:
    DataChunk tmp_bq_chunk;
};

struct BigqueryArrowScanFunction : public TableFunction {
public:
    BigqueryArrowScanFunction();

public:
    static unique_ptr<FunctionData> BigqueryArrowScanBind(ClientContext &context,
                                                          TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types,
                                                          vector<string> &names);
    static unique_ptr<GlobalTableFunctionState> BigqueryArrowScanInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input);
    static unique_ptr<LocalTableFunctionState> BigqueryArrowScanInitLocal(ExecutionContext &context,
                                                                          TableFunctionInitInput &input,
                                                                          GlobalTableFunctionState *global_state_p);
    static void BigqueryArrowScanExecute(ClientContext &ctx, TableFunctionInput &data_p, DataChunk &output);
};

} // namespace bigquery
} // namespace duckdb
