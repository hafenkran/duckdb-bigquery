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
    explicit FactoryDependency(shared_ptr<BigQueryStreamFactory> ptr) : DependencyItem(), factory(std::move(ptr)) {
    }
    shared_ptr<BigQueryStreamFactory> factory;
};

struct BigQueryArrowScanBindData : public ArrowScanFunctionData {
	explicit BigQueryArrowScanBindData()
		: ArrowScanFunctionData(&BigQueryStreamFactory::Produce, 0, make_shared_ptr<FactoryDependency>(nullptr)) {
	}
    explicit BigQueryArrowScanBindData(stream_factory_produce_t prod, uintptr_t ptr, shared_ptr<DependencyItem> dep)
        : ArrowScanFunctionData(prod, ptr, std::move(dep)) {
    }
    BigQueryArrowScanBindData(const BigQueryArrowScanBindData &) = delete;
    BigQueryArrowScanBindData &operator=(const BigQueryArrowScanBindData &) = delete;

public:
    // The BigQuery table reference for this scan
    BigqueryTableRef table_ref;
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
	vector<idx_t> column_mapping; // Maps output columns to source columns
    //! Estimated row count of the table
    idx_t estimated_row_count = 1;

    shared_ptr<FactoryDependency> factory_dep() {
        return dependency ? shared_ptr_cast<DependencyItem, FactoryDependency>(dependency) : nullptr;
    }

    string ParentString() const {
        return BigqueryUtils::FormatParentString(table_ref.project_id);
    }

    string TableString() const {
        return BigqueryUtils::FormatTableStringSimple(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
    }
};

struct BigQueryArrowScanFunction : public TableFunction {
    BigQueryArrowScanFunction();
};

} // namespace bigquery
} // namespace duckdb
