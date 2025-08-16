#pragma once

#include "duckdb.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"
#include "storage/bigquery_catalog.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryClient;

enum class ScanEngine { V1, V2 };

ScanEngine DetermineScanEngine(const TableFunctionBindInput &input);

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

class BigqueryScanFunction : public TableFunction {
public:
    BigqueryScanFunction();
};

class BigqueryQueryFunction : public TableFunction {
public:
    BigqueryQueryFunction();
};

} // namespace bigquery
} // namespace duckdb
