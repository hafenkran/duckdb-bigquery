#pragma once

#include "duckdb.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryClient;

struct BigqueryBindData : public TableFunctionData {
	BigqueryConfig config;
	BigqueryTableRef table_ref;

    shared_ptr<BigqueryClient> bq_client;

    vector<string> names;
    vector<LogicalType> types;
    idx_t estimated_row_count = 1;

    string ParentString() const {
        return BigqueryUtils::FormatParentString(table_ref.project_id);
    }

    string TableString() const {
        return BigqueryUtils::FormatTableString(table_ref.project_id, table_ref.dataset_id, table_ref.table_id);
    }
};

// struct BigqueryGlobalTableFunctionState : public GlobalTableFunctionState {
// public:
//     idx_t MaxThreads() const override;
// };

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
