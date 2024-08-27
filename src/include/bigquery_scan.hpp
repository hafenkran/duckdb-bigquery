#pragma once

#include "duckdb.hpp"

#include "bigquery_client.hpp"
#include "bigquery_utils.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryClient;

struct BigqueryBindData : public TableFunctionData {
    string execution_project_id;
    string project_id;
    string dataset_id;
    string table_id;

    shared_ptr<BigqueryClient> bq_client;

    vector<string> names;
    vector<LogicalType> types;
    idx_t estimated_row_count = 1;

    string ParentString() const {
        return BigqueryUtils::FormatParentString(project_id);
    }

    string TableString() const {
        return BigqueryUtils::FormatTableString(project_id, dataset_id, table_id);
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

} // namespace bigquery
} // namespace duckdb
