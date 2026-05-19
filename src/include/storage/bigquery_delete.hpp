#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryDelete : public PhysicalOperator {
public:
    BigqueryDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query);

    TableCatalogEntry &table;
    string query;

public:
    // Source Interface
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    SourceResultType GetDataInternal(ExecutionContext &context,
                                     DataChunk &chunk,
                                     OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace bigquery
} // namespace duckdb
