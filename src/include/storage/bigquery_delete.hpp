#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
namespace bigquery {

class BigqueryDeleteGlobalState : public GlobalSinkState {
public:
    explicit BigqueryDeleteGlobalState() : deleted_count(0) {
    }
    idx_t deleted_count;
};

class BigqueryDelete : public PhysicalOperator {
public:
    BigqueryDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query);

    TableCatalogEntry &table;
    string query;

public:
    // Source Interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

public:
    // Sink Interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline,
                              Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override {
        return true;
    }

    bool ParallelSink() const override {
        return false;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace bigquery
} // namespace duckdb
