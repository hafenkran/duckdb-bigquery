#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
namespace bigquery {


class BigqueryUpdate : public PhysicalOperator {
public:
    BigqueryUpdate(LogicalOperator &op, TableCatalogEntry &table, string query);

    // Table to do updates on
    TableCatalogEntry &table;

    // Query to execute
    string query;

public:
    // Source Interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
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
