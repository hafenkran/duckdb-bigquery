#pragma once

#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
namespace bigquery {


class BigqueryInsert : public PhysicalOperator {
public:
    //! INSERT INTO
    BigqueryInsert(LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map_p);
    //! CREATE TABLE AS
    BigqueryInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

    ~BigqueryInsert() override;

    optional_ptr<TableCatalogEntry> table;   // table for INSERT INTO
    optional_ptr<SchemaCatalogEntry> schema; // required for CREATE TABLE AS
    unique_ptr<BoundCreateTableInfo> info;   // required for CREATE TABLE AS
    physical_index_vector_t<idx_t> column_index_map;

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override {
        return true;
    }

    bool ParallelSink() const override {
        return true;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
};


} // namespace bigquery
} // namespace duckdb
