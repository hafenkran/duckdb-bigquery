
#include "duckdb.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

#include "bigquery_sql.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_delete.hpp"
#include "storage/bigquery_table_entry.hpp"
#include "storage/bigquery_transaction.hpp"

#include <iostream>

namespace duckdb {
namespace bigquery {

BigqueryDelete::BigqueryDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table), query(std::move(query)) {
}

unique_ptr<GlobalSinkState> BigqueryDelete::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<BigqueryDeleteGlobalState>();
}

SinkResultType BigqueryDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    return SinkResultType::FINISHED;
}

SinkFinalizeType BigqueryDelete::Finalize(Pipeline &pipeline,
                                          Event &event,
                                          ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<BigqueryDeleteGlobalState>();
    auto &transaction = BigqueryTransaction::Get(context, table.catalog);
    auto bq_client = transaction.GetBigqueryClient();
    auto result = bq_client->ExecuteQuery(query); // TODO
    gstate.deleted_count = result.total_rows().value();
    return SinkFinalizeType::READY;
}

SourceResultType BigqueryDelete::GetData(ExecutionContext &context,
                                         DataChunk &chunk,
                                         OperatorSourceInput &input) const {
    auto &gstate = sink_state->Cast<BigqueryDeleteGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.deleted_count));
    return SourceResultType::FINISHED;
}

string BigqueryDelete::GetName() const {
    return "BIGQUERY_DELETE";
}

InsertionOrderPreservingMap<string> BigqueryDelete::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
}

PhysicalOperator &BigqueryCatalog::PlanDelete(ClientContext &context,
                                              PhysicalPlanGenerator &planner,
                                              LogicalDelete &op,
                                              PhysicalOperator &plan) {
    if (op.return_chunk) {
        throw BinderException("RETURNING clause is not supported.");
    }
	auto query = BigquerySQL::LogicalDeleteToSQL(GetProjectID(), op, plan);
	auto &delete_op = planner.Make<BigqueryDelete>(op, op.table, query);
	delete_op.children.push_back(plan);
    return delete_op;
}

} // namespace bigquery
} // namespace duckdb
