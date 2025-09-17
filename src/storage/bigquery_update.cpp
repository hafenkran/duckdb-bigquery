
#include "duckdb/planner/operator/logical_update.hpp"

#include "bigquery_sql.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_update.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryUpdateGlobalState : public GlobalSinkState {
    explicit BigqueryUpdateGlobalState() : updated_count(0) {
    }
    idx_t updated_count;
};

BigqueryUpdate::BigqueryUpdate(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table), query(std::move(query)) {
}

unique_ptr<GlobalSinkState> BigqueryUpdate::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<BigqueryUpdateGlobalState>();
}

SinkResultType BigqueryUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    return SinkResultType::FINISHED;
}

SinkFinalizeType BigqueryUpdate::Finalize(Pipeline &pipeline,
                                          Event &event,
                                          ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<BigqueryUpdateGlobalState>();
    auto &transaction = BigqueryTransaction::Get(context, table.catalog);
    auto bq_client = transaction.GetBigqueryClient();
    auto result = bq_client->ExecuteQuery(query);

    auto total_rows = result.total_rows();
    uint64_t extracted_value = total_rows.value();
    gstate.updated_count = extracted_value;
    return SinkFinalizeType::READY;
}

SourceResultType BigqueryUpdate::GetData(ExecutionContext &context,
                                         DataChunk &chunk,
                                         OperatorSourceInput &input) const {
    auto &gstate = sink_state->Cast<BigqueryUpdateGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.updated_count));
    return SourceResultType::FINISHED;
}

string BigqueryUpdate::GetName() const {
    return "BIGQUERY_UPDATE";
}

InsertionOrderPreservingMap<string> BigqueryUpdate::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table.name;
    return result;
}

PhysicalOperator &BigqueryCatalog::PlanUpdate(ClientContext &context,
                                              PhysicalPlanGenerator &planner,
                                              LogicalUpdate &op,
                                              PhysicalOperator &plan) {
    if (op.return_chunk) {
        throw NotImplementedException("RETURNING clause not supported.");
    }
    auto query = BigquerySQL::LogicalUpdateToSQL(GetProjectID(), op, plan);
    auto &update = planner.Make<BigqueryUpdate>(op, op.table, query);
    update.children.push_back(plan);
    return update;
}

} // namespace bigquery
} // namespace duckdb
