
#include "duckdb/planner/operator/logical_update.hpp"

#include "bigquery_sql.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_transaction.hpp"
#include "storage/bigquery_update.hpp"

namespace duckdb {
namespace bigquery {

struct BigqueryUpdateGlobalSourceState : public GlobalSourceState {
    explicit BigqueryUpdateGlobalSourceState() : finished(false), updated_count(0) {
    }
    bool finished;
    idx_t updated_count;
};

BigqueryUpdate::BigqueryUpdate(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
      query(std::move(query)) {
}

unique_ptr<GlobalSourceState> BigqueryUpdate::GetGlobalSourceState(ClientContext &context) const {
    return make_uniq<BigqueryUpdateGlobalSourceState>();
}

SourceResultType BigqueryUpdate::GetDataInternal(ExecutionContext &context,
                                                 DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
    auto &gstate = input.global_state.Cast<BigqueryUpdateGlobalSourceState>();
    if (gstate.finished) {
        return SourceResultType::FINISHED;
    }

    auto &transaction = BigqueryTransaction::Get(context.client, table.catalog);
    auto bq_client = transaction.GetBigqueryClient();
    gstate.updated_count = bq_client->ExecuteDmlQuery(query, BigqueryDmlStatementType::DML_UPDATE);
    gstate.finished = true;

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
    (void)plan;
    return PlanUpdate(context, planner, op);
}

PhysicalOperator &BigqueryCatalog::PlanUpdate(ClientContext &context,
                                              PhysicalPlanGenerator &planner,
                                              LogicalUpdate &op) {
    BigqueryTransaction::CheckReadWrite(context, *this, "update tables");
    if (op.return_chunk) {
        throw NotImplementedException("RETURNING clause not supported.");
    }
    auto query = BigquerySQL::LogicalUpdateToSQL(GetProjectID(), op);
    auto &update = planner.Make<BigqueryUpdate>(op, op.table, query);
    return update;
}

} // namespace bigquery
} // namespace duckdb
