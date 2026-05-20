
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

struct BigqueryDeleteGlobalSourceState : public GlobalSourceState {
    explicit BigqueryDeleteGlobalSourceState() : finished(false), deleted_count(0) {
    }
    bool finished;
    idx_t deleted_count;
};

BigqueryDelete::BigqueryDelete(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table, string query)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(table),
      query(std::move(query)) {
}

unique_ptr<GlobalSourceState> BigqueryDelete::GetGlobalSourceState(ClientContext &context) const {
    return make_uniq<BigqueryDeleteGlobalSourceState>();
}

SourceResultType BigqueryDelete::GetDataInternal(ExecutionContext &context,
                                                 DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
    auto &gstate = input.global_state.Cast<BigqueryDeleteGlobalSourceState>();
    if (gstate.finished) {
        return SourceResultType::FINISHED;
    }

    auto &transaction = BigqueryTransaction::Get(context.client, table.catalog);
    auto bq_client = transaction.GetBigqueryClient();
    gstate.deleted_count = bq_client->ExecuteDmlQuery(query, BigqueryDmlStatementType::DML_DELETE);
    gstate.finished = true;

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
    (void)plan;
    return PlanDelete(context, planner, op);
}

PhysicalOperator &BigqueryCatalog::PlanDelete(ClientContext &context,
                                              PhysicalPlanGenerator &planner,
                                              LogicalDelete &op) {
    BigqueryTransaction::CheckReadWrite(context, *this, "delete from tables");
    if (op.return_chunk) {
        throw BinderException("RETURNING clause is not supported.");
    }
    auto query = BigquerySQL::LogicalDeleteToSQL(GetProjectID(), op);
    auto &delete_op = planner.Make<BigqueryDelete>(op, op.table, query);
    return delete_op;
}

} // namespace bigquery
} // namespace duckdb
