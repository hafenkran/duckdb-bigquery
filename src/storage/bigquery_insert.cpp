#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

#include <google/cloud/bigquery/bigquery_write_client.h>

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "bigquery_proto_writer.hpp"
#include "storage/bigquery_catalog.hpp"
#include "storage/bigquery_insert.hpp"
#include "storage/bigquery_transaction.hpp"

namespace duckdb {
namespace bigquery {

BigqueryInsert::BigqueryInsert(LogicalOperator &op,
                               TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), //
      table(&table),                                                  //
      column_index_map(std::move(column_index_map_p)) {
}

BigqueryInsert::BigqueryInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), //
      schema(&schema),                                                //
      info(std::move(info_p)) {
}

BigqueryInsert::~BigqueryInsert() {
}

class BigqueryInsertGlobalState : public GlobalSinkState {
public:
    explicit BigqueryInsertGlobalState(ClientContext &context) : insert_count(0) {
    }

    idx_t insert_count;
    BigqueryTableEntry *table;
    DataChunk varchar_chunk;

    shared_ptr<BigqueryProtoWriter> writer;
    std::map<std::string, idx_t> column_name_to_index;
};

vector<string> GetInsertColumns(const duckdb::bigquery::BigqueryInsert &insert, BigqueryTableEntry &entry) {
    vector<string> column_names;
    idx_t column_count = 0;
    auto &columns = entry.GetColumns();

    if (!insert.column_index_map.empty()) {
        vector<PhysicalIndex> column_indexes;
        column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));

        for (idx_t i = 0; i < insert.column_index_map.size(); i++) {
            auto column_index = PhysicalIndex(i);
            auto mapped_index = insert.column_index_map[column_index];
            if (mapped_index == DConstants::INVALID_INDEX) {
                continue; // Column not specified
            }
            column_indexes[mapped_index] = column_index;
            column_count++;
        }
        for (idx_t i = 0; i < column_count; i++) {
            auto &col = columns.GetColumn(column_indexes[i]);
            column_names.push_back(col.GetName());
        }
    }
    return column_names;
}

unique_ptr<GlobalSinkState> BigqueryInsert::GetGlobalSinkState(ClientContext &context) const {
    BigqueryTableEntry *insert_table;
    if (!table) { // CREATE TABLE AS case
        auto &schema_ref = *schema.get_mutable();
        auto transaction = schema_ref.GetCatalogTransaction(context);
        insert_table = &schema_ref.CreateTable(transaction, *info)->Cast<BigqueryTableEntry>();
    } else {
        insert_table = &table.get_mutable()->Cast<BigqueryTableEntry>();
    }

    auto &transaction = BigqueryTransaction::Get(context, insert_table->catalog);
    auto bq_client = transaction.GetBigqueryClient();
    auto result = make_uniq<BigqueryInsertGlobalState>(context);
    result->table = insert_table;
    result->writer = bq_client->CreateProtoWriter(insert_table);

    auto insert_columns = GetInsertColumns(*this, *insert_table);
    std::map<std::string, idx_t> column_name_to_index;
    for (idx_t i = 0; i < insert_columns.size(); i++) {
        column_name_to_index[insert_columns[i]] = insert_table->GetColumnIndex(insert_columns[i]).index;
    }
    result->column_name_to_index = column_name_to_index;
    return std::move(result);
}

// ### SINK
SinkResultType BigqueryInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &gstate = sink_state->Cast<BigqueryInsertGlobalState>();
    gstate.writer->WriteChunk(chunk, gstate.column_name_to_index);
    gstate.insert_count += chunk.size();
    return SinkResultType::NEED_MORE_INPUT;
}

// ### GET DATA
SourceResultType BigqueryInsert::GetData(ExecutionContext &context,
                                         DataChunk &chunk,
                                         OperatorSourceInput &input) const {
    auto &gstate = sink_state->Cast<BigqueryInsertGlobalState>();
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));
    return SourceResultType::FINISHED;
}

// ### FINALIZE
SinkFinalizeType BigqueryInsert::Finalize(Pipeline &pipeline,
                                          Event &event,
                                          ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
    auto &gstate = sink_state->Cast<BigqueryInsertGlobalState>();
    gstate.writer->Finalize();
    return SinkFinalizeType::READY;
}

// ### HELPERS
string BigqueryInsert::GetName() const {
    return table ? "BIGQUERY_INSERT" : "BIGQUERY_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> BigqueryInsert::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Table Name"] = table ? table->name : info->Base().table;
    return result;
}

// ### PLAN
PhysicalOperator &AddCastToBigqueryTypes(ClientContext &context,
                                         PhysicalPlanGenerator &planner,
                                         PhysicalOperator &plan) {
    bool requires_casts = false;
    auto &child_types = plan.GetTypes();
    for (auto &type : child_types) {
        auto bigquery_type = BigqueryUtils::CastToBigqueryType(type);
        if (type.id() != bigquery_type.id()) {
            requires_casts = true;
            break;
        }
    }
    if (!requires_casts) {
        return plan;
    }

    vector<LogicalType> bigquery_types;
    vector<unique_ptr<Expression>> select_list;
    for (idx_t i = 0; i < child_types.size(); i++) {
        auto &type = child_types[i];
        unique_ptr<Expression> expr = make_uniq<BoundReferenceExpression>(type, i);

        auto bigquery_type = BigqueryUtils::CastToBigqueryType(type);
        if (type != bigquery_type) {
            expr = BoundCastExpression::AddCastToType(context, std::move(expr), bigquery_type);
        }

        bigquery_types.push_back(std::move(bigquery_type));
        select_list.push_back(std::move(expr));
    }


    auto &proj =
        planner.Make<PhysicalProjection>(std::move(bigquery_types), std::move(select_list), plan.estimated_cardinality);
    proj.children.push_back(plan);
    return proj;
}


PhysicalOperator &BigqueryCatalog::PlanInsert(ClientContext &context,
                                              PhysicalPlanGenerator &planner,
                                              LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
    if (op.return_chunk) {
        throw BinderException("RETURNING clause not supported.");
    }
    if (op.action_type != OnConflictAction::THROW) {
        throw BinderException("ON CONFLCIT clause not supported.");
    }

    auto &child_plan = AddCastToBigqueryTypes(context, planner, *plan);
    auto &insert = planner.Make<BigqueryInsert>(op, op.table, op.column_index_map);
    insert.children.push_back(child_plan);
    return insert;
}

PhysicalOperator &BigqueryCatalog::PlanCreateTableAs(ClientContext &context,
                                                     PhysicalPlanGenerator &planner,
                                                     LogicalCreateTable &op,
                                                     PhysicalOperator &plan) {
    auto &child_plan = AddCastToBigqueryTypes(context, planner, plan); // TODO
    auto &insert = planner.Make<BigqueryInsert>(op, op.schema, std::move(op.info));
    insert.children.push_back(child_plan);
    return insert;
}

} // namespace bigquery
} // namespace duckdb
